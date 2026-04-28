from __future__ import annotations

import logging
import signal
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from app.collector.repository import MarketDataRepository
from app.config import load_config
from app.db.models import init_db
from app.db.session import get_engine
from app.paper.analysis import analyze_long_trade
from app.paper.experiments import ExperimentRuntime, resolve_experiments
from app.paper.repository import PaperTradingRepository
from app.paper.risk import RiskManager
from app.paper.strategy import baseline_long_signal, micro_momentum_burst_signal


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("paper_trader")


class PaperTraderService:
    def __init__(self) -> None:
        self.config = load_config()
        self.engine = get_engine()
        init_db(self.engine)
        self.health_repo = MarketDataRepository(self.engine, self.config.exchange)
        self.repo = PaperTradingRepository(self.engine)
        self.running = True
        self.cooldown_until_by_key: dict[tuple[str, str], datetime] = {}
        self.experiments = resolve_experiments(self.config)
        experiment_rows = self.repo.sync_experiments([experiment.to_record() for experiment in self.experiments])
        self.experiment_id_by_name = {row["name"]: int(row["id"]) for row in experiment_rows}

    def run(self) -> None:
        self._install_signal_handlers()
        while self.running:
            if not self.config.paper_trading.enabled:
                self._write_health("standby", "paper trading disabled; no entries generated")
                logger.info("paper trading disabled")
                time.sleep(300)
                continue
            try:
                self.tick()
                self._write_health("ok", "paper trader tick completed")
            except Exception as exc:
                logger.exception("paper trader tick failed")
                self._write_health("error", f"paper trader tick failed: {exc}")
            time.sleep(self.config.paper_trading.interval_seconds)

    def tick(self) -> None:
        candles_cache: dict[str, object] = {}
        for symbol in self.config.symbols:
            quote = self.repo.latest_quote(symbol)
            feature = self.repo.latest_feature(symbol)
            for experiment in self.experiments:
                self._manage_open_trade(experiment, symbol, quote, feature)
                if self.repo.open_trade(symbol, self._experiment_id(experiment)):
                    continue
                if not quote or not feature:
                    self._record_skip(experiment, symbol, quote, feature, "missing quote or feature")
                    continue
                self._maybe_open_trade(experiment, symbol, quote, feature, candles_cache)
        self._backfill_trade_analysis()

    def _manage_open_trade(
        self,
        experiment: ExperimentRuntime,
        symbol: str,
        quote: dict | None,
        feature: dict | None,
    ) -> None:
        trade = self.repo.open_trade(symbol, self._experiment_id(experiment))
        if not trade or not quote:
            return
        now = datetime.now(UTC)
        bid = Decimal(str(quote["bid"]))
        exit_price = self._apply_slippage(bid, "sell", experiment)
        exit_reason = None
        if exit_price <= Decimal(str(trade["stop_loss_price"])):
            exit_reason = "stop_loss"
        elif exit_price >= Decimal(str(trade["take_profit_price"])):
            exit_reason = "take_profit"
        elif now - trade["entry_time"] >= timedelta(minutes=experiment.paper.max_holding_minutes):
            exit_reason = "max_holding_time"
        elif (
            feature
            and experiment.paper.dynamic_exit_on_trade_flow_negative
            and Decimal(str(feature.get("trade_flow_imbalance") or 0)) < 0
        ):
            exit_reason = "momentum_faded"
        elif (
            feature
            and experiment.paper.dynamic_exit_on_orderbook_negative
            and Decimal(str(feature.get("orderbook_imbalance_avg") or 0)) < 0
        ):
            exit_reason = "orderbook_flipped"
        if not exit_reason:
            return

        entry_price = Decimal(str(trade["entry_price"]))
        quantity = Decimal(str(trade["quantity"]))
        notional_idr = Decimal(str(trade["notional_idr"]))
        gross_pnl_usdt = (exit_price - entry_price) * quantity
        gross_pnl_idr = gross_pnl_usdt * Decimal(str(experiment.paper.usdt_idr_rate))
        fee_idr = self._fee_idr(notional_idr, round_trips=1, experiment=experiment)
        slippage_idr = self._slippage_idr(notional_idr, round_trips=1, experiment=experiment)
        pnl_idr = gross_pnl_idr - fee_idr - slippage_idr
        pnl_percent = (pnl_idr / notional_idr * Decimal("100")) if notional_idr > 0 else Decimal("0")
        self.repo.close_trade(
            trade_id=trade["id"],
            exit_time=now,
            exit_price=exit_price,
            pnl_idr=pnl_idr,
            pnl_percent=pnl_percent,
            fee_estimate_idr=fee_idr,
            slippage_estimate_idr=slippage_idr,
            exit_reason=exit_reason,
        )
        self._finalize_trade_analysis({**trade, "exit_time": now, "exit_price": exit_price})
        cooldown_seconds = (
            experiment.paper.cooldown_after_loss_seconds if pnl_idr < 0 else experiment.paper.cooldown_after_trade_seconds
        )
        self.cooldown_until_by_key[(experiment.name, symbol)] = now + timedelta(seconds=cooldown_seconds)
        logger.info(
            "closed paper trade experiment=%s symbol=%s exit_reason=%s pnl_idr=%s",
            experiment.name,
            symbol,
            exit_reason,
            pnl_idr,
        )

    def _maybe_open_trade(
        self,
        experiment: ExperimentRuntime,
        symbol: str,
        quote: dict,
        feature: dict,
        candles_cache: dict[str, object],
    ) -> None:
        now = datetime.now(UTC)
        cooldown_until = self.cooldown_until_by_key.get((experiment.name, symbol))
        if cooldown_until and now < cooldown_until:
            self._record_skip(experiment, symbol, quote, feature, "cooldown active")
            return
        feature_age = (now - feature["open_time"]).total_seconds()
        quote_age = (now - quote["timestamp"]).total_seconds()
        if feature_age > experiment.paper.max_feature_age_seconds or quote_age > self.config.data.stale_data_seconds:
            self._record_skip(experiment, symbol, quote, feature, "stale market data")
            return

        signal = self._generate_signal(experiment, symbol, feature, quote, candles_cache)
        daily_stats = self.repo.daily_stats(self.config.timezone, self._experiment_id(experiment))
        risk = RiskManager(experiment.risk, Decimal(str(self.config.starting_balance_idr)))
        decision = risk.evaluate_entry(
            realized_pnl_idr=Decimal(str(daily_stats["realized_pnl_idr"])),
            trade_count=int(daily_stats["trade_count"]),
            consecutive_losses=self.repo.consecutive_losses(self.config.timezone, self._experiment_id(experiment)),
            spread_bps=Decimal(str(quote["spread_bps"])),
            has_take_profit=True,
            has_stop_loss=True,
        )
        if signal.decision != "TAKE":
            self._record_signal(experiment, symbol, signal, "SKIP", signal.reason)
            return
        if not decision.allowed:
            self._record_signal(experiment, symbol, signal, "SKIP", decision.reason)
            return

        ask = Decimal(str(quote["ask"]))
        entry_price = self._apply_slippage(ask, "buy", experiment)
        stop_loss_price = entry_price * (Decimal("1") - Decimal(str(experiment.paper.stop_loss_bps)) / Decimal("10000"))
        take_profit_price = entry_price * (Decimal("1") + Decimal(str(experiment.paper.take_profit_bps)) / Decimal("10000"))
        risk_per_unit_usdt = entry_price - stop_loss_price
        risk_idr = decision.risk_idr
        quantity_by_risk = risk_idr / Decimal(str(experiment.paper.usdt_idr_rate)) / risk_per_unit_usdt
        quantity_by_cap = (decision.max_position_idr / Decimal(str(experiment.paper.usdt_idr_rate))) / entry_price
        quantity = min(quantity_by_risk, quantity_by_cap)
        notional_idr = quantity * entry_price * Decimal(str(experiment.paper.usdt_idr_rate))
        if quantity <= 0 or notional_idr <= 0:
            self._record_signal(experiment, symbol, signal, "SKIP", "invalid position size")
            return

        self.repo.insert_trade(
            experiment_id=self._experiment_id(experiment),
            experiment_name=experiment.name,
            strategy_name=experiment.strategy_name,
            symbol=symbol,
            side="long",
            entry_time=now,
            entry_price=entry_price,
            quantity=quantity,
            notional_idr=notional_idr,
            take_profit_price=take_profit_price,
            stop_loss_price=stop_loss_price,
            fee_estimate_idr=self._fee_idr(notional_idr, round_trips=1, experiment=experiment),
            slippage_estimate_idr=self._slippage_idr(notional_idr, round_trips=1, experiment=experiment),
        )
        self._record_signal(experiment, symbol, signal, "TAKE", None)
        logger.info(
            "opened paper trade experiment=%s symbol=%s entry=%s tp=%s sl=%s",
            experiment.name,
            symbol,
            entry_price,
            take_profit_price,
            stop_loss_price,
        )

    def _generate_signal(
        self,
        experiment: ExperimentRuntime,
        symbol: str,
        feature: dict,
        quote: dict,
        candles_cache: dict[str, object],
    ):
        if experiment.strategy_name.startswith("ema_baseline"):
            candles = candles_cache.get(symbol)
            if candles is None:
                candles = self.repo.recent_candles(symbol)
                candles_cache[symbol] = candles
            return baseline_long_signal(
                candles,
                Decimal(str(quote["spread_bps"])),
                Decimal(str(experiment.risk.max_spread_bps)),
            )
        return micro_momentum_burst_signal(feature, quote, experiment.paper, experiment.risk)

    def _record_skip(
        self,
        experiment: ExperimentRuntime,
        symbol: str,
        quote: dict | None,
        feature: dict | None,
        reason: str,
    ) -> None:
        features = {"quote": quote or {}, "feature": feature or {}}
        self.repo.insert_signal(
            timestamp=datetime.now(UTC),
            symbol=symbol,
            experiment_id=self._experiment_id(experiment),
            experiment_name=experiment.name,
            strategy_name=experiment.strategy_name,
            side=None,
            confidence=Decimal("0"),
            reason=reason,
            features=features,
            decision="SKIP",
            skip_reason=reason,
        )

    def _record_signal(self, experiment: ExperimentRuntime, symbol: str, signal, decision: str, skip_reason: str | None) -> None:
        self.repo.insert_signal(
            timestamp=datetime.now(UTC),
            symbol=symbol,
            experiment_id=self._experiment_id(experiment),
            experiment_name=experiment.name,
            strategy_name=experiment.strategy_name,
            side=signal.side,
            confidence=signal.confidence,
            reason=signal.reason,
            features=signal.features,
            decision=decision,
            skip_reason=skip_reason,
        )

    def _apply_slippage(self, price: Decimal, side: str, experiment: ExperimentRuntime) -> Decimal:
        multiplier = Decimal(str(experiment.paper.slippage_bps)) / Decimal("10000")
        if side == "buy":
            return price * (Decimal("1") + multiplier)
        return price * (Decimal("1") - multiplier)

    def _fee_idr(self, notional_idr: Decimal, *, round_trips: int, experiment: ExperimentRuntime) -> Decimal:
        return (
            notional_idr
            * Decimal(str(experiment.paper.fee_rate_bps))
            / Decimal("10000")
            * Decimal(str(round_trips * 2))
        )

    def _slippage_idr(self, notional_idr: Decimal, *, round_trips: int, experiment: ExperimentRuntime) -> Decimal:
        return (
            notional_idr
            * Decimal(str(experiment.paper.slippage_bps))
            / Decimal("10000")
            * Decimal(str(round_trips * 2))
        )

    def _experiment_id(self, experiment: ExperimentRuntime) -> int:
        return self.experiment_id_by_name[experiment.name]

    def _backfill_trade_analysis(self) -> None:
        for trade in self.repo.trades_missing_analysis(limit=100):
            self._finalize_trade_analysis(trade)

    def _finalize_trade_analysis(self, trade: dict) -> None:
        if not trade.get("entry_time") or not trade.get("exit_time"):
            return
        horizon_end = max(
            trade["exit_time"],
            trade["entry_time"] + timedelta(minutes=10),
        )
        quotes = self.repo.quote_path(trade["symbol"], trade["entry_time"], horizon_end)
        analysis = analyze_long_trade(trade, quotes)
        self.repo.update_trade_analysis(
            trade_id=int(trade["id"]),
            gross_pnl_idr=analysis.gross_pnl_idr,
            gross_pnl_percent=analysis.gross_pnl_percent,
            hold_seconds=analysis.hold_seconds,
            max_favorable_excursion_bps=analysis.max_favorable_excursion_bps,
            max_adverse_excursion_bps=analysis.max_adverse_excursion_bps,
            horizon_3m_label=analysis.horizon_3m_label,
            horizon_5m_label=analysis.horizon_5m_label,
            horizon_10m_label=analysis.horizon_10m_label,
            label_source=analysis.label_source,
        )

    def _write_health(self, status: str, message: str) -> None:
        self.health_repo.write_health(
            "paper_trader",
            status,
            message,
            timestamp=datetime.now(UTC),
            last_success_at=datetime.now(UTC) if status in {"ok", "standby"} else None,
            metadata_json={
                "mode": "paper",
                "live_trading": False,
                "enabled": self.config.paper_trading.enabled,
                "experiments": [experiment.name for experiment in self.experiments],
            },
        )

    def _install_signal_handlers(self) -> None:
        def stop(_signum: int, _frame: object) -> None:
            self.running = False

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)


def main() -> None:
    PaperTraderService().run()


if __name__ == "__main__":
    main()
