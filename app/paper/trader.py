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
from app.paper.repository import PaperTradingRepository
from app.paper.risk import RiskManager
from app.paper.strategy import micro_momentum_burst_signal


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
        self.cooldown_until_by_symbol: dict[str, datetime] = {}

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
        for symbol in self.config.symbols:
            quote = self.repo.latest_quote(symbol)
            feature = self.repo.latest_feature(symbol)
            if not quote or not feature:
                self._record_skip(symbol, quote, feature, "missing quote or feature")
                continue
            self._manage_open_trade(symbol, quote, feature)
            if self.repo.open_trade(symbol):
                continue
            self._maybe_open_trade(symbol, quote, feature)

    def _manage_open_trade(self, symbol: str, quote: dict, feature: dict) -> None:
        trade = self.repo.open_trade(symbol)
        if not trade:
            return
        now = datetime.now(UTC)
        bid = Decimal(str(quote["bid"]))
        exit_price = self._apply_slippage(bid, "sell")
        exit_reason = None
        if exit_price <= Decimal(str(trade["stop_loss_price"])):
            exit_reason = "stop_loss"
        elif exit_price >= Decimal(str(trade["take_profit_price"])):
            exit_reason = "take_profit"
        elif now - trade["entry_time"] >= timedelta(minutes=self.config.paper_trading.max_holding_minutes):
            exit_reason = "max_holding_time"
        elif Decimal(str(feature.get("trade_flow_imbalance") or 0)) < 0:
            exit_reason = "momentum_faded"
        elif Decimal(str(feature.get("orderbook_imbalance_avg") or 0)) < 0:
            exit_reason = "orderbook_flipped"
        if not exit_reason:
            return

        entry_price = Decimal(str(trade["entry_price"]))
        quantity = Decimal(str(trade["quantity"]))
        notional_idr = Decimal(str(trade["notional_idr"]))
        gross_pnl_usdt = (exit_price - entry_price) * quantity
        gross_pnl_idr = gross_pnl_usdt * Decimal(str(self.config.paper_trading.usdt_idr_rate))
        fee_idr = self._fee_idr(notional_idr, round_trips=1)
        slippage_idr = self._slippage_idr(notional_idr, round_trips=1)
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
        cooldown = self.config.paper_trading.cooldown_after_loss_seconds if pnl_idr < 0 else self.config.paper_trading.cooldown_after_trade_seconds
        self.cooldown_until_by_symbol[symbol] = now + timedelta(seconds=cooldown)
        logger.info("closed paper trade %s %s pnl_idr=%s", symbol, exit_reason, pnl_idr)

    def _maybe_open_trade(self, symbol: str, quote: dict, feature: dict) -> None:
        now = datetime.now(UTC)
        cooldown_until = self.cooldown_until_by_symbol.get(symbol)
        if cooldown_until and now < cooldown_until:
            self._record_skip(symbol, quote, feature, "cooldown active")
            return
        feature_age = (now - feature["open_time"]).total_seconds()
        quote_age = (now - quote["timestamp"]).total_seconds()
        if feature_age > self.config.paper_trading.max_feature_age_seconds or quote_age > self.config.data.stale_data_seconds:
            self._record_skip(symbol, quote, feature, "stale market data")
            return

        signal = micro_momentum_burst_signal(feature, quote, self.config)
        daily_stats = self.repo.daily_stats(self.config.timezone)
        risk = RiskManager(self.config.risk, Decimal(str(self.config.starting_balance_idr)))
        decision = risk.evaluate_entry(
            realized_pnl_idr=Decimal(str(daily_stats["realized_pnl_idr"])),
            trade_count=int(daily_stats["trade_count"]),
            consecutive_losses=self.repo.consecutive_losses(self.config.timezone),
            spread_bps=Decimal(str(quote["spread_bps"])),
            has_take_profit=True,
            has_stop_loss=True,
        )
        if signal.decision != "TAKE":
            self._record_signal(symbol, signal, "SKIP", signal.reason)
            return
        if not decision.allowed:
            self._record_signal(symbol, signal, "SKIP", decision.reason)
            return

        ask = Decimal(str(quote["ask"]))
        entry_price = self._apply_slippage(ask, "buy")
        stop_loss_price = entry_price * (Decimal("1") - Decimal(str(self.config.paper_trading.stop_loss_bps)) / Decimal("10000"))
        take_profit_price = entry_price * (Decimal("1") + Decimal(str(self.config.paper_trading.take_profit_bps)) / Decimal("10000"))
        risk_per_unit_usdt = entry_price - stop_loss_price
        risk_idr = decision.risk_idr
        quantity_by_risk = risk_idr / Decimal(str(self.config.paper_trading.usdt_idr_rate)) / risk_per_unit_usdt
        quantity_by_cap = (decision.max_position_idr / Decimal(str(self.config.paper_trading.usdt_idr_rate))) / entry_price
        quantity = min(quantity_by_risk, quantity_by_cap)
        notional_idr = quantity * entry_price * Decimal(str(self.config.paper_trading.usdt_idr_rate))
        if quantity <= 0 or notional_idr <= 0:
            self._record_signal(symbol, signal, "SKIP", "invalid position size")
            return

        self.repo.insert_trade(
            symbol=symbol,
            side="long",
            entry_time=now,
            entry_price=entry_price,
            quantity=quantity,
            notional_idr=notional_idr,
            take_profit_price=take_profit_price,
            stop_loss_price=stop_loss_price,
            fee_estimate_idr=self._fee_idr(notional_idr, round_trips=1),
            slippage_estimate_idr=self._slippage_idr(notional_idr, round_trips=1),
        )
        self._record_signal(symbol, signal, "TAKE", None)
        logger.info("opened paper trade %s entry=%s tp=%s sl=%s", symbol, entry_price, take_profit_price, stop_loss_price)

    def _record_skip(self, symbol: str, quote: dict | None, feature: dict | None, reason: str) -> None:
        features = {"quote": quote or {}, "feature": feature or {}}
        self.repo.insert_signal(
            timestamp=datetime.now(UTC),
            symbol=symbol,
            strategy_name=self.config.paper_trading.strategy_name,
            side=None,
            confidence=Decimal("0"),
            reason=reason,
            features=features,
            decision="SKIP",
            skip_reason=reason,
        )

    def _record_signal(self, symbol: str, signal, decision: str, skip_reason: str | None) -> None:
        self.repo.insert_signal(
            timestamp=datetime.now(UTC),
            symbol=symbol,
            strategy_name=self.config.paper_trading.strategy_name,
            side=signal.side,
            confidence=signal.confidence,
            reason=signal.reason,
            features=signal.features,
            decision=decision,
            skip_reason=skip_reason,
        )

    def _apply_slippage(self, price: Decimal, side: str) -> Decimal:
        multiplier = Decimal(str(self.config.paper_trading.slippage_bps)) / Decimal("10000")
        if side == "buy":
            return price * (Decimal("1") + multiplier)
        return price * (Decimal("1") - multiplier)

    def _fee_idr(self, notional_idr: Decimal, *, round_trips: int) -> Decimal:
        return notional_idr * Decimal(str(self.config.paper_trading.fee_rate_bps)) / Decimal("10000") * Decimal(str(round_trips * 2))

    def _slippage_idr(self, notional_idr: Decimal, *, round_trips: int) -> Decimal:
        return notional_idr * Decimal(str(self.config.paper_trading.slippage_bps)) / Decimal("10000") * Decimal(str(round_trips * 2))

    def _write_health(self, status: str, message: str) -> None:
        self.health_repo.write_health(
            "paper_trader",
            status,
            message,
            timestamp=datetime.now(UTC),
            last_success_at=datetime.now(UTC) if status in {"ok", "standby"} else None,
            metadata_json={"mode": "paper", "live_trading": False, "enabled": self.config.paper_trading.enabled},
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
