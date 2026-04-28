from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


class PaperTradingRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def latest_feature(self, symbol: str) -> dict | None:
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT *
                    FROM market_features_1m
                    WHERE symbol = :symbol
                      AND open_time <= date_trunc('minute', now() - interval '1 minute')
                    ORDER BY open_time DESC
                    LIMIT 1
                    """
                ),
                {"symbol": symbol},
            ).mappings().first()
        return dict(row) if row else None

    def latest_quote(self, symbol: str) -> dict | None:
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT *
                    FROM market_quotes
                    WHERE symbol = :symbol
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """
                ),
                {"symbol": symbol},
            ).mappings().first()
        return dict(row) if row else None

    def recent_candles(self, symbol: str, limit: int = 60) -> pd.DataFrame:
        query = text(
            """
            SELECT open_time, open, high, low, close, volume
            FROM market_candles
            WHERE symbol = :symbol
            ORDER BY open_time DESC
            LIMIT :limit
            """
        )
        frame = pd.read_sql_query(query, self.engine, params={"symbol": symbol, "limit": limit})
        return frame.sort_values("open_time")

    def quote_path(self, symbol: str, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        query = text(
            """
            SELECT timestamp, bid, ask, spread_bps
            FROM market_quotes
            WHERE symbol = :symbol
              AND timestamp >= :start_time
              AND timestamp <= :end_time
            ORDER BY timestamp
            """
        )
        return pd.read_sql_query(
            query,
            self.engine,
            params={"symbol": symbol, "start_time": start_time, "end_time": end_time},
        )

    def sync_experiments(self, experiments: list[dict]) -> list[dict]:
        rows: list[dict] = []
        with self.engine.begin() as conn:
            for experiment in experiments:
                row = conn.execute(
                    text(
                        """
                        INSERT INTO experiments (name, strategy_name, status, config_json, updated_at)
                        VALUES (
                            :name,
                            :strategy_name,
                            :status,
                            CAST(:config_json AS jsonb),
                            now()
                        )
                        ON CONFLICT (name)
                        DO UPDATE SET
                            strategy_name = EXCLUDED.strategy_name,
                            status = EXCLUDED.status,
                            config_json = EXCLUDED.config_json,
                            updated_at = now()
                        RETURNING id, name, strategy_name, status
                        """
                    ),
                    {
                        "name": experiment["name"],
                        "strategy_name": experiment["strategy_name"],
                        "status": experiment["status"],
                        "config_json": json.dumps(experiment["config_json"], default=str),
                    },
                ).mappings().one()
                rows.append(dict(row))
        return rows

    def open_trade(self, symbol: str, experiment_id: int) -> dict | None:
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT *
                    FROM paper_trades
                    WHERE symbol = :symbol
                      AND experiment_id = :experiment_id
                      AND status = 'OPEN'
                    ORDER BY entry_time DESC
                    LIMIT 1
                    """
                ),
                {"symbol": symbol, "experiment_id": experiment_id},
            ).mappings().first()
        return dict(row) if row else None

    def trades_missing_analysis(self, limit: int = 100) -> list[dict]:
        with self.engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT *
                    FROM paper_trades
                    WHERE status = 'CLOSED'
                      AND exit_time IS NOT NULL
                      AND (
                        gross_pnl_idr IS NULL
                        OR gross_pnl_percent IS NULL
                        OR hold_seconds IS NULL
                        OR max_favorable_excursion_bps IS NULL
                        OR max_adverse_excursion_bps IS NULL
                        OR horizon_10m_label IS NULL
                      )
                    ORDER BY exit_time DESC
                    LIMIT :limit
                    """
                ),
                {"limit": limit},
            ).mappings().all()
        return [dict(row) for row in rows]

    def insert_signal(
        self,
        *,
        timestamp: datetime,
        symbol: str,
        experiment_id: int,
        experiment_name: str,
        strategy_name: str,
        side: str | None,
        confidence: Decimal,
        reason: str,
        features: dict,
        decision: str,
        skip_reason: str | None,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO paper_signals (
                        timestamp, symbol, experiment_id, experiment_name, strategy_name,
                        side, confidence, reason, features_json, decision, skip_reason
                    )
                    VALUES (
                        :timestamp, :symbol, :experiment_id, :experiment_name, :strategy_name,
                        :side, :confidence, :reason, CAST(:features_json AS jsonb),
                        :decision, :skip_reason
                    )
                    """
                ),
                {
                    "timestamp": timestamp,
                    "symbol": symbol,
                    "experiment_id": experiment_id,
                    "experiment_name": experiment_name,
                    "strategy_name": strategy_name,
                    "side": side,
                    "confidence": confidence,
                    "reason": reason,
                    "features_json": json.dumps(features, default=str),
                    "decision": decision,
                    "skip_reason": skip_reason,
                },
            )

    def insert_trade(
        self,
        *,
        experiment_id: int,
        experiment_name: str,
        strategy_name: str,
        symbol: str,
        side: str,
        entry_time: datetime,
        entry_price: Decimal,
        quantity: Decimal,
        notional_idr: Decimal,
        take_profit_price: Decimal,
        stop_loss_price: Decimal,
        fee_estimate_idr: Decimal,
        slippage_estimate_idr: Decimal,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO paper_trades (
                        experiment_id, experiment_name, strategy_name, symbol, side, status,
                        entry_time, entry_price, quantity, notional_idr, take_profit_price,
                        stop_loss_price, fee_estimate_idr, slippage_estimate_idr
                    )
                    VALUES (
                        :experiment_id, :experiment_name, :strategy_name, :symbol, :side, 'OPEN',
                        :entry_time, :entry_price, :quantity, :notional_idr, :take_profit_price, :stop_loss_price,
                        :fee_estimate_idr, :slippage_estimate_idr
                    )
                    """
                ),
                {
                    "experiment_id": experiment_id,
                    "experiment_name": experiment_name,
                    "strategy_name": strategy_name,
                    "symbol": symbol,
                    "side": side,
                    "entry_time": entry_time,
                    "entry_price": entry_price,
                    "quantity": quantity,
                    "notional_idr": notional_idr,
                    "take_profit_price": take_profit_price,
                    "stop_loss_price": stop_loss_price,
                    "fee_estimate_idr": fee_estimate_idr,
                    "slippage_estimate_idr": slippage_estimate_idr,
                },
            )

    def close_trade(
        self,
        *,
        trade_id: int,
        exit_time: datetime,
        exit_price: Decimal,
        pnl_idr: Decimal,
        pnl_percent: Decimal,
        fee_estimate_idr: Decimal,
        slippage_estimate_idr: Decimal,
        exit_reason: str,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE paper_trades
                    SET
                        status = 'CLOSED',
                        exit_time = :exit_time,
                        exit_price = :exit_price,
                        pnl_idr = :pnl_idr,
                        pnl_percent = :pnl_percent,
                        fee_estimate_idr = :fee_estimate_idr,
                        slippage_estimate_idr = :slippage_estimate_idr,
                        exit_reason = :exit_reason
                    WHERE id = :trade_id
                    """
                ),
                {
                    "trade_id": trade_id,
                    "exit_time": exit_time,
                    "exit_price": exit_price,
                    "pnl_idr": pnl_idr,
                    "pnl_percent": pnl_percent,
                    "fee_estimate_idr": fee_estimate_idr,
                    "slippage_estimate_idr": slippage_estimate_idr,
                    "exit_reason": exit_reason,
                },
            )

    def update_trade_analysis(
        self,
        *,
        trade_id: int,
        gross_pnl_idr: Decimal,
        gross_pnl_percent: Decimal,
        hold_seconds: int,
        max_favorable_excursion_bps: Decimal | None,
        max_adverse_excursion_bps: Decimal | None,
        horizon_3m_label: str | None,
        horizon_5m_label: str | None,
        horizon_10m_label: str | None,
        label_source: str,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE paper_trades
                    SET
                        gross_pnl_idr = :gross_pnl_idr,
                        gross_pnl_percent = :gross_pnl_percent,
                        hold_seconds = :hold_seconds,
                        max_favorable_excursion_bps = :max_favorable_excursion_bps,
                        max_adverse_excursion_bps = :max_adverse_excursion_bps,
                        horizon_3m_label = :horizon_3m_label,
                        horizon_5m_label = :horizon_5m_label,
                        horizon_10m_label = :horizon_10m_label,
                        label_source = :label_source
                    WHERE id = :trade_id
                    """
                ),
                {
                    "trade_id": trade_id,
                    "gross_pnl_idr": gross_pnl_idr,
                    "gross_pnl_percent": gross_pnl_percent,
                    "hold_seconds": hold_seconds,
                    "max_favorable_excursion_bps": max_favorable_excursion_bps,
                    "max_adverse_excursion_bps": max_adverse_excursion_bps,
                    "horizon_3m_label": horizon_3m_label,
                    "horizon_5m_label": horizon_5m_label,
                    "horizon_10m_label": horizon_10m_label,
                    "label_source": label_source,
                },
            )

    def daily_stats(self, timezone: str, experiment_id: int) -> dict:
        trading_date = datetime.now(ZoneInfo(timezone)).date()
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT
                        coalesce(sum(pnl_idr) FILTER (WHERE status = 'CLOSED'), 0) AS realized_pnl_idr,
                        count(*) FILTER (WHERE entry_time IS NOT NULL) AS trade_count
                    FROM paper_trades
                    WHERE (entry_time AT TIME ZONE :timezone)::date = :trading_date
                      AND experiment_id = :experiment_id
                    """
                ),
                {"timezone": timezone, "trading_date": trading_date, "experiment_id": experiment_id},
            ).mappings().one()
        return {"realized_pnl_idr": row["realized_pnl_idr"], "trade_count": row["trade_count"]}

    def consecutive_losses(self, timezone: str, experiment_id: int) -> int:
        trading_date = datetime.now(ZoneInfo(timezone)).date()
        with self.engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT pnl_idr
                    FROM paper_trades
                    WHERE status = 'CLOSED'
                      AND (entry_time AT TIME ZONE :timezone)::date = :trading_date
                      AND experiment_id = :experiment_id
                    ORDER BY exit_time DESC
                    LIMIT 20
                    """
                ),
                {"timezone": timezone, "trading_date": trading_date, "experiment_id": experiment_id},
            ).all()
        losses = 0
        for row in rows:
            pnl = row[0]
            if pnl is not None and pnl < 0:
                losses += 1
            else:
                break
        return losses
