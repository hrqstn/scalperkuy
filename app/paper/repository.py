from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

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

    def open_trade(self, symbol: str) -> dict | None:
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT *
                    FROM paper_trades
                    WHERE symbol = :symbol AND status = 'OPEN'
                    ORDER BY entry_time DESC
                    LIMIT 1
                    """
                ),
                {"symbol": symbol},
            ).mappings().first()
        return dict(row) if row else None

    def insert_signal(
        self,
        *,
        timestamp: datetime,
        symbol: str,
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
                        timestamp, symbol, strategy_name, side, confidence, reason,
                        features_json, decision, skip_reason
                    )
                    VALUES (
                        :timestamp, :symbol, :strategy_name, :side, :confidence, :reason,
                        CAST(:features_json AS jsonb), :decision, :skip_reason
                    )
                    """
                ),
                {
                    "timestamp": timestamp,
                    "symbol": symbol,
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
                        symbol, side, status, entry_time, entry_price, quantity,
                        notional_idr, take_profit_price, stop_loss_price,
                        fee_estimate_idr, slippage_estimate_idr
                    )
                    VALUES (
                        :symbol, :side, 'OPEN', :entry_time, :entry_price, :quantity,
                        :notional_idr, :take_profit_price, :stop_loss_price,
                        :fee_estimate_idr, :slippage_estimate_idr
                    )
                    """
                ),
                {
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

    def daily_stats(self, timezone: str) -> dict:
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
                    """
                ),
                {"timezone": timezone, "trading_date": trading_date},
            ).mappings().one()
        return {"realized_pnl_idr": row["realized_pnl_idr"], "trade_count": row["trade_count"]}

    def consecutive_losses(self) -> int:
        with self.engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT pnl_idr
                    FROM paper_trades
                    WHERE status = 'CLOSED'
                    ORDER BY exit_time DESC
                    LIMIT 20
                    """
                )
            ).all()
        losses = 0
        for row in rows:
            pnl = row[0]
            if pnl is not None and pnl < 0:
                losses += 1
            else:
                break
        return losses
