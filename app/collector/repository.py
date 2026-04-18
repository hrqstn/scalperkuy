from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.exchange.base import Candle, OrderBookSnapshot, Quote, RecentTrade


class MarketDataRepository:
    def __init__(self, engine: Engine, exchange: str) -> None:
        self.engine = engine
        self.exchange = exchange

    def save_candles(self, candles: list[Candle]) -> int:
        if not candles:
            return 0
        statement = text(
            """
            INSERT INTO market_candles (
                exchange, symbol, timeframe, open_time, close_time,
                open, high, low, close, volume
            )
            VALUES (
                :exchange, :symbol, :timeframe, :open_time, :close_time,
                :open, :high, :low, :close, :volume
            )
            ON CONFLICT (exchange, symbol, timeframe, open_time)
            DO UPDATE SET
                close_time = EXCLUDED.close_time,
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
            """
        )
        rows = [{**candle.__dict__, "exchange": self.exchange} for candle in candles]
        with self.engine.begin() as conn:
            conn.execute(statement, rows)
        return len(rows)

    def save_quote(self, quote: Quote) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO market_quotes (exchange, symbol, timestamp, bid, ask, spread, spread_bps)
                    VALUES (:exchange, :symbol, :timestamp, :bid, :ask, :spread, :spread_bps)
                    """
                ),
                {**quote.__dict__, "exchange": self.exchange},
            )

    def save_trades(self, trades: list[RecentTrade]) -> int:
        if not trades:
            return 0
        statement = text(
            """
            INSERT INTO market_trades (exchange, symbol, trade_id, timestamp, price, amount, side)
            VALUES (:exchange, :symbol, :trade_id, :timestamp, :price, :amount, :side)
            ON CONFLICT (exchange, symbol, trade_id) DO NOTHING
            """
        )
        rows = [{**trade.__dict__, "exchange": self.exchange} for trade in trades]
        with self.engine.begin() as conn:
            conn.execute(statement, rows)
        return len(rows)

    def save_order_book(self, snapshot: OrderBookSnapshot) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO order_book_snapshots (
                        exchange, symbol, timestamp, depth, bids_json, asks_json,
                        best_bid, best_ask, spread, imbalance
                    )
                    VALUES (
                        :exchange, :symbol, :timestamp, :depth, CAST(:bids_json AS jsonb), CAST(:asks_json AS jsonb),
                        :best_bid, :best_ask, :spread, :imbalance
                    )
                    """
                ),
                {
                    "exchange": self.exchange,
                    "symbol": snapshot.symbol,
                    "timestamp": snapshot.timestamp,
                    "depth": snapshot.depth,
                    "bids_json": json.dumps(snapshot.bids),
                    "asks_json": json.dumps(snapshot.asks),
                    "best_bid": snapshot.best_bid,
                    "best_ask": snapshot.best_ask,
                    "spread": snapshot.spread,
                    "imbalance": snapshot.imbalance,
                },
            )

    def write_health(
        self,
        service_name: str,
        status: str,
        message: str,
        *,
        timestamp: datetime,
        last_success_at: datetime | None = None,
        metadata_json: dict | None = None,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO service_health (
                        service_name, timestamp, status, last_success_at, message, metadata_json
                    )
                    VALUES (
                        :service_name, :timestamp, :status, :last_success_at, :message, CAST(:metadata_json AS jsonb)
                    )
                    """
                ),
                {
                    "service_name": service_name,
                    "timestamp": timestamp,
                    "status": status,
                    "last_success_at": last_success_at,
                    "message": message,
                    "metadata_json": json.dumps(metadata_json or {}),
                },
            )
