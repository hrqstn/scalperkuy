from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol


@dataclass(frozen=True)
class Candle:
    symbol: str
    timeframe: str
    open_time: datetime
    close_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal


@dataclass(frozen=True)
class Quote:
    symbol: str
    timestamp: datetime
    bid: Decimal
    ask: Decimal
    spread: Decimal
    spread_bps: Decimal


@dataclass(frozen=True)
class RecentTrade:
    symbol: str
    trade_id: str
    timestamp: datetime
    price: Decimal
    amount: Decimal
    side: str | None


@dataclass(frozen=True)
class OrderBookSnapshot:
    symbol: str
    timestamp: datetime
    depth: int
    bids: list[list[str]]
    asks: list[list[str]]
    best_bid: Decimal
    best_ask: Decimal
    spread: Decimal
    imbalance: Decimal


class MarketDataAdapter(Protocol):
    def fetch_recent_candles(self, symbol: str, timeframe: str, limit: int = 5) -> list[Candle]:
        ...

    def fetch_quote(self, symbol: str) -> Quote:
        ...

    def fetch_recent_trades(self, symbol: str, limit: int = 50) -> list[RecentTrade]:
        ...

    def fetch_order_book(self, symbol: str, depth: int = 20) -> OrderBookSnapshot:
        ...
