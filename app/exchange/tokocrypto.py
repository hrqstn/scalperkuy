from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx

from app.config import AppConfig, exchange_symbol
from app.exchange.base import Candle, OrderBookSnapshot, Quote, RecentTrade


def _dt_from_ms(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=UTC)


class TokocryptoAdapter:
    """Public market data adapter.

    Tokocrypto's public REST API is Binance-style. Keep all endpoint details here
    so the collector and paper trader do not depend on exchange quirks.
    """

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.client = httpx.Client(
            base_url=config.tokocrypto.base_url,
            timeout=config.tokocrypto.request_timeout_seconds,
            headers={"User-Agent": "scalperkuy-data-collector/0.1"},
        )

    def fetch_recent_candles(self, symbol: str, timeframe: str, limit: int = 5) -> list[Candle]:
        rows = self._get_json(
            "/api/v3/klines",
            params={"symbol": exchange_symbol(symbol), "interval": timeframe, "limit": limit},
        )
        return [self._parse_candle(symbol, timeframe, row) for row in rows]

    def fetch_quote(self, symbol: str) -> Quote:
        data = self._get_json("/api/v3/ticker/bookTicker", params={"symbol": exchange_symbol(symbol)})
        bid = Decimal(str(data["bidPrice"]))
        ask = Decimal(str(data["askPrice"]))
        spread = ask - bid
        mid = (ask + bid) / Decimal("2")
        spread_bps = (spread / mid * Decimal("10000")) if mid > 0 else Decimal("0")
        return Quote(
            symbol=symbol,
            timestamp=datetime.now(UTC),
            bid=bid,
            ask=ask,
            spread=spread,
            spread_bps=spread_bps,
        )

    def fetch_recent_trades(self, symbol: str, limit: int = 50) -> list[RecentTrade]:
        rows = self._get_json("/api/v3/trades", params={"symbol": exchange_symbol(symbol), "limit": limit})
        return [self._parse_trade(symbol, item) for item in rows]

    def fetch_order_book(self, symbol: str, depth: int = 20) -> OrderBookSnapshot:
        data = self._get_json("/api/v3/depth", params={"symbol": exchange_symbol(symbol), "limit": depth})
        bids = data.get("bids", [])[:depth]
        asks = data.get("asks", [])[:depth]
        if not bids or not asks:
            raise ValueError(f"empty order book for {symbol}")
        best_bid = Decimal(str(bids[0][0]))
        best_ask = Decimal(str(asks[0][0]))
        spread = best_ask - best_bid
        bid_size = sum(Decimal(str(row[1])) for row in bids)
        ask_size = sum(Decimal(str(row[1])) for row in asks)
        total_size = bid_size + ask_size
        imbalance = ((bid_size - ask_size) / total_size) if total_size > 0 else Decimal("0")
        return OrderBookSnapshot(
            symbol=symbol,
            timestamp=datetime.now(UTC),
            depth=depth,
            bids=bids,
            asks=asks,
            best_bid=best_bid,
            best_ask=best_ask,
            spread=spread,
            imbalance=imbalance,
        )

    def close(self) -> None:
        self.client.close()

    def _get_json(self, path: str, params: dict[str, Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(1, self.config.tokocrypto.max_retries + 1):
            try:
                response = self.client.get(path, params=params)
                if response.status_code in {408, 429} or response.status_code >= 500:
                    response.raise_for_status()
                elif response.status_code >= 400:
                    response.raise_for_status()
                return response.json()
            except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError, httpx.HTTPStatusError) as exc:
                last_error = exc
                if not self._should_retry(exc) or attempt >= self.config.tokocrypto.max_retries:
                    break
                time.sleep(self.config.tokocrypto.retry_backoff_seconds * attempt)
        if last_error:
            raise last_error
        raise RuntimeError(f"Tokocrypto request failed for {path}")

    @staticmethod
    def _should_retry(exc: Exception) -> bool:
        if isinstance(exc, httpx.HTTPStatusError):
            status_code = exc.response.status_code
            return status_code in {408, 429} or status_code >= 500
        return True

    @staticmethod
    def _parse_candle(symbol: str, timeframe: str, row: list[Any]) -> Candle:
        return Candle(
            symbol=symbol,
            timeframe=timeframe,
            open_time=_dt_from_ms(int(row[0])),
            close_time=_dt_from_ms(int(row[6])),
            open=Decimal(str(row[1])),
            high=Decimal(str(row[2])),
            low=Decimal(str(row[3])),
            close=Decimal(str(row[4])),
            volume=Decimal(str(row[5])),
        )

    @staticmethod
    def _parse_trade(symbol: str, item: dict[str, Any]) -> RecentTrade:
        side = "sell" if item.get("isBuyerMaker") else "buy"
        return RecentTrade(
            symbol=symbol,
            trade_id=str(item["id"]),
            timestamp=_dt_from_ms(int(item["time"])),
            price=Decimal(str(item["price"])),
            amount=Decimal(str(item["qty"])),
            side=side,
        )
