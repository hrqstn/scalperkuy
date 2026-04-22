from __future__ import annotations

import logging
import signal
import time
from datetime import UTC, datetime

from sqlalchemy import text

from app.collector.repository import MarketDataRepository
from app.config import load_config
from app.db.models import init_db
from app.db.session import get_engine


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("aggregator")


class AggregatorService:
    def __init__(self) -> None:
        self.config = load_config()
        self.engine = get_engine()
        init_db(self.engine)
        self.repo = MarketDataRepository(self.engine, self.config.exchange)
        self.running = True

    def run(self) -> None:
        self._install_signal_handlers()
        self._write_health("starting", "aggregator starting")
        while self.running:
            try:
                if self.config.aggregation.enabled:
                    rows = self.aggregate_features_1m()
                    self._write_health("ok", f"aggregated {rows} market feature rows")
                    logger.info("aggregated %s market feature rows", rows)
                else:
                    self._write_health("standby", "aggregation disabled")
            except Exception as exc:
                logger.exception("aggregation failed")
                self._write_health("error", f"aggregation failed: {exc}")
            time.sleep(self.config.aggregation.interval_seconds)
        self._write_health("stopped", "aggregator stopped")

    def aggregate_features_1m(self) -> int:
        statement = text(
            """
            WITH candle AS (
                SELECT
                    exchange,
                    symbol,
                    open_time,
                    open AS candle_open,
                    high AS candle_high,
                    low AS candle_low,
                    close AS candle_close,
                    volume AS candle_volume,
                    CASE
                        WHEN open > 0 THEN abs(high - low) / open * 10000
                        ELSE NULL
                    END AS volatility_1m
                FROM market_candles
                WHERE open_time >= date_trunc('minute', now() - (:lookback_minutes * interval '1 minute'))
            ),
            quotes AS (
                SELECT
                    exchange,
                    symbol,
                    date_trunc('minute', timestamp) AS open_time,
                    count(*)::integer AS quote_count,
                    avg((bid + ask) / 2) AS mid_price_avg,
                    avg(spread) AS spread_avg,
                    min(spread) AS spread_min,
                    max(spread) AS spread_max,
                    avg(spread_bps) AS spread_bps_avg
                FROM market_quotes
                WHERE timestamp >= date_trunc('minute', now() - (:lookback_minutes * interval '1 minute'))
                GROUP BY exchange, symbol, date_trunc('minute', timestamp)
            ),
            trades AS (
                SELECT
                    exchange,
                    symbol,
                    date_trunc('minute', timestamp) AS open_time,
                    count(*)::integer AS trade_count,
                    count(*) FILTER (WHERE side = 'buy')::integer AS trade_buy_count,
                    count(*) FILTER (WHERE side = 'sell')::integer AS trade_sell_count,
                    coalesce(sum(amount) FILTER (WHERE side = 'buy'), 0) AS buy_volume,
                    coalesce(sum(amount) FILTER (WHERE side = 'sell'), 0) AS sell_volume,
                    coalesce(sum(amount), 0) AS trade_volume,
                    coalesce(sum(price * amount), 0) AS trade_notional
                FROM market_trades
                WHERE timestamp >= date_trunc('minute', now() - (:lookback_minutes * interval '1 minute'))
                GROUP BY exchange, symbol, date_trunc('minute', timestamp)
            ),
            order_books_enriched AS (
                SELECT
                    exchange,
                    symbol,
                    date_trunc('minute', timestamp) AS open_time,
                    imbalance,
                    (
                        SELECT coalesce(sum((level.value->>1)::numeric), 0)
                        FROM jsonb_array_elements(bids_json) AS level(value)
                    ) AS bid_depth_top20,
                    (
                        SELECT coalesce(sum((level.value->>1)::numeric), 0)
                        FROM jsonb_array_elements(asks_json) AS level(value)
                    ) AS ask_depth_top20
                FROM order_book_snapshots
                WHERE timestamp >= date_trunc('minute', now() - (:lookback_minutes * interval '1 minute'))
            ),
            order_books AS (
                SELECT
                    exchange,
                    symbol,
                    open_time,
                    count(*)::integer AS order_book_count,
                    avg(imbalance) AS orderbook_imbalance_avg,
                    min(imbalance) AS orderbook_imbalance_min,
                    max(imbalance) AS orderbook_imbalance_max,
                    avg(bid_depth_top20) AS bid_depth_top20_avg,
                    avg(ask_depth_top20) AS ask_depth_top20_avg
                FROM order_books_enriched
                GROUP BY exchange, symbol, open_time
            ),
            bucket AS (
                SELECT exchange, symbol, open_time FROM candle
                UNION
                SELECT exchange, symbol, open_time FROM quotes
                UNION
                SELECT exchange, symbol, open_time FROM trades
                UNION
                SELECT exchange, symbol, open_time FROM order_books
            ),
            feature_rows AS (
                SELECT
                    bucket.exchange,
                    bucket.symbol,
                    bucket.open_time,
                    candle.candle_open,
                    candle.candle_high,
                    candle.candle_low,
                    candle.candle_close,
                    candle.candle_volume,
                    coalesce(quotes.quote_count, 0) AS quote_count,
                    coalesce(trades.trade_count, 0) AS trade_count,
                    coalesce(order_books.order_book_count, 0) AS order_book_count,
                    quotes.mid_price_avg,
                    quotes.spread_avg,
                    quotes.spread_min,
                    quotes.spread_max,
                    quotes.spread_bps_avg,
                    coalesce(trades.trade_buy_count, 0) AS trade_buy_count,
                    coalesce(trades.trade_sell_count, 0) AS trade_sell_count,
                    coalesce(trades.buy_volume, 0) AS buy_volume,
                    coalesce(trades.sell_volume, 0) AS sell_volume,
                    coalesce(trades.trade_volume, 0) AS trade_volume,
                    coalesce(trades.trade_notional, 0) AS trade_notional,
                    CASE
                        WHEN coalesce(trades.buy_volume, 0) + coalesce(trades.sell_volume, 0) > 0
                        THEN (coalesce(trades.buy_volume, 0) - coalesce(trades.sell_volume, 0))
                            / (coalesce(trades.buy_volume, 0) + coalesce(trades.sell_volume, 0))
                        ELSE NULL
                    END AS trade_flow_imbalance,
                    order_books.orderbook_imbalance_avg,
                    order_books.orderbook_imbalance_min,
                    order_books.orderbook_imbalance_max,
                    order_books.bid_depth_top20_avg,
                    order_books.ask_depth_top20_avg,
                    candle.volatility_1m
                FROM bucket
                LEFT JOIN candle USING (exchange, symbol, open_time)
                LEFT JOIN quotes USING (exchange, symbol, open_time)
                LEFT JOIN trades USING (exchange, symbol, open_time)
                LEFT JOIN order_books USING (exchange, symbol, open_time)
            ),
            quality_rows AS (
                SELECT
                    feature_rows.*,
                    array_remove(
                        ARRAY[
                            CASE WHEN candle_close IS NULL THEN 'missing_candle' END,
                            CASE WHEN quote_count < :min_quote_count THEN 'low_quote_samples' END,
                            CASE WHEN trade_count < :min_trade_count THEN 'low_trade_samples' END,
                            CASE WHEN order_book_count < :min_order_book_count THEN 'low_order_book_samples' END,
                            CASE WHEN coalesce(spread_bps_avg, 0) > :max_spread_bps THEN 'spread_too_wide' END,
                            CASE WHEN coalesce(volatility_1m, 0) < :min_volatility_bps THEN 'volatility_too_low' END
                        ]::text[],
                        NULL
                    ) AS quality_flags_array
                FROM feature_rows
            )
            INSERT INTO market_features_1m (
                exchange,
                symbol,
                open_time,
                candle_open,
                candle_high,
                candle_low,
                candle_close,
                candle_volume,
                quote_count,
                trade_count,
                order_book_count,
                mid_price_avg,
                spread_avg,
                spread_min,
                spread_max,
                spread_bps_avg,
                trade_buy_count,
                trade_sell_count,
                buy_volume,
                sell_volume,
                trade_volume,
                trade_notional,
                trade_flow_imbalance,
                orderbook_imbalance_avg,
                orderbook_imbalance_min,
                orderbook_imbalance_max,
                bid_depth_top20_avg,
                ask_depth_top20_avg,
                volatility_1m,
                quality_score,
                is_tradeable_minute,
                quality_flags,
                updated_at
            )
            SELECT
                exchange,
                symbol,
                open_time,
                candle_open,
                candle_high,
                candle_low,
                candle_close,
                candle_volume,
                quote_count,
                trade_count,
                order_book_count,
                mid_price_avg,
                spread_avg,
                spread_min,
                spread_max,
                spread_bps_avg,
                trade_buy_count,
                trade_sell_count,
                buy_volume,
                sell_volume,
                trade_volume,
                trade_notional,
                trade_flow_imbalance,
                orderbook_imbalance_avg,
                orderbook_imbalance_min,
                orderbook_imbalance_max,
                bid_depth_top20_avg,
                ask_depth_top20_avg,
                volatility_1m,
                greatest(0, 100 - 20 * cardinality(quality_flags_array))::numeric(5, 2) AS quality_score,
                cardinality(quality_flags_array) = 0 AS is_tradeable_minute,
                to_jsonb(quality_flags_array) AS quality_flags,
                now()
            FROM quality_rows
            ON CONFLICT (exchange, symbol, open_time)
            DO UPDATE SET
                candle_open = EXCLUDED.candle_open,
                candle_high = EXCLUDED.candle_high,
                candle_low = EXCLUDED.candle_low,
                candle_close = EXCLUDED.candle_close,
                candle_volume = EXCLUDED.candle_volume,
                quote_count = EXCLUDED.quote_count,
                trade_count = EXCLUDED.trade_count,
                order_book_count = EXCLUDED.order_book_count,
                mid_price_avg = EXCLUDED.mid_price_avg,
                spread_avg = EXCLUDED.spread_avg,
                spread_min = EXCLUDED.spread_min,
                spread_max = EXCLUDED.spread_max,
                spread_bps_avg = EXCLUDED.spread_bps_avg,
                trade_buy_count = EXCLUDED.trade_buy_count,
                trade_sell_count = EXCLUDED.trade_sell_count,
                buy_volume = EXCLUDED.buy_volume,
                sell_volume = EXCLUDED.sell_volume,
                trade_volume = EXCLUDED.trade_volume,
                trade_notional = EXCLUDED.trade_notional,
                trade_flow_imbalance = EXCLUDED.trade_flow_imbalance,
                orderbook_imbalance_avg = EXCLUDED.orderbook_imbalance_avg,
                orderbook_imbalance_min = EXCLUDED.orderbook_imbalance_min,
                orderbook_imbalance_max = EXCLUDED.orderbook_imbalance_max,
                bid_depth_top20_avg = EXCLUDED.bid_depth_top20_avg,
                ask_depth_top20_avg = EXCLUDED.ask_depth_top20_avg,
                volatility_1m = EXCLUDED.volatility_1m,
                quality_score = EXCLUDED.quality_score,
                is_tradeable_minute = EXCLUDED.is_tradeable_minute,
                quality_flags = EXCLUDED.quality_flags,
                updated_at = now()
            RETURNING id
            """
        )
        with self.engine.begin() as conn:
            rows = conn.execute(
                statement,
                {
                    "lookback_minutes": self.config.aggregation.lookback_minutes,
                    "min_quote_count": self.config.paper_trading.min_quote_count,
                    "min_trade_count": self.config.paper_trading.min_trade_count,
                    "min_order_book_count": self.config.paper_trading.min_order_book_count,
                    "max_spread_bps": self.config.risk.max_spread_bps,
                    "min_volatility_bps": self.config.paper_trading.min_volatility_bps,
                },
            ).fetchall()
        return len(rows)

    def _write_health(self, status: str, message: str) -> None:
        self.repo.write_health(
            "aggregator",
            status,
            message,
            timestamp=datetime.now(UTC),
            last_success_at=datetime.now(UTC) if status == "ok" else None,
            metadata_json={
                "enabled": self.config.aggregation.enabled,
                "lookback_minutes": self.config.aggregation.lookback_minutes,
            },
        )

    def _install_signal_handlers(self) -> None:
        def stop(_signum: int, _frame: object) -> None:
            self.running = False

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)


def main() -> None:
    AggregatorService().run()


if __name__ == "__main__":
    main()
