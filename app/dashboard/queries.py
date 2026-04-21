from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def latest_service_health(engine: Engine) -> pd.DataFrame:
    query = text(
        """
        SELECT DISTINCT ON (service_name)
            service_name, timestamp, status, last_success_at, message
        FROM service_health
        ORDER BY service_name, timestamp DESC
        """
    )
    return pd.read_sql_query(query, engine)


def latest_quotes(engine: Engine) -> pd.DataFrame:
    query = text(
        """
        SELECT DISTINCT ON (symbol)
            symbol, timestamp, bid, ask, spread, spread_bps
        FROM market_quotes
        ORDER BY symbol, timestamp DESC
        """
    )
    return pd.read_sql_query(query, engine)


def latest_candles(engine: Engine) -> pd.DataFrame:
    query = text(
        """
        SELECT DISTINCT ON (symbol)
            symbol, open_time, close_time, open, high, low, close, volume
        FROM market_candles
        ORDER BY symbol, open_time DESC
        """
    )
    return pd.read_sql_query(query, engine)


def candle_history(engine: Engine, symbol: str, limit: int = 120) -> pd.DataFrame:
    query = text(
        """
        SELECT open_time, open, high, low, close, volume
        FROM market_candles
        WHERE symbol = :symbol
        ORDER BY open_time DESC
        LIMIT :limit
        """
    )
    frame = pd.read_sql_query(query, engine, params={"symbol": symbol, "limit": limit})
    return frame.sort_values("open_time")


def table_counts(engine: Engine) -> pd.DataFrame:
    query = text(
        """
        SELECT 'market_candles' AS table_name, count(*) AS rows FROM market_candles
        UNION ALL SELECT 'market_quotes', count(*) FROM market_quotes
        UNION ALL SELECT 'market_trades', count(*) FROM market_trades
        UNION ALL SELECT 'order_book_snapshots', count(*) FROM order_book_snapshots
        UNION ALL SELECT 'market_features_1m', count(*) FROM market_features_1m
        UNION ALL SELECT 'paper_signals', count(*) FROM paper_signals
        UNION ALL SELECT 'paper_trades', count(*) FROM paper_trades
        UNION ALL SELECT 'service_health', count(*) FROM service_health
        ORDER BY table_name
        """
    )
    return pd.read_sql_query(query, engine)


def market_data_freshness(engine: Engine, stale_threshold_seconds: int, symbols: list[str]) -> pd.DataFrame:
    query = text(
        """
        WITH latest AS (
            SELECT 'candles' AS feed, symbol, max(open_time) AS latest_at FROM market_candles GROUP BY symbol
            UNION ALL
            SELECT 'quotes' AS feed, symbol, max(timestamp) AS latest_at FROM market_quotes GROUP BY symbol
            UNION ALL
            SELECT 'trades' AS feed, symbol, max(timestamp) AS latest_at FROM market_trades GROUP BY symbol
            UNION ALL
            SELECT 'order_books' AS feed, symbol, max(timestamp) AS latest_at FROM order_book_snapshots GROUP BY symbol
        )
        SELECT
            feed,
            symbol,
            latest_at,
            greatest(0, round(extract(epoch FROM (now() - latest_at)))::integer) AS age_seconds,
            CASE
                WHEN latest_at IS NULL THEN 'missing'
                WHEN extract(epoch FROM (now() - latest_at)) > :stale_threshold_seconds THEN 'stale'
                ELSE 'fresh'
            END AS status
        FROM latest
        ORDER BY feed, symbol
        """
    )
    frame = pd.read_sql_query(query, engine, params={"stale_threshold_seconds": stale_threshold_seconds})
    expected = pd.MultiIndex.from_product(
        [["candles", "quotes", "trades", "order_books"], symbols],
        names=["feed", "symbol"],
    ).to_frame(index=False)
    merged = expected.merge(frame, how="left", on=["feed", "symbol"])
    merged["status"] = merged["status"].fillna("missing")
    return merged.sort_values(["feed", "symbol"])


def recent_trades(engine: Engine, limit: int = 25) -> pd.DataFrame:
    query = text(
        """
        SELECT
            id,
            symbol,
            side,
            status,
            entry_time,
            exit_time,
            entry_price,
            exit_price,
            quantity,
            notional_idr,
            take_profit_price,
            stop_loss_price,
            pnl_idr,
            pnl_percent,
            fee_estimate_idr,
            slippage_estimate_idr,
            exit_reason
        FROM paper_trades
        ORDER BY created_at DESC
        LIMIT :limit
        """
    )
    return pd.read_sql_query(query, engine, params={"limit": limit})


def open_positions(engine: Engine) -> pd.DataFrame:
    query = text(
        """
        SELECT
            id,
            symbol,
            side,
            entry_time,
            entry_price,
            quantity,
            notional_idr,
            take_profit_price,
            stop_loss_price,
            fee_estimate_idr,
            slippage_estimate_idr
        FROM paper_trades
        WHERE status = 'OPEN'
        ORDER BY entry_time DESC
        """
    )
    return pd.read_sql_query(query, engine)


def signal_summary(engine: Engine, limit_hours: int = 24) -> pd.DataFrame:
    query = text(
        """
        SELECT
            decision,
            coalesce(skip_reason, 'TAKE') AS reason,
            count(*) AS rows
        FROM paper_signals
        WHERE timestamp >= now() - (:limit_hours * interval '1 hour')
        GROUP BY decision, coalesce(skip_reason, 'TAKE')
        ORDER BY rows DESC
        LIMIT 25
        """
    )
    return pd.read_sql_query(query, engine, params={"limit_hours": limit_hours})


def recent_signals(engine: Engine, limit: int = 50) -> pd.DataFrame:
    query = text(
        """
        SELECT
            timestamp,
            symbol,
            strategy_name,
            decision,
            side,
            confidence,
            reason,
            skip_reason
        FROM paper_signals
        ORDER BY timestamp DESC
        LIMIT :limit
        """
    )
    return pd.read_sql_query(query, engine, params={"limit": limit})


def paper_performance(engine: Engine) -> pd.DataFrame:
    query = text(
        """
        SELECT
            count(*) FILTER (WHERE status = 'CLOSED') AS closed_trades,
            count(*) FILTER (WHERE status = 'OPEN') AS open_trades,
            coalesce(sum(pnl_idr) FILTER (WHERE status = 'CLOSED'), 0) AS realized_pnl_idr,
            coalesce(avg(pnl_percent) FILTER (WHERE status = 'CLOSED'), 0) AS avg_pnl_percent,
            count(*) FILTER (WHERE status = 'CLOSED' AND pnl_idr > 0) AS wins,
            count(*) FILTER (WHERE status = 'CLOSED' AND pnl_idr < 0) AS losses,
            coalesce(sum(pnl_idr) FILTER (WHERE status = 'CLOSED' AND pnl_idr > 0), 0) AS gross_profit_idr,
            abs(coalesce(sum(pnl_idr) FILTER (WHERE status = 'CLOSED' AND pnl_idr < 0), 0)) AS gross_loss_idr,
            coalesce(sum(fee_estimate_idr) FILTER (WHERE status = 'CLOSED'), 0) AS fees_idr,
            coalesce(sum(slippage_estimate_idr) FILTER (WHERE status = 'CLOSED'), 0) AS slippage_idr
        FROM paper_trades
        """
    )
    return pd.read_sql_query(query, engine)


def equity_curve(engine: Engine) -> pd.DataFrame:
    query = text(
        """
        SELECT
            exit_time,
            pnl_idr,
            sum(pnl_idr) OVER (ORDER BY exit_time, id) AS cumulative_pnl_idr
        FROM paper_trades
        WHERE status = 'CLOSED' AND exit_time IS NOT NULL
        ORDER BY exit_time
        """
    )
    return pd.read_sql_query(query, engine)


def recent_health_events(engine: Engine, limit: int = 25) -> pd.DataFrame:
    query = text(
        """
        SELECT timestamp, service_name, status, message
        FROM service_health
        ORDER BY timestamp DESC
        LIMIT :limit
        """
    )
    return pd.read_sql_query(query, engine, params={"limit": limit})
