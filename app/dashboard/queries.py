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
        UNION ALL SELECT 'paper_signals', count(*) FROM paper_signals
        UNION ALL SELECT 'paper_trades', count(*) FROM paper_trades
        UNION ALL SELECT 'service_health', count(*) FROM service_health
        ORDER BY table_name
        """
    )
    return pd.read_sql_query(query, engine)


def recent_trades(engine: Engine, limit: int = 25) -> pd.DataFrame:
    query = text(
        """
        SELECT symbol, side, status, entry_time, exit_time, entry_price, exit_price, pnl_idr, exit_reason
        FROM paper_trades
        ORDER BY created_at DESC
        LIMIT :limit
        """
    )
    return pd.read_sql_query(query, engine, params={"limit": limit})


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
