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
        SELECT 'experiments' AS table_name, count(*) AS rows FROM experiments
        UNION ALL
        SELECT 'market_candles' AS table_name, count(*) AS rows FROM market_candles
        UNION ALL SELECT 'market_quotes', count(*) FROM market_quotes
        UNION ALL SELECT 'market_trades', count(*) FROM market_trades
        UNION ALL SELECT 'order_book_snapshots', count(*) FROM order_book_snapshots
        UNION ALL SELECT 'market_features_1m', count(*) FROM market_features_1m
        UNION ALL SELECT 'paper_signals', count(*) FROM paper_signals
        UNION ALL SELECT 'paper_trades', count(*) FROM paper_trades
        UNION ALL SELECT 'journal_entries', count(*) FROM journal_entries
        UNION ALL SELECT 'service_health', count(*) FROM service_health
        ORDER BY table_name
        """
    )
    return pd.read_sql_query(query, engine)


def list_experiments(engine: Engine) -> pd.DataFrame:
    query = text(
        """
        SELECT id, name AS experiment_name, strategy_name, status, updated_at
        FROM experiments
        ORDER BY name
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


def data_quality_summary(engine: Engine, limit_hours: int = 24) -> pd.DataFrame:
    query = text(
        """
        SELECT
            symbol,
            count(*) AS feature_rows,
            count(*) FILTER (WHERE is_tradeable_minute) AS tradeable_rows,
            round(count(*) FILTER (WHERE is_tradeable_minute)::numeric / nullif(count(*), 0) * 100, 2) AS tradeable_percent,
            round(avg(quality_score), 2) AS avg_quality_score,
            count(*) FILTER (WHERE quality_flags ? 'missing_candle') AS missing_candle,
            count(*) FILTER (WHERE quality_flags ? 'low_quote_samples') AS low_quote_samples,
            count(*) FILTER (WHERE quality_flags ? 'low_trade_samples') AS low_trade_samples,
            count(*) FILTER (WHERE quality_flags ? 'low_order_book_samples') AS low_order_book_samples,
            count(*) FILTER (WHERE quality_flags ? 'spread_too_wide') AS spread_too_wide,
            count(*) FILTER (WHERE quality_flags ? 'volatility_too_low') AS volatility_too_low
        FROM market_features_1m
        WHERE open_time >= now() - (:limit_hours * interval '1 hour')
        GROUP BY symbol
        ORDER BY symbol
        """
    )
    return pd.read_sql_query(query, engine, params={"limit_hours": limit_hours})


def recent_trades(engine: Engine, limit: int = 25, experiment_name: str | None = None) -> pd.DataFrame:
    query = text(
        """
        SELECT
            id,
            experiment_name,
            strategy_name,
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
        WHERE (CAST(:experiment_name AS text) IS NULL OR experiment_name = CAST(:experiment_name AS text))
        ORDER BY created_at DESC
        LIMIT :limit
        """
    )
    return pd.read_sql_query(query, engine, params={"limit": limit, "experiment_name": experiment_name})


def open_positions(engine: Engine, experiment_name: str | None = None) -> pd.DataFrame:
    query = text(
        """
        SELECT
            id,
            experiment_name,
            strategy_name,
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
          AND (CAST(:experiment_name AS text) IS NULL OR experiment_name = CAST(:experiment_name AS text))
        ORDER BY entry_time DESC
        """
    )
    return pd.read_sql_query(query, engine, params={"experiment_name": experiment_name})


def signal_summary(engine: Engine, limit_hours: int = 24, experiment_name: str | None = None) -> pd.DataFrame:
    query = text(
        """
        SELECT
            decision,
            coalesce(skip_reason, 'TAKE') AS reason,
            count(*) AS rows
        FROM paper_signals
        WHERE timestamp >= now() - (:limit_hours * interval '1 hour')
          AND (CAST(:experiment_name AS text) IS NULL OR experiment_name = CAST(:experiment_name AS text))
        GROUP BY decision, coalesce(skip_reason, 'TAKE')
        ORDER BY rows DESC
        LIMIT 25
        """
    )
    return pd.read_sql_query(query, engine, params={"limit_hours": limit_hours, "experiment_name": experiment_name})


def recent_signals(engine: Engine, limit: int = 50, experiment_name: str | None = None) -> pd.DataFrame:
    query = text(
        """
        SELECT
            timestamp,
            experiment_name,
            symbol,
            strategy_name,
            decision,
            side,
            confidence,
            reason,
            skip_reason
        FROM paper_signals
        WHERE (CAST(:experiment_name AS text) IS NULL OR experiment_name = CAST(:experiment_name AS text))
        ORDER BY timestamp DESC
        LIMIT :limit
        """
    )
    return pd.read_sql_query(query, engine, params={"limit": limit, "experiment_name": experiment_name})


def paper_performance(engine: Engine, experiment_name: str | None = None) -> pd.DataFrame:
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
        WHERE (CAST(:experiment_name AS text) IS NULL OR experiment_name = CAST(:experiment_name AS text))
        """
    )
    return pd.read_sql_query(query, engine, params={"experiment_name": experiment_name})


def equity_curve(engine: Engine, experiment_name: str | None = None) -> pd.DataFrame:
    query = text(
        """
        SELECT
            exit_time,
            pnl_idr,
            sum(pnl_idr) OVER (ORDER BY exit_time, id) AS cumulative_pnl_idr
        FROM paper_trades
        WHERE status = 'CLOSED' AND exit_time IS NOT NULL
          AND (CAST(:experiment_name AS text) IS NULL OR experiment_name = CAST(:experiment_name AS text))
        ORDER BY exit_time
        """
    )
    return pd.read_sql_query(query, engine, params={"experiment_name": experiment_name})


def experiment_summary(engine: Engine, limit_hours: int = 24) -> pd.DataFrame:
    query = text(
        """
        WITH experiment_names AS (
            SELECT name AS experiment_name FROM experiments
            UNION
            SELECT DISTINCT experiment_name FROM paper_signals WHERE experiment_name IS NOT NULL
            UNION
            SELECT DISTINCT experiment_name FROM paper_trades WHERE experiment_name IS NOT NULL
        ),
        signal_stats AS (
            SELECT
                experiment_name,
                max(strategy_name) AS strategy_name,
                count(*) FILTER (WHERE timestamp >= now() - (:limit_hours * interval '1 hour') AND decision = 'TAKE') AS take_signals_24h,
                count(*) FILTER (WHERE timestamp >= now() - (:limit_hours * interval '1 hour') AND decision = 'SKIP') AS skip_signals_24h
            FROM paper_signals
            GROUP BY experiment_name
        ),
        trade_stats AS (
            SELECT
                experiment_name,
                max(strategy_name) AS strategy_name,
                count(*) FILTER (WHERE status = 'CLOSED') AS closed_trades,
                count(*) FILTER (WHERE status = 'OPEN') AS open_trades,
                coalesce(sum(pnl_idr) FILTER (WHERE status = 'CLOSED'), 0) AS realized_pnl_idr,
                count(*) FILTER (WHERE status = 'CLOSED' AND pnl_idr > 0) AS wins,
                count(*) FILTER (WHERE status = 'CLOSED' AND pnl_idr < 0) AS losses,
                coalesce(sum(pnl_idr) FILTER (WHERE status = 'CLOSED' AND pnl_idr > 0), 0) AS gross_profit_idr,
                abs(coalesce(sum(pnl_idr) FILTER (WHERE status = 'CLOSED' AND pnl_idr < 0), 0)) AS gross_loss_idr
            FROM paper_trades
            GROUP BY experiment_name
        )
        SELECT
            names.experiment_name,
            coalesce(experiments.strategy_name, signal_stats.strategy_name, trade_stats.strategy_name, 'unknown') AS strategy_name,
            coalesce(signal_stats.take_signals_24h, 0) AS take_signals_24h,
            coalesce(signal_stats.skip_signals_24h, 0) AS skip_signals_24h,
            coalesce(trade_stats.closed_trades, 0) AS closed_trades,
            coalesce(trade_stats.open_trades, 0) AS open_trades,
            coalesce(trade_stats.realized_pnl_idr, 0) AS realized_pnl_idr,
            CASE
                WHEN coalesce(trade_stats.closed_trades, 0) = 0 THEN 0
                ELSE round(coalesce(trade_stats.wins, 0)::numeric / trade_stats.closed_trades * 100, 2)
            END AS win_rate_percent,
            CASE
                WHEN coalesce(trade_stats.gross_loss_idr, 0) = 0 THEN 0
                ELSE round(coalesce(trade_stats.gross_profit_idr, 0) / nullif(trade_stats.gross_loss_idr, 0), 4)
            END AS profit_factor
        FROM experiment_names names
        LEFT JOIN experiments ON experiments.name = names.experiment_name
        LEFT JOIN signal_stats ON signal_stats.experiment_name = names.experiment_name
        LEFT JOIN trade_stats ON trade_stats.experiment_name = names.experiment_name
        ORDER BY realized_pnl_idr DESC, names.experiment_name
        """
    )
    return pd.read_sql_query(query, engine, params={"limit_hours": limit_hours})


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


def latest_journal_entry(engine: Engine) -> pd.DataFrame:
    query = text(
        """
        SELECT entry_date, entry_type, title, summary, metrics_json, updated_at
        FROM journal_entries
        ORDER BY entry_date DESC, updated_at DESC
        LIMIT 1
        """
    )
    return pd.read_sql_query(query, engine)


def recent_journal_entries(engine: Engine, limit: int = 14) -> pd.DataFrame:
    query = text(
        """
        SELECT entry_date, entry_type, title, updated_at
        FROM journal_entries
        ORDER BY entry_date DESC, updated_at DESC
        LIMIT :limit
        """
    )
    return pd.read_sql_query(query, engine, params={"limit": limit})
