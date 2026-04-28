from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def _experiment_predicate(column: str, experiment_name: str | None) -> tuple[str, dict]:
    if experiment_name:
        return f"{column} = :experiment_name", {"experiment_name": experiment_name}
    return f"{column} IN (SELECT name FROM experiments)", {}


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
    predicate, params = _experiment_predicate("experiment_name", experiment_name)
    query = text(
        f"""
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
            gross_pnl_idr,
            gross_pnl_percent,
            hold_seconds,
            max_favorable_excursion_bps,
            max_adverse_excursion_bps,
            horizon_3m_label,
            horizon_5m_label,
            horizon_10m_label,
            fee_estimate_idr,
            slippage_estimate_idr,
            exit_reason
        FROM paper_trades
        WHERE {predicate}
        ORDER BY created_at DESC
        LIMIT :limit
        """
    )
    return pd.read_sql_query(query, engine, params={**params, "limit": limit})


def open_positions(engine: Engine, experiment_name: str | None = None) -> pd.DataFrame:
    predicate, params = _experiment_predicate("experiment_name", experiment_name)
    query = text(
        f"""
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
          AND {predicate}
        ORDER BY entry_time DESC
        """
    )
    return pd.read_sql_query(query, engine, params=params)


def signal_summary(engine: Engine, limit_hours: int = 24, experiment_name: str | None = None) -> pd.DataFrame:
    predicate, params = _experiment_predicate("experiment_name", experiment_name)
    query = text(
        f"""
        SELECT
            decision,
            coalesce(skip_reason, 'TAKE') AS reason,
            count(*) AS rows
        FROM paper_signals
        WHERE timestamp >= now() - (:limit_hours * interval '1 hour')
          AND {predicate}
        GROUP BY decision, coalesce(skip_reason, 'TAKE')
        ORDER BY rows DESC
        LIMIT 25
        """
    )
    return pd.read_sql_query(query, engine, params={**params, "limit_hours": limit_hours})


def recent_signals(engine: Engine, limit: int = 50, experiment_name: str | None = None) -> pd.DataFrame:
    predicate, params = _experiment_predicate("experiment_name", experiment_name)
    query = text(
        f"""
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
        WHERE {predicate}
        ORDER BY timestamp DESC
        LIMIT :limit
        """
    )
    return pd.read_sql_query(query, engine, params={**params, "limit": limit})


def paper_performance(engine: Engine, experiment_name: str | None = None) -> pd.DataFrame:
    predicate, params = _experiment_predicate("experiment_name", experiment_name)
    query = text(
        f"""
        SELECT
            count(*) FILTER (WHERE status = 'CLOSED') AS closed_trades,
            count(*) FILTER (WHERE status = 'OPEN') AS open_trades,
            coalesce(sum(pnl_idr) FILTER (WHERE status = 'CLOSED'), 0) AS realized_pnl_idr,
            coalesce(avg(pnl_percent) FILTER (WHERE status = 'CLOSED'), 0) AS avg_pnl_percent,
            coalesce(avg(gross_pnl_percent) FILTER (WHERE status = 'CLOSED'), 0) AS avg_gross_pnl_percent,
            coalesce(avg(hold_seconds) FILTER (WHERE status = 'CLOSED'), 0) AS avg_hold_seconds,
            count(*) FILTER (WHERE status = 'CLOSED' AND pnl_idr > 0) AS wins,
            count(*) FILTER (WHERE status = 'CLOSED' AND pnl_idr < 0) AS losses,
            coalesce(sum(pnl_idr) FILTER (WHERE status = 'CLOSED' AND pnl_idr > 0), 0) AS gross_profit_idr,
            abs(coalesce(sum(pnl_idr) FILTER (WHERE status = 'CLOSED' AND pnl_idr < 0), 0)) AS gross_loss_idr,
            coalesce(sum(fee_estimate_idr) FILTER (WHERE status = 'CLOSED'), 0) AS fees_idr,
            coalesce(sum(slippage_estimate_idr) FILTER (WHERE status = 'CLOSED'), 0) AS slippage_idr
        FROM paper_trades
        WHERE {predicate}
        """
    )
    return pd.read_sql_query(query, engine, params=params)


def equity_curve(engine: Engine, experiment_name: str | None = None) -> pd.DataFrame:
    predicate, params = _experiment_predicate("experiment_name", experiment_name)
    query = text(
        f"""
        SELECT
            exit_time,
            pnl_idr,
            sum(pnl_idr) OVER (ORDER BY exit_time, id) AS cumulative_pnl_idr
        FROM paper_trades
        WHERE status = 'CLOSED' AND exit_time IS NOT NULL
          AND {predicate}
        ORDER BY exit_time
        """
    )
    return pd.read_sql_query(query, engine, params=params)


def experiment_summary(engine: Engine, limit_hours: int = 24) -> pd.DataFrame:
    query = text(
        """
        WITH signal_stats AS (
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
                coalesce(avg(gross_pnl_percent) FILTER (WHERE status = 'CLOSED'), 0) AS avg_gross_pnl_percent,
                coalesce(avg(pnl_percent) FILTER (WHERE status = 'CLOSED'), 0) AS avg_net_pnl_percent,
                coalesce(avg(hold_seconds) FILTER (WHERE status = 'CLOSED'), 0) AS avg_hold_seconds,
                coalesce(avg(max_favorable_excursion_bps) FILTER (WHERE status = 'CLOSED'), 0) AS avg_mfe_bps,
                coalesce(avg(max_adverse_excursion_bps) FILTER (WHERE status = 'CLOSED'), 0) AS avg_mae_bps,
                count(*) FILTER (WHERE status = 'CLOSED' AND pnl_idr > 0) AS wins,
                count(*) FILTER (WHERE status = 'CLOSED' AND pnl_idr < 0) AS losses,
                coalesce(sum(pnl_idr) FILTER (WHERE status = 'CLOSED' AND pnl_idr > 0), 0) AS gross_profit_idr,
                abs(coalesce(sum(pnl_idr) FILTER (WHERE status = 'CLOSED' AND pnl_idr < 0), 0)) AS gross_loss_idr
            FROM paper_trades
            GROUP BY experiment_name
        ),
        exit_ranked AS (
            SELECT
                experiment_name,
                exit_reason,
                count(*) AS rows,
                row_number() OVER (PARTITION BY experiment_name ORDER BY count(*) DESC, exit_reason) AS rank_order
            FROM paper_trades
            WHERE status = 'CLOSED'
              AND exit_reason IS NOT NULL
            GROUP BY experiment_name, exit_reason
        )
        SELECT
            experiments.name AS experiment_name,
            coalesce(experiments.strategy_name, signal_stats.strategy_name, trade_stats.strategy_name, 'unknown') AS strategy_name,
            coalesce(signal_stats.take_signals_24h, 0) AS take_signals_24h,
            coalesce(signal_stats.skip_signals_24h, 0) AS skip_signals_24h,
            coalesce(trade_stats.closed_trades, 0) AS closed_trades,
            coalesce(trade_stats.open_trades, 0) AS open_trades,
            coalesce(trade_stats.realized_pnl_idr, 0) AS realized_pnl_idr,
            round(coalesce(trade_stats.avg_gross_pnl_percent, 0), 4) AS avg_gross_pnl_percent,
            round(coalesce(trade_stats.avg_net_pnl_percent, 0), 4) AS avg_net_pnl_percent,
            round(coalesce(trade_stats.avg_hold_seconds, 0), 1) AS avg_hold_seconds,
            round(coalesce(trade_stats.avg_mfe_bps, 0), 2) AS avg_mfe_bps,
            round(coalesce(trade_stats.avg_mae_bps, 0), 2) AS avg_mae_bps,
            coalesce(exit_ranked.exit_reason, 'no_closed_trades') AS top_exit_reason,
            CASE
                WHEN coalesce(trade_stats.closed_trades, 0) = 0 THEN 0
                ELSE round(coalesce(trade_stats.wins, 0)::numeric / trade_stats.closed_trades * 100, 2)
            END AS win_rate_percent,
            CASE
                WHEN coalesce(trade_stats.gross_loss_idr, 0) = 0 THEN 0
                ELSE round(coalesce(trade_stats.gross_profit_idr, 0) / nullif(trade_stats.gross_loss_idr, 0), 4)
            END AS profit_factor
        FROM experiments
        LEFT JOIN signal_stats ON signal_stats.experiment_name = experiments.name
        LEFT JOIN trade_stats ON trade_stats.experiment_name = experiments.name
        LEFT JOIN exit_ranked ON exit_ranked.experiment_name = experiments.name AND exit_ranked.rank_order = 1
        ORDER BY realized_pnl_idr DESC, experiments.name
        """
    )
    return pd.read_sql_query(query, engine, params={"limit_hours": limit_hours})


def experiment_exit_breakdown(engine: Engine, experiment_name: str | None = None) -> pd.DataFrame:
    predicate, params = _experiment_predicate("paper_trades.experiment_name", experiment_name)
    query = text(
        f"""
        SELECT
            paper_trades.experiment_name,
            paper_trades.exit_reason,
            count(*) AS rows
        FROM paper_trades
        WHERE paper_trades.status = 'CLOSED'
          AND paper_trades.exit_reason IS NOT NULL
          AND {predicate}
        GROUP BY paper_trades.experiment_name, paper_trades.exit_reason
        ORDER BY paper_trades.experiment_name, rows DESC, paper_trades.exit_reason
        """
    )
    return pd.read_sql_query(query, engine, params=params)


def experiment_horizon_summary(engine: Engine, experiment_name: str | None = None) -> pd.DataFrame:
    predicate, params = _experiment_predicate("experiment_name", experiment_name)
    query = text(
        f"""
        SELECT
            experiment_name,
            horizon_5m_label,
            count(*) AS rows
        FROM paper_trades
        WHERE status = 'CLOSED'
          AND horizon_5m_label IS NOT NULL
          AND {predicate}
        GROUP BY experiment_name, horizon_5m_label
        ORDER BY experiment_name, rows DESC, horizon_5m_label
        """
    )
    return pd.read_sql_query(query, engine, params=params)


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
