from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.config import AppConfig


class JournalReporter:
    def __init__(self, engine: Engine, config: AppConfig) -> None:
        self.engine = engine
        self.config = config

    def generate_daily_summary(self) -> dict:
        entry_date = datetime.now(ZoneInfo(self.config.timezone)).date()
        metrics = self._collect_metrics(entry_date)
        summary = self._render_summary(entry_date, metrics)
        title = f"Daily Research Summary {entry_date.isoformat()}"
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO journal_entries (
                        entry_date, entry_type, title, summary, metrics_json, llm_model, updated_at
                    )
                    VALUES (
                        :entry_date, 'daily_research', :title, :summary,
                        CAST(:metrics_json AS jsonb), NULL, now()
                    )
                    ON CONFLICT (entry_date, entry_type)
                    DO UPDATE SET
                        title = EXCLUDED.title,
                        summary = EXCLUDED.summary,
                        metrics_json = EXCLUDED.metrics_json,
                        updated_at = now()
                    """
                ),
                {
                    "entry_date": entry_date,
                    "title": title,
                    "summary": summary,
                    "metrics_json": json.dumps(metrics, default=str),
                },
            )
        return {"entry_date": entry_date, "title": title, "summary": summary, "metrics": metrics}

    def _collect_metrics(self, entry_date) -> dict:
        with self.engine.begin() as conn:
            row_counts = [dict(row) for row in conn.execute(text(self._row_counts_sql())).mappings().all()]
            service_status = [dict(row) for row in conn.execute(text(self._service_status_sql())).mappings().all()]
            freshness = [dict(row) for row in conn.execute(text(self._freshness_sql())).mappings().all()]
            signal_summary = [
                dict(row)
                for row in conn.execute(
                    text(self._signal_summary_sql()),
                    {"timezone": self.config.timezone, "entry_date": entry_date},
                ).mappings().all()
            ]
            exit_summary = [
                dict(row)
                for row in conn.execute(
                    text(self._exit_summary_sql()),
                    {"timezone": self.config.timezone, "entry_date": entry_date},
                ).mappings().all()
            ]
            experiment_summary = [
                dict(row)
                for row in conn.execute(
                    text(self._experiment_summary_sql()),
                    {"timezone": self.config.timezone, "entry_date": entry_date},
                ).mappings().all()
            ]
            experiment_exit_summary = [
                dict(row)
                for row in conn.execute(
                    text(self._experiment_exit_summary_sql()),
                    {"timezone": self.config.timezone, "entry_date": entry_date},
                ).mappings().all()
            ]
            performance = dict(
                conn.execute(
                    text(self._performance_sql()),
                    {"timezone": self.config.timezone, "entry_date": entry_date},
                ).mappings().one()
            )
        return {
            "row_counts": row_counts,
            "service_status": service_status,
            "freshness": freshness,
            "signal_summary": signal_summary,
            "exit_summary": exit_summary,
            "experiment_summary": experiment_summary,
            "experiment_exit_summary": experiment_exit_summary,
            "performance": performance,
        }

    def _render_summary(self, entry_date, metrics: dict) -> str:
        perf = metrics["performance"]
        closed_trades = int(perf.get("closed_trades") or 0)
        wins = int(perf.get("wins") or 0)
        losses = int(perf.get("losses") or 0)
        realized = Decimal(str(perf.get("realized_pnl_idr") or 0))
        fees = Decimal(str(perf.get("fees_idr") or 0))
        slippage = Decimal(str(perf.get("slippage_idr") or 0))
        gross_profit = Decimal(str(perf.get("gross_profit_idr") or 0))
        gross_loss = Decimal(str(perf.get("gross_loss_idr") or 0))
        win_rate = (Decimal(wins) / Decimal(closed_trades) * Decimal("100")) if closed_trades else Decimal("0")
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else Decimal("0")
        stale_rows = [row for row in metrics["freshness"] if row["status"] != "fresh"]
        freshness_line = "all feeds fresh" if not stale_rows else f"{len(stale_rows)} stale/missing feed rows"
        sample_note = "Sample size is still small; do not tune aggressively yet."
        if closed_trades >= 20:
            sample_note = "Sample size is large enough for first-pass tuning review."

        signal_lines = self._rows_to_lines(metrics["signal_summary"], "decision", "reason")
        exit_lines = self._rows_to_lines(metrics["exit_summary"], "exit_reason", None)
        experiment_lines = self._experiment_lines(metrics["experiment_summary"])
        experiment_exit_lines = self._rows_to_lines(metrics["experiment_exit_summary"], "experiment_name", "exit_reason")

        return "\n".join(
            [
                f"Daily research summary for {entry_date.isoformat()}",
                "",
                f"Market data freshness: {freshness_line}.",
                f"Closed trades: {closed_trades} ({wins} wins / {losses} losses).",
                f"Realized PnL: Rp{realized:,.0f}.",
                f"Win rate: {win_rate:.1f}%.",
                f"Profit factor: {profit_factor:.2f}.",
                f"Estimated fees: Rp{fees:,.0f}.",
                f"Estimated slippage: Rp{slippage:,.0f}.",
                "",
                "Top signal outcomes:",
                signal_lines,
                "",
                "Exit reasons:",
                exit_lines,
                "",
                "Experiment breakdown:",
                experiment_lines,
                "",
                "Experiment exit reasons:",
                experiment_exit_lines,
                "",
                f"Observation: {sample_note}",
                "Reminder: journal is deterministic; Gemini is not used for this summary.",
            ]
        )

    @staticmethod
    def _rows_to_lines(rows: list[dict], key_a: str, key_b: str | None) -> str:
        if not rows:
            return "- No rows yet"
        lines = []
        for row in rows[:8]:
            label = str(row[key_a])
            if key_b:
                label = f"{label} / {row[key_b]}"
            lines.append(f"- {label}: {row['rows']}")
        return "\n".join(lines)

    @staticmethod
    def _experiment_lines(rows: list[dict]) -> str:
        if not rows:
            return "- No experiment rows yet"
        lines = []
        for row in rows[:6]:
            lines.append(
                "- {experiment_name} / {strategy_name}: trades={closed_trades}, pnl=Rp{realized_pnl_idr}, gross={avg_gross:.3f}%, net={avg_net:.3f}%, hold={avg_hold_minutes:.1f}m, top_exit={top_exit_reason}".format(
                    experiment_name=row["experiment_name"],
                    strategy_name=row["strategy_name"],
                    closed_trades=row["closed_trades"],
                    realized_pnl_idr=f"{Decimal(str(row['realized_pnl_idr'] or 0)):,.0f}",
                    avg_gross=float(row["avg_gross_pnl_percent"] or 0),
                    avg_net=float(row["avg_net_pnl_percent"] or 0),
                    avg_hold_minutes=float(row["avg_hold_seconds"] or 0) / 60,
                    top_exit_reason=row["top_exit_reason"],
                )
            )
        return "\n".join(lines)

    @staticmethod
    def _row_counts_sql() -> str:
        return """
        SELECT 'experiments' AS table_name, count(*) AS rows FROM experiments
        UNION ALL SELECT 'market_candles', count(*) FROM market_candles
        UNION ALL SELECT 'market_quotes', count(*) FROM market_quotes
        UNION ALL SELECT 'market_trades', count(*) FROM market_trades
        UNION ALL SELECT 'order_book_snapshots', count(*) FROM order_book_snapshots
        UNION ALL SELECT 'market_features_1m', count(*) FROM market_features_1m
        UNION ALL SELECT 'paper_signals', count(*) FROM paper_signals
        UNION ALL SELECT 'paper_trades', count(*) FROM paper_trades
        UNION ALL SELECT 'journal_entries', count(*) FROM journal_entries
        ORDER BY table_name
        """

    @staticmethod
    def _service_status_sql() -> str:
        return """
        SELECT DISTINCT ON (service_name)
            service_name, timestamp, status, last_success_at, message
        FROM service_health
        ORDER BY service_name, timestamp DESC
        """

    @staticmethod
    def _freshness_sql() -> str:
        return """
        WITH latest AS (
            SELECT 'candles' AS feed, symbol, max(open_time) AS latest_at FROM market_candles GROUP BY symbol
            UNION ALL SELECT 'quotes', symbol, max(timestamp) FROM market_quotes GROUP BY symbol
            UNION ALL SELECT 'trades', symbol, max(timestamp) FROM market_trades GROUP BY symbol
            UNION ALL SELECT 'order_books', symbol, max(timestamp) FROM order_book_snapshots GROUP BY symbol
            UNION ALL SELECT 'features_1m', symbol, max(open_time) FROM market_features_1m GROUP BY symbol
        )
        SELECT
            feed,
            symbol,
            latest_at,
            greatest(0, round(extract(epoch FROM (now() - latest_at)))::integer) AS age_seconds,
            CASE
                WHEN latest_at IS NULL THEN 'missing'
                WHEN extract(epoch FROM (now() - latest_at)) > 180 THEN 'stale'
                ELSE 'fresh'
            END AS status
        FROM latest
        ORDER BY feed, symbol
        """

    @staticmethod
    def _signal_summary_sql() -> str:
        return """
        SELECT
            decision,
            coalesce(skip_reason, 'TAKE') AS reason,
            count(*) AS rows
        FROM paper_signals
        WHERE (timestamp AT TIME ZONE :timezone)::date = :entry_date
        GROUP BY decision, coalesce(skip_reason, 'TAKE')
        ORDER BY rows DESC
        """

    @staticmethod
    def _exit_summary_sql() -> str:
        return """
        SELECT coalesce(exit_reason, 'OPEN') AS exit_reason, count(*) AS rows
        FROM paper_trades
        WHERE (entry_time AT TIME ZONE :timezone)::date = :entry_date
        GROUP BY coalesce(exit_reason, 'OPEN')
        ORDER BY rows DESC
        """

    @staticmethod
    def _experiment_summary_sql() -> str:
        return """
        WITH signal_stats AS (
            SELECT
                experiment_name,
                max(strategy_name) AS strategy_name,
                count(*) FILTER (WHERE decision = 'TAKE') AS take_signals,
                count(*) FILTER (WHERE decision = 'SKIP') AS skip_signals
            FROM paper_signals
            WHERE (timestamp AT TIME ZONE :timezone)::date = :entry_date
            GROUP BY experiment_name
        ),
        trade_stats AS (
            SELECT
                experiment_name,
                max(strategy_name) AS strategy_name,
                count(*) FILTER (WHERE status = 'CLOSED') AS closed_trades,
                coalesce(sum(pnl_idr) FILTER (WHERE status = 'CLOSED'), 0) AS realized_pnl_idr,
                coalesce(avg(gross_pnl_percent) FILTER (WHERE status = 'CLOSED'), 0) AS avg_gross_pnl_percent,
                coalesce(avg(pnl_percent) FILTER (WHERE status = 'CLOSED'), 0) AS avg_net_pnl_percent,
                coalesce(avg(hold_seconds) FILTER (WHERE status = 'CLOSED'), 0) AS avg_hold_seconds
            FROM paper_trades
            WHERE (entry_time AT TIME ZONE :timezone)::date = :entry_date
            GROUP BY experiment_name
        ),
        exit_ranked AS (
            SELECT
                experiment_name,
                exit_reason,
                count(*) AS rows,
                row_number() OVER (PARTITION BY experiment_name ORDER BY count(*) DESC, exit_reason) AS rank_order
            FROM paper_trades
            WHERE (entry_time AT TIME ZONE :timezone)::date = :entry_date
              AND status = 'CLOSED'
              AND exit_reason IS NOT NULL
              AND experiment_name IN (SELECT name FROM experiments)
            GROUP BY experiment_name, exit_reason
        )
        SELECT
            experiments.name AS experiment_name,
            coalesce(signal_stats.strategy_name, trade_stats.strategy_name, experiments.strategy_name, 'unknown') AS strategy_name,
            coalesce(trade_stats.closed_trades, 0) AS closed_trades,
            coalesce(trade_stats.realized_pnl_idr, 0) AS realized_pnl_idr,
            coalesce(trade_stats.avg_gross_pnl_percent, 0) AS avg_gross_pnl_percent,
            coalesce(trade_stats.avg_net_pnl_percent, 0) AS avg_net_pnl_percent,
            coalesce(trade_stats.avg_hold_seconds, 0) AS avg_hold_seconds,
            coalesce(signal_stats.take_signals, 0) AS take_signals,
            coalesce(signal_stats.skip_signals, 0) AS skip_signals,
            coalesce(exit_ranked.exit_reason, 'no_closed_trades') AS top_exit_reason
        FROM experiments
        LEFT JOIN signal_stats ON signal_stats.experiment_name = experiments.name
        LEFT JOIN trade_stats ON trade_stats.experiment_name = experiments.name
        LEFT JOIN exit_ranked ON exit_ranked.experiment_name = experiments.name AND exit_ranked.rank_order = 1
        ORDER BY realized_pnl_idr DESC, experiment_name
        """

    @staticmethod
    def _experiment_exit_summary_sql() -> str:
        return """
        SELECT
            experiment_name,
            exit_reason,
            count(*) AS rows
        FROM paper_trades
        WHERE (entry_time AT TIME ZONE :timezone)::date = :entry_date
          AND status = 'CLOSED'
          AND exit_reason IS NOT NULL
          AND experiment_name IN (SELECT name FROM experiments)
        GROUP BY experiment_name, exit_reason
        ORDER BY experiment_name, rows DESC, exit_reason
        """

    @staticmethod
    def _performance_sql() -> str:
        return """
        SELECT
            count(*) FILTER (WHERE status = 'CLOSED') AS closed_trades,
            count(*) FILTER (WHERE status = 'OPEN') AS open_trades,
            coalesce(sum(pnl_idr) FILTER (WHERE status = 'CLOSED'), 0) AS realized_pnl_idr,
            coalesce(avg(gross_pnl_percent) FILTER (WHERE status = 'CLOSED'), 0) AS avg_gross_pnl_percent,
            coalesce(avg(pnl_percent) FILTER (WHERE status = 'CLOSED'), 0) AS avg_net_pnl_percent,
            coalesce(avg(hold_seconds) FILTER (WHERE status = 'CLOSED'), 0) AS avg_hold_seconds,
            count(*) FILTER (WHERE status = 'CLOSED' AND pnl_idr > 0) AS wins,
            count(*) FILTER (WHERE status = 'CLOSED' AND pnl_idr < 0) AS losses,
            coalesce(sum(pnl_idr) FILTER (WHERE status = 'CLOSED' AND pnl_idr > 0), 0) AS gross_profit_idr,
            abs(coalesce(sum(pnl_idr) FILTER (WHERE status = 'CLOSED' AND pnl_idr < 0), 0)) AS gross_loss_idr,
            coalesce(sum(fee_estimate_idr) FILTER (WHERE status = 'CLOSED'), 0) AS fees_idr,
            coalesce(sum(slippage_estimate_idr) FILTER (WHERE status = 'CLOSED'), 0) AS slippage_idr
        FROM paper_trades
        WHERE (entry_time AT TIME ZONE :timezone)::date = :entry_date
        """
