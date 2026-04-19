from __future__ import annotations

import logging
import signal
import shutil
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Callable

import httpx

from app.collector.repository import MarketDataRepository
from app.config import AppConfig, load_config
from app.db.models import init_db
from app.db.session import get_engine
from app.exchange.tokocrypto import TokocryptoAdapter
from app.reporting.discord import DiscordAlertClient


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("collector")


@dataclass
class ScheduledTask:
    name: str
    interval_seconds: int
    fn: Callable[[], None]
    next_run: float = field(default_factory=lambda: 0.0)

    def due(self, now: float) -> bool:
        return now >= self.next_run

    def mark_done(self, now: float) -> None:
        self.next_run = now + self.interval_seconds


class CollectorService:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.engine = get_engine()
        init_db(self.engine)
        self.adapter = TokocryptoAdapter(config)
        self.repo = MarketDataRepository(self.engine, config.exchange)
        self.alerts = DiscordAlertClient(
            config.discord_webhook_url,
            cooldown_seconds=config.alerts_config.cooldown_seconds,
        )
        self.running = True
        self.last_success_at: datetime | None = None
        self.last_success_by_task: dict[str, datetime] = {}
        self.last_health_write_at: datetime | None = None
        self.last_health_status: str | None = None

    def run(self) -> None:
        self._install_signal_handlers()
        self._safe_write_health("starting", "collector starting")
        self._send_alert(
            "collector-startup",
            "Scalperkuy collector started. Mode: paper. Live trading remains disabled.",
            force=True,
        )

        tasks = [
            ScheduledTask("candles", self.config.data.candle_poll_seconds, self.collect_candles),
            ScheduledTask("quotes", self.config.data.quote_interval_seconds, self.collect_quotes),
            ScheduledTask("order_book", self.config.data.order_book_interval_seconds, self.collect_order_books),
            ScheduledTask("trades", self.config.data.trades_interval_seconds, self.collect_trades),
            ScheduledTask("stale_check", 30, self.check_stale_data),
            ScheduledTask(
                "disk_check",
                self.config.alerts_config.disk_check_interval_seconds,
                self.check_disk_usage,
            ),
        ]

        while self.running:
            now = time.monotonic()
            for task in tasks:
                if not task.due(now):
                    continue
                try:
                    task.fn()
                    task.mark_done(now)
                except Exception as exc:
                    logger.exception("task failed: %s", task.name)
                    message = f"{task.name} failed: {self._format_exception(exc)}"
                    self._safe_write_health("error", message)
                    self._send_alert(f"collector-error-{task.name}", f"Scalperkuy collector task `{message}`")
                    task.mark_done(now + min(task.interval_seconds, 30))
            time.sleep(1)

        self.adapter.close()
        self._safe_write_health("stopped", "collector stopped")

    def collect_candles(self) -> None:
        total = 0
        for symbol in self.config.symbols:
            candles = self.adapter.fetch_recent_candles(symbol, self.config.data.candle_timeframe, limit=3)
            total += self.repo.save_candles(candles)
        self._mark_success("candles", "ok", f"stored {total} candles")

    def collect_quotes(self) -> None:
        for symbol in self.config.symbols:
            self.repo.save_quote(self.adapter.fetch_quote(symbol))
        self._mark_success("quotes", "ok", f"stored quotes for {len(self.config.symbols)} symbols")

    def collect_order_books(self) -> None:
        for symbol in self.config.symbols:
            self.repo.save_order_book(self.adapter.fetch_order_book(symbol, self.config.data.order_book_depth))
        self._mark_success("order_book", "ok", f"stored order books for {len(self.config.symbols)} symbols")

    def collect_trades(self) -> None:
        total = 0
        for symbol in self.config.symbols:
            total += self.repo.save_trades(self.adapter.fetch_recent_trades(symbol, limit=50))
        self._mark_success("trades", "ok", f"stored up to {total} recent trades")

    def check_stale_data(self) -> None:
        now = datetime.now(UTC)
        for task_name in ("candles", "quotes", "order_book", "trades"):
            last_success = self.last_success_by_task.get(task_name)
            if not last_success:
                continue
            age_seconds = (now - last_success).total_seconds()
            if age_seconds <= self.config.data.stale_data_seconds:
                continue
            message = f"{task_name} feed stale for {age_seconds:.0f}s"
            self._safe_write_health("stale", message)
            self._send_alert(
                f"collector-stale-{task_name}",
                f"Scalperkuy stale data warning: {message}. Collector is still running.",
            )

    def check_disk_usage(self) -> None:
        usage = shutil.disk_usage("/")
        used_percent = usage.used / usage.total * 100
        if used_percent < self.config.alerts_config.disk_usage_warning_percent:
            return
        message = f"disk usage is {used_percent:.1f}% ({usage.free / (1024 ** 3):.1f} GB free)"
        self._safe_write_health("warning", message)
        self._send_alert("disk-usage-warning", f"Scalperkuy disk warning: {message}")

    def _mark_success(self, task_name: str, status: str, message: str) -> None:
        success_at = datetime.now(UTC)
        self.last_success_at = success_at
        self.last_success_by_task[task_name] = success_at
        self._safe_write_health(status, message)
        logger.info(message)

    def _safe_write_health(self, status: str, message: str) -> None:
        now = datetime.now(UTC)
        if (
            status == "ok"
            and self.last_health_status == "ok"
            and self.last_health_write_at
            and (now - self.last_health_write_at).total_seconds()
            < self.config.data.service_health_ok_interval_seconds
        ):
            return
        try:
            self._write_health(status, message)
            self.last_health_write_at = now
            self.last_health_status = status
        except Exception as exc:
            logger.warning("service health write failed: %s", exc)
            self._send_alert("database-unavailable", f"Scalperkuy database/health write failed: {exc}")

    def _write_health(self, status: str, message: str) -> None:
        now = datetime.now(UTC)
        self.repo.write_health(
            "collector",
            status,
            message,
            timestamp=now,
            last_success_at=self.last_success_at,
            metadata_json={
                "symbols": self.config.symbols,
                "mode": self.config.mode,
                "last_success_by_task": {
                    task_name: success_at.isoformat()
                    for task_name, success_at in self.last_success_by_task.items()
                },
            },
        )

    def _send_alert(self, key: str, message: str, *, force: bool = False) -> None:
        try:
            self.alerts.send(key, message, force=force)
        except Exception as exc:
            logger.warning("discord alert failed: %s", exc)

    @staticmethod
    def _format_exception(exc: Exception) -> str:
        if isinstance(exc, httpx.HTTPStatusError):
            status_code = exc.response.status_code
            reason = exc.response.reason_phrase
            path = exc.request.url.path
            query = exc.request.url.query.decode("utf-8") if isinstance(exc.request.url.query, bytes) else exc.request.url.query
            return f"HTTP {status_code} {reason} from Tokocrypto {path}?{query}"
        if isinstance(exc, httpx.RequestError):
            return f"{exc.__class__.__name__}: {exc}"
        return str(exc)

    def _install_signal_handlers(self) -> None:
        def stop(signum: int, _frame: object) -> None:
            logger.info("received signal %s, stopping", signum)
            self.running = False

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)


def main() -> None:
    CollectorService(load_config()).run()


if __name__ == "__main__":
    main()
