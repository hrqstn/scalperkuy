from __future__ import annotations

import logging
import signal
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Callable

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

    def run(self) -> None:
        self._install_signal_handlers()
        self._write_health("starting", "collector starting")
        self._send_alert("collector-startup", "Scalperkuy collector started.", force=True)

        tasks = [
            ScheduledTask("candles", self.config.data.candle_poll_seconds, self.collect_candles),
            ScheduledTask("quotes", self.config.data.quote_interval_seconds, self.collect_quotes),
            ScheduledTask("order_book", self.config.data.order_book_interval_seconds, self.collect_order_books),
            ScheduledTask("trades", self.config.data.trades_interval_seconds, self.collect_trades),
            ScheduledTask("stale_check", 30, self.check_stale_data),
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
                    self._write_health("error", f"{task.name} failed: {exc}")
                    self._send_alert("collector-error", f"Scalperkuy collector task `{task.name}` failed: {exc}")
                    task.mark_done(now + min(task.interval_seconds, 30))
            time.sleep(1)

        self.adapter.close()
        self._write_health("stopped", "collector stopped")

    def collect_candles(self) -> None:
        total = 0
        for symbol in self.config.symbols:
            candles = self.adapter.fetch_recent_candles(symbol, self.config.data.candle_timeframe, limit=3)
            total += self.repo.save_candles(candles)
        self._mark_success("ok", f"stored {total} candles")

    def collect_quotes(self) -> None:
        for symbol in self.config.symbols:
            self.repo.save_quote(self.adapter.fetch_quote(symbol))
        self._mark_success("ok", f"stored quotes for {len(self.config.symbols)} symbols")

    def collect_order_books(self) -> None:
        for symbol in self.config.symbols:
            self.repo.save_order_book(self.adapter.fetch_order_book(symbol, self.config.data.order_book_depth))
        self._mark_success("ok", f"stored order books for {len(self.config.symbols)} symbols")

    def collect_trades(self) -> None:
        total = 0
        for symbol in self.config.symbols:
            total += self.repo.save_trades(self.adapter.fetch_recent_trades(symbol, limit=50))
        self._mark_success("ok", f"stored up to {total} recent trades")

    def check_stale_data(self) -> None:
        if not self.last_success_at:
            return
        age_seconds = (datetime.now(UTC) - self.last_success_at).total_seconds()
        if age_seconds <= self.config.data.stale_data_seconds:
            return
        message = f"collector stale for {age_seconds:.0f}s"
        self._write_health("stale", message)
        self._send_alert("collector-stale", f"Scalperkuy collector stale data warning: {message}")

    def _mark_success(self, status: str, message: str) -> None:
        self.last_success_at = datetime.now(UTC)
        self._write_health(status, message)
        logger.info(message)

    def _write_health(self, status: str, message: str) -> None:
        now = datetime.now(UTC)
        self.repo.write_health(
            "collector",
            status,
            message,
            timestamp=now,
            last_success_at=self.last_success_at,
            metadata_json={"symbols": self.config.symbols, "mode": self.config.mode},
        )

    def _send_alert(self, key: str, message: str, *, force: bool = False) -> None:
        try:
            self.alerts.send(key, message, force=force)
        except Exception as exc:
            logger.warning("discord alert failed: %s", exc)

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
