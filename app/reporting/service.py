from __future__ import annotations

import logging
import signal
import time
from datetime import UTC, datetime

from app.collector.repository import MarketDataRepository
from app.config import load_config
from app.db.models import init_db
from app.db.session import get_engine
from app.reporting.discord import DiscordAlertClient
from app.reporting.journal import JournalReporter


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("reporter")


class ReporterService:
    def __init__(self) -> None:
        self.config = load_config()
        self.engine = get_engine()
        init_db(self.engine)
        self.repo = MarketDataRepository(self.engine, self.config.exchange)
        self.journal = JournalReporter(self.engine, self.config)
        self.alerts = DiscordAlertClient(
            self.config.discord_webhook_url,
            cooldown_seconds=self.config.alerts_config.cooldown_seconds,
        )
        self.running = True

    def run(self) -> None:
        self._install_signal_handlers()
        while self.running:
            if not self.config.reporting.enabled:
                self._write_health("standby", "reporting disabled")
                logger.info("reporting disabled")
                time.sleep(300)
                continue
            try:
                entry = self.journal.generate_daily_summary()
                self._write_health("ok", f"journal updated for {entry['entry_date']}")
                logger.info("journal updated for %s", entry["entry_date"])
                if self.config.reporting.discord_daily_summary_enabled:
                    self.alerts.send(
                        f"daily-summary-{entry['entry_date']}",
                        f"**{entry['title']}**\n```text\n{entry['summary'][:1700]}\n```",
                    )
            except Exception as exc:
                logger.exception("reporting failed")
                self._write_health("error", f"reporting failed: {exc}")
            time.sleep(self.config.reporting.interval_seconds)

    def _write_health(self, status: str, message: str) -> None:
        self.repo.write_health(
            "reporter",
            status,
            message,
            timestamp=datetime.now(UTC),
            last_success_at=datetime.now(UTC) if status in {"ok", "standby"} else None,
            metadata_json={
                "llm": "disabled",
                "deterministic_journal": True,
                "interval_seconds": self.config.reporting.interval_seconds,
            },
        )

    def _install_signal_handlers(self) -> None:
        def stop(_signum: int, _frame: object) -> None:
            self.running = False

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)


def main() -> None:
    ReporterService().run()


if __name__ == "__main__":
    main()
