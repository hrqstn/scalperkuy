from __future__ import annotations

import logging
import signal
import time
from datetime import UTC, datetime

from app.collector.repository import MarketDataRepository
from app.config import load_config
from app.db.models import init_db
from app.db.session import get_engine


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("paper_trader")


class PaperTraderService:
    """Standby paper trader.

    It writes health only in milestone 1. Trade simulation will be enabled after
    the collector has proven stable and the dashboard shows trustworthy data.
    """

    def __init__(self) -> None:
        self.config = load_config()
        self.engine = get_engine()
        init_db(self.engine)
        self.repo = MarketDataRepository(self.engine, self.config.exchange)
        self.running = True

    def run(self) -> None:
        self._install_signal_handlers()
        while self.running:
            self.repo.write_health(
                "paper_trader",
                "standby",
                "paper trader standby; no entries generated in milestone 1",
                timestamp=datetime.now(UTC),
                last_success_at=datetime.now(UTC),
                metadata_json={"mode": "paper", "live_trading": False},
            )
            logger.info("paper trader standby")
            time.sleep(300)

    def _install_signal_handlers(self) -> None:
        def stop(_signum: int, _frame: object) -> None:
            self.running = False

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)


def main() -> None:
    PaperTraderService().run()


if __name__ == "__main__":
    main()
