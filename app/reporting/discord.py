from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import httpx


@dataclass
class DiscordAlertClient:
    webhook_url: str | None
    cooldown_seconds: int = 900
    _last_sent: dict[str, datetime] = field(default_factory=dict)

    def send(self, key: str, message: str, *, force: bool = False) -> bool:
        if not self.webhook_url:
            return False

        now = datetime.now(UTC)
        last_sent = self._last_sent.get(key)
        if not force and last_sent and now - last_sent < timedelta(seconds=self.cooldown_seconds):
            return False

        response = httpx.post(self.webhook_url, json={"content": message}, timeout=10)
        response.raise_for_status()
        self._last_sent[key] = now
        return True
