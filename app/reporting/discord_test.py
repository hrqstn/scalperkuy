from __future__ import annotations

from app.config import load_config
from app.reporting.discord import DiscordAlertClient


def main() -> None:
    config = load_config()
    alerts = DiscordAlertClient(
        config.discord_webhook_url,
        cooldown_seconds=config.alerts_config.cooldown_seconds,
    )
    sent = alerts.send(
        "manual-test",
        "Scalperkuy Discord alert test. If you see this, the webhook is working.",
        force=True,
    )
    if sent:
        print("Discord test alert sent.")
    else:
        print("DISCORD_WEBHOOK_URL is empty; no alert sent.")


if __name__ == "__main__":
    main()
