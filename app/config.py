from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class RiskConfig(BaseModel):
    daily_profit_target_percent: float = 1.0
    daily_max_loss_percent: float = 1.0
    risk_per_trade_percent: float = 0.1
    max_position_size_percent: float = 25.0
    max_trades_per_day: int = 10
    max_consecutive_losses: int = 3
    pause_after_consecutive_losses_minutes: int = 60
    max_spread_bps: float = 8.0


class CompoundingConfig(BaseModel):
    enabled: bool = True
    update_frequency: str = "weekly"
    max_weekly_size_increase_percent: float = 10.0
    reduce_size_immediately_on_drawdown: bool = True


class DataConfig(BaseModel):
    candle_timeframe: str = "1m"
    candle_poll_seconds: int = 30
    quote_interval_seconds: int = 5
    order_book_depth: int = 20
    order_book_interval_seconds: int = 10
    trades_interval_seconds: int = 30
    dashboard_refresh_seconds: int = 10
    stale_data_seconds: int = 120


class TokocryptoConfig(BaseModel):
    base_url: str = "https://www.tokocrypto.site"
    request_timeout_seconds: int = 10


class AlertsConfig(BaseModel):
    cooldown_seconds: int = 900


class AppConfig(BaseModel):
    exchange: str = "Tokocrypto"
    mode: str = "paper"
    starting_balance_idr: int = 1_000_000
    timezone: str = "Asia/Jakarta"
    symbols: list[str] = Field(default_factory=lambda: ["BTC/USDT", "ETH/USDT"])
    risk: RiskConfig = Field(default_factory=RiskConfig)
    compounding: CompoundingConfig = Field(default_factory=CompoundingConfig)
    data: DataConfig = Field(default_factory=DataConfig)
    tokocrypto: TokocryptoConfig = Field(default_factory=TokocryptoConfig)
    alerts_config: AlertsConfig = Field(default_factory=AlertsConfig)
    database_url: str = "postgresql+psycopg://scalperkuy:scalperkuy_dev_password@postgres:5432/scalperkuy"
    discord_webhook_url: str | None = None


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def load_config() -> AppConfig:
    config_path = Path(os.getenv("CONFIG_PATH", "config.example.yaml"))
    raw = _read_yaml(config_path)
    raw["database_url"] = os.getenv("DATABASE_URL", raw.get("database_url", AppConfig().database_url))
    raw["discord_webhook_url"] = os.getenv("DISCORD_WEBHOOK_URL") or raw.get("discord_webhook_url")
    return AppConfig.model_validate(raw)


def exchange_symbol(symbol: str) -> str:
    return symbol.replace("/", "").upper()
