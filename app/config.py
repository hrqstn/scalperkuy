from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class RiskConfig(BaseModel):
    daily_profit_target_percent: float = 1.0
    daily_max_loss_percent: float = 0.5
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
    service_health_ok_interval_seconds: int = 60


class AggregationConfig(BaseModel):
    enabled: bool = True
    interval_seconds: int = 60
    lookback_minutes: int = 180


class PaperTradingConfig(BaseModel):
    enabled: bool = False
    interval_seconds: int = 30
    strategy_name: str = "micro_momentum_burst_v0"
    usdt_idr_rate: float = 16_000.0
    fee_rate_bps: float = 10.0
    slippage_bps: float = 2.0
    take_profit_bps: float = 40.0
    stop_loss_bps: float = 20.0
    max_holding_minutes: int = 5
    cooldown_after_trade_seconds: int = 300
    cooldown_after_loss_seconds: int = 900
    max_feature_age_seconds: int = 180
    min_quote_count: int = 3
    min_trade_count: int = 3
    min_order_book_count: int = 3
    min_trade_flow_imbalance: float = 0.35
    min_orderbook_imbalance: float = 0.15
    min_volatility_bps: float = 2.0
    dynamic_exit_on_trade_flow_negative: bool = True
    dynamic_exit_on_orderbook_negative: bool = True
    experiments: list["PaperTradingExperimentConfig"] = Field(default_factory=list)


class ExperimentRiskOverrides(BaseModel):
    daily_profit_target_percent: float | None = None
    daily_max_loss_percent: float | None = None
    risk_per_trade_percent: float | None = None
    max_position_size_percent: float | None = None
    max_trades_per_day: int | None = None
    max_consecutive_losses: int | None = None
    pause_after_consecutive_losses_minutes: int | None = None
    max_spread_bps: float | None = None


class PaperTradingExperimentConfig(BaseModel):
    name: str
    enabled: bool = True
    strategy_name: str | None = None
    fee_rate_bps: float | None = None
    slippage_bps: float | None = None
    take_profit_bps: float | None = None
    stop_loss_bps: float | None = None
    max_holding_minutes: int | None = None
    cooldown_after_trade_seconds: int | None = None
    cooldown_after_loss_seconds: int | None = None
    max_feature_age_seconds: int | None = None
    min_quote_count: int | None = None
    min_trade_count: int | None = None
    min_order_book_count: int | None = None
    min_trade_flow_imbalance: float | None = None
    min_orderbook_imbalance: float | None = None
    min_volatility_bps: float | None = None
    dynamic_exit_on_trade_flow_negative: bool | None = None
    dynamic_exit_on_orderbook_negative: bool | None = None
    risk: ExperimentRiskOverrides = Field(default_factory=ExperimentRiskOverrides)


class ReportingConfig(BaseModel):
    enabled: bool = True
    interval_seconds: int = 900
    discord_daily_summary_enabled: bool = False


class TokocryptoConfig(BaseModel):
    base_url: str = "https://www.tokocrypto.site"
    request_timeout_seconds: int = 10
    max_retries: int = 3
    retry_backoff_seconds: float = 1.5


class AlertsConfig(BaseModel):
    cooldown_seconds: int = 900
    disk_usage_warning_percent: float = 85.0
    disk_check_interval_seconds: int = 300


class AppConfig(BaseModel):
    exchange: str = "Tokocrypto"
    mode: str = "paper"
    starting_balance_idr: int = 1_000_000
    timezone: str = "Asia/Jakarta"
    symbols: list[str] = Field(default_factory=lambda: ["BTC/USDT", "ETH/USDT"])
    risk: RiskConfig = Field(default_factory=RiskConfig)
    compounding: CompoundingConfig = Field(default_factory=CompoundingConfig)
    data: DataConfig = Field(default_factory=DataConfig)
    aggregation: AggregationConfig = Field(default_factory=AggregationConfig)
    paper_trading: PaperTradingConfig = Field(default_factory=PaperTradingConfig)
    reporting: ReportingConfig = Field(default_factory=ReportingConfig)
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


PaperTradingConfig.model_rebuild()
