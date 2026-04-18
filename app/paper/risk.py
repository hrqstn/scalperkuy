from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from app.config import RiskConfig


@dataclass(frozen=True)
class EntryDecision:
    allowed: bool
    reason: str
    max_position_idr: Decimal
    risk_idr: Decimal


class RiskManager:
    """Deterministic paper-trading risk checks. LLMs must never call shots here."""

    def __init__(self, config: RiskConfig, equity_idr: Decimal) -> None:
        self.config = config
        self.equity_idr = equity_idr

    def evaluate_entry(
        self,
        *,
        realized_pnl_idr: Decimal,
        trade_count: int,
        consecutive_losses: int,
        spread_bps: Decimal,
        has_take_profit: bool,
        has_stop_loss: bool,
    ) -> EntryDecision:
        risk_idr = self.equity_idr * Decimal(str(self.config.risk_per_trade_percent)) / Decimal("100")
        max_position_idr = self.equity_idr * Decimal(str(self.config.max_position_size_percent)) / Decimal("100")

        daily_loss_limit = -(self.equity_idr * Decimal(str(self.config.daily_max_loss_percent)) / Decimal("100"))
        daily_profit_target = self.equity_idr * Decimal(str(self.config.daily_profit_target_percent)) / Decimal("100")

        if realized_pnl_idr <= daily_loss_limit:
            return EntryDecision(False, "daily max loss reached", max_position_idr, risk_idr)
        if realized_pnl_idr >= daily_profit_target:
            return EntryDecision(False, "daily profit target reached", max_position_idr, risk_idr)
        if trade_count >= self.config.max_trades_per_day:
            return EntryDecision(False, "max trades per day reached", max_position_idr, risk_idr)
        if consecutive_losses >= self.config.max_consecutive_losses:
            return EntryDecision(False, "max consecutive losses reached", max_position_idr, risk_idr)
        if spread_bps > Decimal(str(self.config.max_spread_bps)):
            return EntryDecision(False, "spread too wide", max_position_idr, risk_idr)
        if not has_take_profit or not has_stop_loss:
            return EntryDecision(False, "take profit and stop loss are required", max_position_idr, risk_idr)
        return EntryDecision(True, "allowed", max_position_idr, risk_idr)
