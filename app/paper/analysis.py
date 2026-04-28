from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

import pandas as pd


@dataclass(frozen=True)
class TradeExcursionAnalysis:
    gross_pnl_idr: Decimal
    gross_pnl_percent: Decimal
    hold_seconds: int
    max_favorable_excursion_bps: Decimal | None
    max_adverse_excursion_bps: Decimal | None
    horizon_3m_label: str | None
    horizon_5m_label: str | None
    horizon_10m_label: str | None
    label_source: str


def analyze_long_trade(trade: dict, quotes: pd.DataFrame) -> TradeExcursionAnalysis:
    entry_time: datetime = trade["entry_time"]
    exit_time: datetime = trade["exit_time"]
    entry_price = Decimal(str(trade["entry_price"]))
    exit_price = Decimal(str(trade["exit_price"]))
    quantity = Decimal(str(trade["quantity"]))
    notional_idr = Decimal(str(trade["notional_idr"]))
    take_profit_price = Decimal(str(trade["take_profit_price"]))
    stop_loss_price = Decimal(str(trade["stop_loss_price"]))

    usdt_idr_rate = Decimal("0")
    if quantity > 0 and entry_price > 0:
        usdt_idr_rate = notional_idr / (quantity * entry_price)

    gross_pnl_usdt = (exit_price - entry_price) * quantity
    gross_pnl_idr = gross_pnl_usdt * usdt_idr_rate
    gross_pnl_percent = (gross_pnl_idr / notional_idr * Decimal("100")) if notional_idr > 0 else Decimal("0")
    hold_seconds = max(0, int((exit_time - entry_time).total_seconds()))

    if quotes.empty:
        return TradeExcursionAnalysis(
            gross_pnl_idr=gross_pnl_idr,
            gross_pnl_percent=gross_pnl_percent,
            hold_seconds=hold_seconds,
            max_favorable_excursion_bps=None,
            max_adverse_excursion_bps=None,
            horizon_3m_label=None,
            horizon_5m_label=None,
            horizon_10m_label=None,
            label_source="quotes_best_bid",
        )

    frame = quotes.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame["bid"] = pd.to_numeric(frame["bid"])
    frame = frame.sort_values("timestamp")

    max_bid = Decimal(str(frame["bid"].max()))
    min_bid = Decimal(str(frame["bid"].min()))
    max_favorable_excursion_bps = _to_bps(max_bid, entry_price, positive_only=True)
    max_adverse_excursion_bps = _to_adverse_bps(min_bid, entry_price)

    return TradeExcursionAnalysis(
        gross_pnl_idr=gross_pnl_idr,
        gross_pnl_percent=gross_pnl_percent,
        hold_seconds=hold_seconds,
        max_favorable_excursion_bps=max_favorable_excursion_bps,
        max_adverse_excursion_bps=max_adverse_excursion_bps,
        horizon_3m_label=_horizon_label(frame, entry_time, take_profit_price, stop_loss_price, minutes=3),
        horizon_5m_label=_horizon_label(frame, entry_time, take_profit_price, stop_loss_price, minutes=5),
        horizon_10m_label=_horizon_label(frame, entry_time, take_profit_price, stop_loss_price, minutes=10),
        label_source="quotes_best_bid",
    )


def _horizon_label(
    quotes: pd.DataFrame,
    entry_time: datetime,
    take_profit_price: Decimal,
    stop_loss_price: Decimal,
    *,
    minutes: int,
) -> str | None:
    horizon_end = entry_time + timedelta(minutes=minutes)
    subset = quotes[quotes["timestamp"] <= pd.Timestamp(horizon_end)]
    if subset.empty:
        return None
    coverage_seconds = int((subset["timestamp"].max().to_pydatetime() - entry_time).total_seconds())
    if coverage_seconds < (minutes * 60) - 20:
        return None

    for row in subset.itertuples(index=False):
        bid = Decimal(str(row.bid))
        if bid >= take_profit_price:
            return "tp_first"
        if bid <= stop_loss_price:
            return "sl_first"
    return "neither"


def _to_bps(price: Decimal, reference_price: Decimal, *, positive_only: bool) -> Decimal:
    if reference_price <= 0:
        return Decimal("0")
    bps = (price - reference_price) / reference_price * Decimal("10000")
    if positive_only:
        return max(bps, Decimal("0"))
    return bps


def _to_adverse_bps(min_price: Decimal, entry_price: Decimal) -> Decimal:
    if entry_price <= 0:
        return Decimal("0")
    return max((entry_price - min_price) / entry_price * Decimal("10000"), Decimal("0"))
