from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import pandas as pd

from app.config import PaperTradingConfig, RiskConfig


@dataclass(frozen=True)
class Signal:
    decision: str
    side: str | None
    confidence: Decimal
    reason: str
    features: dict


def baseline_long_signal(candles: pd.DataFrame, spread_bps: Decimal, max_spread_bps: Decimal) -> Signal:
    if len(candles) < 30:
        return Signal("SKIP", None, Decimal("0"), "not enough candles", {"rows": len(candles)})

    frame = candles.copy()
    frame["close"] = pd.to_numeric(frame["close"])
    frame["volume"] = pd.to_numeric(frame["volume"])
    frame["ema_9"] = frame["close"].ewm(span=9, adjust=False).mean()
    frame["ema_21"] = frame["close"].ewm(span=21, adjust=False).mean()
    frame["volume_avg_20"] = frame["volume"].rolling(20).mean()
    latest = frame.iloc[-1]

    features = {
        "ema_9": float(latest["ema_9"]),
        "ema_21": float(latest["ema_21"]),
        "volume": float(latest["volume"]),
        "volume_avg_20": float(latest["volume_avg_20"]),
        "spread_bps": float(spread_bps),
    }
    if spread_bps > max_spread_bps:
        return Signal("SKIP", None, Decimal("0"), "spread too wide", features)
    if latest["ema_9"] <= latest["ema_21"]:
        return Signal("SKIP", None, Decimal("0"), "ema trend not bullish", features)
    if latest["volume"] <= latest["volume_avg_20"]:
        return Signal("SKIP", None, Decimal("0"), "volume below rolling average", features)
    return Signal("TAKE", "long", Decimal("0.55"), "baseline long conditions met", features)


def micro_momentum_burst_signal(feature: dict, quote: dict, paper: PaperTradingConfig, risk: RiskConfig) -> Signal:
    features = {
        "feature_open_time": feature.get("open_time"),
        "quote_timestamp": quote.get("timestamp"),
        "quote_count": feature.get("quote_count"),
        "trade_count": feature.get("trade_count"),
        "order_book_count": feature.get("order_book_count"),
        "spread_bps_avg": feature.get("spread_bps_avg"),
        "latest_spread_bps": quote.get("spread_bps"),
        "trade_flow_imbalance": feature.get("trade_flow_imbalance"),
        "orderbook_imbalance_avg": feature.get("orderbook_imbalance_avg"),
        "volatility_1m": feature.get("volatility_1m"),
        "take_profit_bps": paper.take_profit_bps,
        "stop_loss_bps": paper.stop_loss_bps,
        "fee_rate_bps": paper.fee_rate_bps,
        "slippage_bps": paper.slippage_bps,
    }

    spread_bps = Decimal(str(quote.get("spread_bps") or feature.get("spread_bps_avg") or 0))
    trade_flow = Decimal(str(feature.get("trade_flow_imbalance") or 0))
    orderbook_imbalance = Decimal(str(feature.get("orderbook_imbalance_avg") or 0))
    volatility = Decimal(str(feature.get("volatility_1m") or 0))
    quote_count = int(feature.get("quote_count") or 0)
    trade_count = int(feature.get("trade_count") or 0)
    order_book_count = int(feature.get("order_book_count") or 0)

    if quote_count < paper.min_quote_count:
        return Signal("SKIP", None, Decimal("0"), "not enough quote samples", features)
    if trade_count < paper.min_trade_count:
        return Signal("SKIP", None, Decimal("0"), "not enough trade samples", features)
    if order_book_count < paper.min_order_book_count:
        return Signal("SKIP", None, Decimal("0"), "not enough order book samples", features)
    if spread_bps > Decimal(str(risk.max_spread_bps)):
        return Signal("SKIP", None, Decimal("0"), "spread too wide", features)
    if volatility < Decimal(str(paper.min_volatility_bps)):
        return Signal("SKIP", None, Decimal("0"), "volatility too low to cover costs", features)
    if trade_flow < Decimal(str(paper.min_trade_flow_imbalance)):
        return Signal("SKIP", None, Decimal("0"), "trade flow not bullish enough", features)
    if orderbook_imbalance < Decimal(str(paper.min_orderbook_imbalance)):
        return Signal("SKIP", None, Decimal("0"), "order book imbalance not bullish enough", features)

    confidence = Decimal("0.50")
    confidence += min(trade_flow, Decimal("1")) * Decimal("0.20")
    confidence += min(orderbook_imbalance, Decimal("1")) * Decimal("0.15")
    confidence += min(volatility / Decimal("20"), Decimal("1")) * Decimal("0.10")
    confidence = min(confidence, Decimal("0.90"))
    return Signal("TAKE", "long", confidence, "micro momentum burst conditions met", features)
