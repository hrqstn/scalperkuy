from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import pandas as pd


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
