from __future__ import annotations

from typing import Any


def to_float_fallback(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_upper_or_none(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    return text or None


def infer_direction(entry_price, exit_price, stop_price, target_price, side=None) -> str | None:
    normalized_side = to_upper_or_none(side)
    if normalized_side == "BUY":
        return "LONG"
    if normalized_side == "SELL":
        return "SHORT"

    entry_val = to_float_fallback(entry_price)
    stop_val = to_float_fallback(stop_price)
    target_val = to_float_fallback(target_price)
    exit_val = to_float_fallback(exit_price)

    if entry_val is not None and target_val is not None and stop_val is not None:
        if target_val > entry_val and stop_val < entry_val:
            return "LONG"
        if target_val < entry_val and stop_val > entry_val:
            return "SHORT"

    if entry_val is not None and exit_val is not None:
        if exit_val > entry_val:
            return "LONG"
        if exit_val < entry_val:
            return "SHORT"

    return None


def resolve_lifecycle_side(open_row: dict[str, Any], direction: str | None) -> str | None:
    open_side = to_upper_or_none(open_row.get("side", ""))
    if open_side:
        return open_side

    direction_val = str(direction or "").strip().upper()
    if direction_val == "LONG":
        return "BUY"
    if direction_val == "SHORT":
        return "SELL"
    return None


def compute_realized_pnl(entry_price, exit_price, shares, direction):
    entry_val = to_float_fallback(entry_price)
    exit_val = to_float_fallback(exit_price)
    shares_val = to_float_fallback(shares)
    direction_val = str(direction or "").strip().upper()

    if entry_val is None or exit_val is None or shares_val is None:
        return None

    if direction_val == "LONG":
        return round((exit_val - entry_val) * shares_val, 6)
    if direction_val == "SHORT":
        return round((entry_val - exit_val) * shares_val, 6)
    return None


def compute_realized_pnl_percent(entry_price, exit_price, direction):
    entry_val = to_float_fallback(entry_price)
    exit_val = to_float_fallback(exit_price)
    direction_val = str(direction or "").strip().upper()

    if entry_val in (None, 0) or exit_val is None:
        return None

    if direction_val == "LONG":
        return round(((exit_val - entry_val) / entry_val) * 100.0, 6)
    if direction_val == "SHORT":
        return round(((entry_val - exit_val) / entry_val) * 100.0, 6)
    return None


def compute_duration_minutes(entry_timestamp, exit_timestamp):
    if entry_timestamp is None or exit_timestamp is None:
        return None
    try:
        return round((exit_timestamp - entry_timestamp).total_seconds() / 60.0, 2)
    except Exception:
        return None


def normalize_trade_key(symbol: str, broker_parent_order_id: str, broker_order_id: str, broker: str | None = None) -> str:
    normalized_broker = str(broker or "").strip().upper()
    base_key = broker_parent_order_id or broker_order_id or symbol
    if normalized_broker == "IBKR":
        normalized_symbol = str(symbol or "").strip().upper()
        if normalized_symbol and base_key:
            return f"{normalized_broker}:{normalized_symbol}:{base_key}"
    return base_key
