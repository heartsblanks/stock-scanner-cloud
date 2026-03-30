"""Paper Alpaca helpers built on the centralized Alpaca client/order/position layers."""
import os
from typing import Any

from alpaca.alpaca_client import alpaca_client
from alpaca.alpaca_orders import cancel_order_by_id, fetch_open_orders
from alpaca.alpaca_positions import close_position_by_symbol, fetch_positions
ALPACA_MAX_NOTIONAL = float(os.getenv("ALPACA_MAX_NOTIONAL", "5000"))
MIN_NOTIONAL_TO_PLACE = float(os.getenv("MIN_NOTIONAL_TO_PLACE", "50"))


def get_account() -> dict[str, Any]:
    data = alpaca_client.get("/v2/account")
    if isinstance(data, dict):
        return data
    raise ValueError("Unexpected Alpaca account response format")


def get_open_positions() -> list[dict[str, Any]]:
    return fetch_positions()


def get_open_orders() -> list[dict[str, Any]]:
    return fetch_open_orders(limit=500, nested=True, direction="desc")


def cancel_open_orders_for_symbol(symbol: str) -> list[str]:
    symbol = str(symbol).strip().upper()
    if not symbol:
        raise ValueError("symbol is required")

    open_orders = get_open_orders()
    canceled_order_ids: list[str] = []

    for order in open_orders:
        order_symbol = str(order.get("symbol", "")).strip().upper()
        if order_symbol != symbol:
            continue

        order_id = str(order.get("id", "")).strip()
        if not order_id:
            continue

        cancel_order_by_id(order_id)
        canceled_order_ids.append(order_id)

        for leg in order.get("legs") or []:
            leg_id = str(leg.get("id", "")).strip()
            if leg_id:
                canceled_order_ids.append(leg_id)

    return canceled_order_ids


def close_position(symbol: str, cancel_orders: bool = True) -> dict[str, Any]:
    return close_position_by_symbol(symbol, cancel_orders=cancel_orders)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _resolve_trade_notional_limit(trade: dict, explicit_max_notional: float | None) -> float:
    if explicit_max_notional is not None:
        return float(explicit_max_notional)

    metrics = trade.get("metrics", {}) if isinstance(trade, dict) else {}
    per_trade_notional = _to_float(metrics.get("per_trade_notional"), 0.0)
    if per_trade_notional > 0:
        return per_trade_notional

    return ALPACA_MAX_NOTIONAL


def _build_client_order_id(symbol: str, direction: str, entry: float, final_shares: int) -> str:
    symbol = str(symbol).strip().upper()
    direction = str(direction).strip().upper()
    direction_tag = "BUY" if direction == "BUY" else "SELL"
    price_tag = int(round(entry * 10000))
    return f"scanner-{symbol}-{direction_tag}-{price_tag}-{final_shares}"


def place_paper_bracket_order_from_trade(trade: dict, max_notional: float | None = None) -> dict[str, Any]:
    max_notional = _resolve_trade_notional_limit(trade, max_notional)
    metrics = trade.get("metrics", {})
    symbol = str(metrics.get("symbol", "")).strip().upper()
    direction = str(metrics.get("direction", "BUY")).strip().upper() or "BUY"
    entry = _to_float(metrics.get("entry"))
    stop = _to_float(metrics.get("stop"))
    target = _to_float(metrics.get("target"))
    scanner_shares = int(_to_float(metrics.get("shares"), 0))
    per_trade_notional = _to_float(metrics.get("per_trade_notional"), 0.0)
    remaining_slots = int(_to_float(metrics.get("remaining_slots"), 0))
    remaining_allocatable_capital = _to_float(metrics.get("remaining_allocatable_capital"), 0.0)
    current_open_positions = int(_to_float(metrics.get("current_open_positions"), 0))
    current_open_exposure = _to_float(metrics.get("current_open_exposure"), 0.0)
    max_total_allocated_capital = _to_float(metrics.get("max_total_allocated_capital"), 0.0)
    max_capital_allocation_pct = _to_float(metrics.get("max_capital_allocation_pct"), 0.0)
    sizing_source = "dynamic_per_trade_notional" if per_trade_notional > 0 else "fallback_max_notional"

    if not symbol:
        return {"attempted": False, "placed": False, "reason": "missing_symbol"}
    if entry <= 0:
        return {"attempted": False, "placed": False, "symbol": symbol, "reason": "invalid_entry_price"}
    if stop <= 0 or target <= 0:
        return {"attempted": False, "placed": False, "symbol": symbol, "reason": "invalid_stop_or_target"}
    if direction == "BUY":
        if stop >= entry:
            return {"attempted": False, "placed": False, "symbol": symbol, "reason": "stop_must_be_below_entry"}
        if target <= entry:
            return {"attempted": False, "placed": False, "symbol": symbol, "reason": "target_must_be_above_entry"}
    elif direction == "SELL":
        if stop <= entry:
            return {"attempted": False, "placed": False, "symbol": symbol, "reason": "short_stop_must_be_above_entry"}
        if target >= entry:
            return {"attempted": False, "placed": False, "symbol": symbol, "reason": "short_target_must_be_below_entry"}
    else:
        return {"attempted": False, "placed": False, "symbol": symbol, "reason": "invalid_direction"}

    if remaining_slots <= 0:
        return {
            "attempted": False,
            "placed": False,
            "symbol": symbol,
            "reason": "no_remaining_position_slots",
            "remaining_slots": remaining_slots,
            "remaining_allocatable_capital": round(remaining_allocatable_capital, 2),
        }

    if direction == "SELL":
        minimum_short_stop = round(entry + 0.01, 2)
        if stop < minimum_short_stop:
            stop = minimum_short_stop

    if max_notional < MIN_NOTIONAL_TO_PLACE:
        return {
            "attempted": False,
            "placed": False,
            "symbol": symbol,
            "reason": "notional_limit_too_small",
            "max_notional": round(max_notional, 2),
            "minimum_required_notional": round(MIN_NOTIONAL_TO_PLACE, 2),
            "remaining_slots": remaining_slots,
            "remaining_allocatable_capital": round(remaining_allocatable_capital, 2),
        }
    capped_shares = int(max_notional / entry)
    if scanner_shares > 0:
        final_shares = min(scanner_shares, capped_shares)
    else:
        final_shares = capped_shares
    if final_shares <= 0:
        return {
            "attempted": False,
            "placed": False,
            "symbol": symbol,
            "reason": "max_notional_too_small_for_symbol",
            "max_notional": round(max_notional, 2),
            "entry": round(entry, 4),
            "scanner_shares": scanner_shares,
            "capped_shares": capped_shares,
            "sizing_source": sizing_source,
        }

    estimated_notional = round(final_shares * entry, 2)
    actual_risk = _to_float(metrics.get("actual_risk"), 0.0)
    take_profit_dollars = _to_float(metrics.get("take_profit_dollars"), 0.0)
    account = get_account()
    account_status = str(account.get("status", "")).strip().upper()
    buying_power = _to_float(account.get("buying_power"))
    cash = _to_float(account.get("cash"))
    available_funds = buying_power if buying_power > 0 else cash

    if account_status and account_status != "ACTIVE":
        return {
            "attempted": True,
            "placed": False,
            "symbol": symbol,
            "shares": final_shares,
            "estimated_notional": estimated_notional,
            "per_trade_notional": round(max_notional, 2),
            "remaining_slots": remaining_slots,
            "remaining_allocatable_capital": round(remaining_allocatable_capital, 2),
            "scanner_shares": scanner_shares,
            "capped_shares": capped_shares,
            "sizing_source": sizing_source,
            "reason": "account_not_active",
            "account_status": account_status,
        }
    if estimated_notional > available_funds:
        return {
            "attempted": True,
            "placed": False,
            "symbol": symbol,
            "shares": final_shares,
            "estimated_notional": estimated_notional,
            "per_trade_notional": round(max_notional, 2),
            "remaining_slots": remaining_slots,
            "remaining_allocatable_capital": round(remaining_allocatable_capital, 2),
            "scanner_shares": scanner_shares,
            "capped_shares": capped_shares,
            "sizing_source": sizing_source,
            "reason": "insufficient_buying_power",
            "available_funds": round(available_funds, 2),
        }

    client_order_id = _build_client_order_id(symbol, direction, entry, final_shares)
    payload = {
        "symbol": symbol,
        "qty": str(final_shares),
        "side": "buy" if direction == "BUY" else "sell",
        "type": "market",
        "time_in_force": "day",
        "order_class": "bracket",
        "take_profit": {"limit_price": f"{target:.2f}"},
        "stop_loss": {"stop_price": f"{stop:.2f}"},
        "client_order_id": client_order_id,
    }

    try:
        order = alpaca_client.post("/v2/orders", json_body=payload)
    except Exception as e:
        return {
            "attempted": True,
            "placed": False,
            "symbol": symbol,
            "shares": final_shares,
            "estimated_notional": estimated_notional,
            "per_trade_notional": round(max_notional, 2),
            "remaining_slots": remaining_slots,
            "remaining_allocatable_capital": round(remaining_allocatable_capital, 2),
            "scanner_shares": scanner_shares,
            "capped_shares": capped_shares,
            "sizing_source": sizing_source,
            "reason": "alpaca_order_rejected",
            "details": str(e),
        }
    legs = order.get("legs") or []
    take_profit_leg_id = ""
    stop_loss_leg_id = ""
    for leg in legs:
        leg_type = str(leg.get("type", "")).strip().lower()
        if leg_type == "limit":
            take_profit_leg_id = str(leg.get("id", ""))
        elif leg_type == "stop":
            stop_loss_leg_id = str(leg.get("id", ""))

    return {
        "attempted": True,
        "placed": True,
        "symbol": symbol,
        "direction": direction,
        "shares": final_shares,
        "estimated_notional": estimated_notional,
        "entry": round(entry, 4),
        "stop": round(stop, 4),
        "target": round(target, 4),
        "max_notional": round(max_notional, 2),
        "per_trade_notional": round(per_trade_notional if per_trade_notional > 0 else max_notional, 2),
        "remaining_slots": remaining_slots,
        "remaining_allocatable_capital": round(remaining_allocatable_capital, 2),
        "current_open_positions": current_open_positions,
        "current_open_exposure": round(current_open_exposure, 2),
        "max_total_allocated_capital": round(max_total_allocated_capital, 2),
        "max_capital_allocation_pct": round(max_capital_allocation_pct, 4),
        "actual_risk": round(actual_risk, 4),
        "take_profit_dollars": round(take_profit_dollars, 4),
        "scanner_shares": scanner_shares,
        "capped_shares": capped_shares,
        "sizing_source": sizing_source,
        "client_order_id": client_order_id,
        "alpaca_order_id": str(order.get("id", "")),
        "alpaca_order_status": str(order.get("status", "")),
        "alpaca_order_class": str(order.get("order_class", "")),
        "alpaca_take_profit_order_id": take_profit_leg_id,
        "alpaca_stop_loss_order_id": stop_loss_leg_id,
        "account_buying_power": round(buying_power, 2),
        "account_cash": round(cash, 2),
    }