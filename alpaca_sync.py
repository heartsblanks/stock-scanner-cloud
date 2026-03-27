from typing import Any

from alpaca.alpaca_orders import fetch_order_by_id, fetch_orders


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def get_order_by_id(order_id: str, nested: bool = True) -> dict[str, Any]:
    return fetch_order_by_id(order_id, nested=nested)


def get_orders(status: str = "all", limit: int = 200, nested: bool = True, direction: str = "desc") -> list[dict[str, Any]]:
    return fetch_orders(status=status, limit=limit, nested=nested, direction=direction)


def _find_exit_legs(order: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    take_profit_leg = None
    stop_loss_leg = None

    for leg in order.get("legs") or []:
        leg_type = str(leg.get("type", "")).strip().lower()
        if leg_type == "limit":
            take_profit_leg = leg
        elif leg_type == "stop":
            stop_loss_leg = leg

    return take_profit_leg, stop_loss_leg


def summarize_order_sync(order: dict[str, Any]) -> dict[str, Any]:
    parent_order_id = str(order.get("id", ""))
    parent_status = str(order.get("status", ""))
    symbol = str(order.get("symbol", ""))
    filled_qty = _to_float(order.get("filled_qty"))
    filled_avg_price = _to_float(order.get("filled_avg_price"))

    take_profit_leg, stop_loss_leg = _find_exit_legs(order)

    tp_status = str((take_profit_leg or {}).get("status", ""))
    sl_status = str((stop_loss_leg or {}).get("status", ""))
    tp_order_id = str((take_profit_leg or {}).get("id", ""))
    sl_order_id = str((stop_loss_leg or {}).get("id", ""))

    tp_filled_qty = _to_float((take_profit_leg or {}).get("filled_qty"))
    sl_filled_qty = _to_float((stop_loss_leg or {}).get("filled_qty"))
    tp_filled_avg_price = _to_float((take_profit_leg or {}).get("filled_avg_price"))
    sl_filled_avg_price = _to_float((stop_loss_leg or {}).get("filled_avg_price"))

    exit_filled_qty = ""
    exit_filled_avg_price = ""

    entry_filled = parent_status == "filled" or filled_qty > 0
    exit_event = ""
    exit_order_id = ""
    exit_price = ""
    exit_reason = ""
    status = "OPEN"

    if tp_status == "filled" or tp_filled_qty > 0:
        exit_event = "TARGET_HIT"
        exit_order_id = tp_order_id
        exit_price = round(tp_filled_avg_price, 4) if tp_filled_avg_price > 0 else ""
        exit_filled_qty = round(tp_filled_qty, 4) if tp_filled_qty > 0 else ""
        exit_filled_avg_price = round(tp_filled_avg_price, 4) if tp_filled_avg_price > 0 else ""
        exit_reason = "TARGET_HIT"
        status = "CLOSED"
    elif sl_status == "filled" or sl_filled_qty > 0:
        exit_event = "STOP_HIT"
        exit_order_id = sl_order_id
        exit_price = round(sl_filled_avg_price, 4) if sl_filled_avg_price > 0 else ""
        exit_filled_qty = round(sl_filled_qty, 4) if sl_filled_qty > 0 else ""
        exit_filled_avg_price = round(sl_filled_avg_price, 4) if sl_filled_avg_price > 0 else ""
        exit_reason = "STOP_HIT"
        status = "CLOSED"
    elif parent_status in {"canceled", "cancelled", "expired", "rejected"}:
        exit_event = "MANUAL_CLOSE"
        exit_order_id = parent_order_id
        exit_reason = parent_status.upper()
        status = "CLOSED"

    return {
        "parent_order_id": parent_order_id,
        "parent_status": parent_status,
        "symbol": symbol,
        "entry_filled": entry_filled,
        "entry_filled_qty": round(filled_qty, 4) if filled_qty > 0 else "",
        "entry_filled_avg_price": round(filled_avg_price, 4) if filled_avg_price > 0 else "",
        "take_profit_order_id": tp_order_id,
        "take_profit_status": tp_status,
        "stop_loss_order_id": sl_order_id,
        "stop_loss_status": sl_status,
        "exit_event": exit_event,
        "exit_order_id": exit_order_id,
        "exit_price": exit_price,
        "exit_filled_qty": exit_filled_qty,
        "exit_filled_avg_price": exit_filled_avg_price,
        "exit_reason": exit_reason,
        "status": status,
    }


def sync_order_by_id(order_id: str) -> dict[str, Any]:
    order = get_order_by_id(order_id, nested=True)
    summary = summarize_order_sync(order)
    summary["raw_order"] = order
    return summary
