"""Helpers for syncing Alpaca bracket order state through the centralized order client."""
from datetime import datetime
import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from alpaca.alpaca_orders import fetch_order_by_id, fetch_orders


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_alpaca_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        return None


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


def _find_external_exit_order(parent_order: dict[str, Any]) -> dict[str, Any] | None:
    """
    Look for a filled order that closes the position but is NOT a TP/SL leg of the parent.

    Matching rules:
    - same symbol
    - opposite side to parent
    - filled_qty > 0
    - different order id
    - not one of the parent TP/SL leg ids
    - submitted/filled after the parent order was submitted
    - prefer exact quantity match
    - then prefer the earliest qualifying exit after the parent, not an older historical exit
    """
    try:
        symbol = str(parent_order.get("symbol", "")).strip().upper()
        parent_id = str(parent_order.get("id", "")).strip()
        parent_side = str(parent_order.get("side", "")).strip().lower()
        parent_submitted_at = _parse_alpaca_datetime(parent_order.get("submitted_at"))
        parent_filled_qty = _to_float(parent_order.get("filled_qty"))

        leg_ids = {
            str(leg.get("id", "")).strip()
            for leg in (parent_order.get("legs") or [])
            if str(leg.get("id", "")).strip()
        }

        opposite_side = "sell" if parent_side == "buy" else "buy"
        orders = fetch_orders(status="closed", limit=200, nested=False, direction="desc")

        candidates: list[tuple[int, datetime, str, dict[str, Any]]] = []
        for o in orders or []:
            try:
                candidate_id = str(o.get("id", "")).strip()
                if str(o.get("symbol", "")).strip().upper() != symbol:
                    continue
                if not candidate_id or candidate_id == parent_id or candidate_id in leg_ids:
                    continue
                if str(o.get("side", "")).strip().lower() != opposite_side:
                    continue

                filled_qty = _to_float(o.get("filled_qty"))
                if filled_qty <= 0:
                    continue

                status = str(o.get("status", "")).strip().lower()
                if status != "filled":
                    continue

                submitted_at = _parse_alpaca_datetime(o.get("submitted_at"))
                filled_at = _parse_alpaca_datetime(o.get("filled_at"))
                effective_time = filled_at or submitted_at
                if not effective_time:
                    continue

                if parent_submitted_at:
                    if submitted_at and submitted_at < parent_submitted_at:
                        continue
                    if filled_at and filled_at < parent_submitted_at:
                        continue

                qty_rank = 0 if parent_filled_qty > 0 and abs(filled_qty - parent_filled_qty) < 0.0001 else 1
                candidates.append((qty_rank, effective_time, candidate_id, o))
            except Exception:
                continue

        if not candidates:
            return None

        candidates.sort(key=lambda item: (item[0], item[1], item[2]))
        return candidates[0][3]
    except Exception:
        return None


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

    # --- Fallback: detect external/manual close orders (EOD or separate exit) ---
    if not exit_event:
        external = _find_external_exit_order(order)
        if external:
            ext_id = str(external.get("id", ""))
            ext_filled_qty = _to_float(external.get("filled_qty"))
            ext_price = _to_float(external.get("filled_avg_price"))
            exit_event = "MANUAL_CLOSE"
            exit_order_id = ext_id
            exit_price = round(ext_price, 4) if ext_price > 0 else ""
            exit_filled_qty = round(ext_filled_qty, 4) if ext_filled_qty > 0 else ""
            exit_filled_avg_price = round(ext_price, 4) if ext_price > 0 else ""
            exit_reason = "EXTERNAL_EXIT"
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
