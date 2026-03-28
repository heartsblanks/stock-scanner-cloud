from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

def _normalize_trade_key(symbol: str, broker_parent_order_id: str, broker_order_id: str) -> str:
    return broker_parent_order_id or broker_order_id or symbol


def _to_upper_or_none(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    return text or None


def _infer_direction(entry_price, exit_price, stop_price, target_price) -> str | None:
    try:
        entry_val = float(entry_price) if entry_price not in (None, "") else None
    except Exception:
        entry_val = None
    try:
        exit_val = float(exit_price) if exit_price not in (None, "") else None
    except Exception:
        exit_val = None
    try:
        stop_val = float(stop_price) if stop_price not in (None, "") else None
    except Exception:
        stop_val = None
    try:
        target_val = float(target_price) if target_price not in (None, "") else None
    except Exception:
        target_val = None

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


def _compute_realized_pnl(entry_price, exit_price, shares, direction):
    try:
        entry_val = float(entry_price) if entry_price not in (None, "") else None
    except Exception:
        entry_val = None
    try:
        exit_val = float(exit_price) if exit_price not in (None, "") else None
    except Exception:
        exit_val = None
    try:
        shares_val = float(shares) if shares not in (None, "") else None
    except Exception:
        shares_val = None

    if entry_val is None or exit_val is None or shares_val is None:
        return None

    direction_val = str(direction or "").strip().upper()
    if direction_val == "LONG":
        return round((exit_val - entry_val) * shares_val, 6)
    if direction_val == "SHORT":
        return round((entry_val - exit_val) * shares_val, 6)
    return None


def _compute_realized_pnl_percent(entry_price, exit_price, direction):
    try:
        entry_val = float(entry_price) if entry_price not in (None, "") else None
    except Exception:
        entry_val = None
    try:
        exit_val = float(exit_price) if exit_price not in (None, "") else None
    except Exception:
        exit_val = None

    if entry_val in (None, 0) or exit_val is None:
        return None

    direction_val = str(direction or "").strip().upper()
    if direction_val == "LONG":
        return round(((exit_val - entry_val) / entry_val) * 100.0, 6)
    if direction_val == "SHORT":
        return round(((entry_val - exit_val) / entry_val) * 100.0, 6)
    return None


def _compute_duration_minutes(entry_timestamp, exit_timestamp):
    if entry_timestamp is None or exit_timestamp is None:
        return None
    try:
        return round((exit_timestamp - entry_timestamp).total_seconds() / 60.0, 2)
    except Exception:
        return None

def execute_close_all_paper_positions(
    *,
    get_open_positions: Callable[[], list[dict[str, Any]]],
    get_managed_open_paper_trades_for_eod_close: Callable[[], list[dict[str, Any]]],
    cancel_open_orders_for_symbol: Callable[[str], list[str]],
    close_position: Callable[[str], dict[str, Any]],
    get_order_by_id: Callable[..., dict[str, Any]],
    safe_insert_broker_order: Callable[..., None],
    append_trade_log: Callable[[dict[str, Any]], None],
    safe_insert_trade_event: Callable[..., None],
    upsert_trade_lifecycle: Callable[..., None],
    to_float_or_none: Callable[[Any], float | None],
    parse_iso_utc: Callable[[str], Any],
) -> dict[str, Any] | tuple[dict[str, Any], int]:
    try:
        positions = get_open_positions()
    except Exception as e:
        print(f"Open position read failed: {e}", flush=True)
        return {"ok": False, "error": f"open position read failed: {e}"}, 500

    open_paper_rows = get_managed_open_paper_trades_for_eod_close()
    open_paper_symbols = {
        str(row.get("symbol", "")).strip().upper()
        for row in open_paper_rows
        if str(row.get("symbol", "")).strip()
    }
    open_paper_rows_by_symbol = {
        str(row.get("symbol", "")).strip().upper(): row
        for row in open_paper_rows
        if str(row.get("symbol", "")).strip()
    }

    results: list[dict[str, Any]] = []
    closed_count = 0
    skipped_count = 0

    for position in positions:
        symbol = str(position.get("symbol", "")).strip().upper()
        qty = str(position.get("qty", "")).strip()
        side = str(position.get("side", "")).strip().lower()
        current_price = position.get("current_price", "")

        if not symbol:
            skipped_count += 1
            results.append({
                "closed": False,
                "reason": "missing_symbol",
            })
            continue

        if symbol not in open_paper_symbols:
            skipped_count += 1
            results.append({
                "symbol": symbol,
                "closed": False,
                "reason": "not_managed_by_app",
            })
            continue

        try:
            canceled_order_ids = cancel_open_orders_for_symbol(symbol)
        except Exception as e:
            print(f"Paper open-order cancel failed for {symbol}: {e}", flush=True)
            skipped_count += 1
            results.append({
                "symbol": symbol,
                "closed": False,
                "reason": "cancel_open_orders_exception",
                "details": str(e),
            })
            continue

        try:
            close_response = close_position(symbol, cancel_orders=True)
        except Exception as e:
            print(f"Paper position close failed for {symbol}: {e}", flush=True)
            skipped_count += 1
            results.append({
                "symbol": symbol,
                "closed": False,
                "reason": "close_exception",
                "details": str(e),
            })
            continue

        close_order_id = str(close_response.get("id", "")).strip()
        close_order_status = str(close_response.get("status", "")).strip()
        close_filled_avg_price = ""
        close_filled_qty = qty

        if close_order_id:
            try:
                close_order = get_order_by_id(close_order_id, nested=False)
                close_order_status = str(close_order.get("status", close_order_status)).strip()
                close_filled_qty = str(close_order.get("filled_qty", close_filled_qty)).strip()

                close_filled_avg_price_raw = close_order.get("filled_avg_price", "")
                if close_filled_avg_price_raw not in (None, ""):
                    close_filled_avg_price = str(close_filled_avg_price_raw).strip()
            except Exception as order_read_error:
                print(f"Paper close order read failed for {symbol}: {order_read_error}", flush=True)

        timestamp_utc = datetime.now(timezone.utc).isoformat()
        safe_insert_broker_order(
            order_id=close_order_id or f"close-request-{symbol}-{timestamp_utc}",
            symbol=symbol,
            side=side,
            order_type="eod_close_request",
            status=close_order_status,
            qty=to_float_or_none(qty),
            filled_qty=to_float_or_none(close_filled_qty),
            avg_fill_price=to_float_or_none(close_filled_avg_price),
            submitted_at=parse_iso_utc(timestamp_utc),
            filled_at=parse_iso_utc(timestamp_utc) if close_filled_avg_price else None,
        )
        open_row = open_paper_rows_by_symbol.get(symbol)
        if open_row:
            entry_timestamp_utc = str(open_row.get("timestamp_utc", "")).strip()
            entry_timestamp = parse_iso_utc(entry_timestamp_utc) if entry_timestamp_utc else None
            event_timestamp = parse_iso_utc(timestamp_utc)
            entry_price = open_row.get("entry_price", "")
            stop_price = open_row.get("stop_price", "")
            target_price = open_row.get("target_price", "")
            shares_value = open_row.get("shares", qty)
            exit_price = close_filled_avg_price if close_filled_avg_price else current_price
            exit_reason = "EOD_CLOSE"
            mode = str(open_row.get("mode", "")).strip()
            trade_source = str(open_row.get("trade_source", "ALPACA_PAPER")).strip().upper() or "ALPACA_PAPER"
            broker_order_id = str(open_row.get("broker_order_id", "")).strip()
            broker_parent_order_id = str(open_row.get("broker_parent_order_id", "")).strip()
            linked_signal_timestamp_utc = str(open_row.get("linked_signal_timestamp_utc", "")).strip()
            linked_signal_entry = open_row.get("linked_signal_entry", "")
            linked_signal_stop = open_row.get("linked_signal_stop", "")
            linked_signal_target = open_row.get("linked_signal_target", "")
            linked_signal_confidence = open_row.get("linked_signal_confidence", "")
            direction = _infer_direction(entry_price, exit_price, stop_price, target_price)
            realized_pnl = _compute_realized_pnl(entry_price, exit_price, shares_value, direction)
            realized_pnl_percent = _compute_realized_pnl_percent(entry_price, exit_price, direction)
            duration_minutes = _compute_duration_minutes(entry_timestamp, event_timestamp)
            trade_key = _normalize_trade_key(symbol, broker_parent_order_id, broker_order_id)

            append_trade_log({
                "timestamp_utc": timestamp_utc,
                "event_type": "EOD_CLOSE",
                "symbol": symbol,
                "name": open_row.get("name", symbol),
                "mode": mode,
                "trade_source": trade_source,
                "broker": "ALPACA",
                "broker_order_id": broker_order_id,
                "broker_parent_order_id": broker_parent_order_id,
                "broker_status": close_order_status,
                "broker_filled_qty": close_filled_qty,
                "broker_filled_avg_price": close_filled_avg_price,
                "broker_exit_order_id": close_order_id,
                "shares": shares_value,
                "entry_price": "",
                "stop_price": stop_price,
                "target_price": target_price,
                "exit_price": exit_price,
                "exit_reason": exit_reason,
                "status": "CLOSED",
                "notes": "Paper position closed by EOD close flow.",
                "linked_signal_timestamp_utc": linked_signal_timestamp_utc,
                "linked_signal_entry": linked_signal_entry,
                "linked_signal_stop": linked_signal_stop,
                "linked_signal_target": linked_signal_target,
                "linked_signal_confidence": linked_signal_confidence,
                "inferred_stop_hit": "",
                "inferred_target_hit": "",
                "inferred_first_level_hit": "",
                "inferred_analysis_start_utc": entry_timestamp_utc,
                "inferred_analysis_end_utc": timestamp_utc,
            })

            safe_insert_trade_event(
                event_time=event_timestamp,
                event_type="EOD_CLOSE",
                symbol=symbol,
                side=_to_upper_or_none(side),
                shares=to_float_or_none(shares_value),
                price=to_float_or_none(exit_price),
                mode=mode,
                order_id=close_order_id,
                parent_order_id=broker_parent_order_id,
                status="CLOSED",
            )

            upsert_trade_lifecycle(
                trade_key=trade_key,
                symbol=symbol,
                mode=mode,
                side=_to_upper_or_none(side),
                direction=direction,
                status="CLOSED",
                entry_time=entry_timestamp,
                entry_price=to_float_or_none(entry_price),
                exit_time=event_timestamp,
                exit_price=to_float_or_none(exit_price),
                stop_price=to_float_or_none(stop_price),
                target_price=to_float_or_none(target_price),
                exit_reason=exit_reason,
                shares=to_float_or_none(shares_value),
                realized_pnl=realized_pnl,
                realized_pnl_percent=realized_pnl_percent,
                duration_minutes=duration_minutes,
                signal_timestamp=parse_iso_utc(linked_signal_timestamp_utc) if linked_signal_timestamp_utc else None,
                signal_entry=to_float_or_none(linked_signal_entry),
                signal_stop=to_float_or_none(linked_signal_stop),
                signal_target=to_float_or_none(linked_signal_target),
                signal_confidence=to_float_or_none(linked_signal_confidence),
                order_id=broker_order_id,
                parent_order_id=broker_parent_order_id,
                exit_order_id=close_order_id,
            )
        closed_count += 1
        results.append({
            "symbol": symbol,
            "closed": True,
            "qty": qty,
            "side": side,
            "exit_price": close_filled_avg_price if close_filled_avg_price else current_price,
            "close_order_id": close_order_id,
            "close_status": close_order_status,
            "close_filled_qty": close_filled_qty,
            "close_filled_avg_price": close_filled_avg_price,
            "canceled_order_count": len(canceled_order_ids),
        })

    return {
        "position_count": len(positions),
        "closed_count": closed_count,
        "skipped_count": skipped_count,
        "results": results,
    }
