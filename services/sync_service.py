from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable
from core.logging_utils import log_exception
from core.trade_math import (
    compute_duration_minutes,
    compute_realized_pnl,
    compute_realized_pnl_percent,
    infer_direction,
    normalize_trade_key,
    resolve_lifecycle_side,
)


def _resolve_exit_timestamp(sync_result: dict[str, Any], timestamp_utc: str, parse_iso_utc: Callable[[str], Any]):
    for key in ("exit_filled_at", "exit_time", "filled_at", "updated_at"):
        raw_value = str(sync_result.get(key, "") or "").strip()
        if raw_value:
            try:
                return parse_iso_utc(raw_value)
            except Exception:
                pass
    return parse_iso_utc(timestamp_utc)



def execute_sync_paper_trades(
    *,
    get_open_paper_trades: Callable[[], list[dict[str, Any]]],
    sync_order_by_id: Callable[[str], dict[str, Any]],
    paper_trade_exit_already_logged: Callable[[str, str], bool],
    append_trade_log: Callable[[dict[str, Any]], None],
    safe_insert_trade_event: Callable[..., None],
    safe_insert_broker_order: Callable[..., None],
    upsert_trade_lifecycle: Callable[..., None],
    parse_iso_utc: Callable[[str], Any],
    to_float_or_none: Callable[[Any], float | None],
    get_open_positions: Callable[[], list[dict[str, Any]]] | None = None,
    close_position: Callable[[str], Any] | None = None,
) -> dict[str, Any] | tuple[dict[str, Any], int]:
    try:
        open_rows = get_open_paper_trades()
    except Exception as e:
        log_exception("Open paper trade read failed", e, component="sync_service", operation="execute_sync_paper_trades")
        return {"ok": False, "error": f"open paper trade read failed: {e}"}, 500

    results: list[dict[str, Any]] = []
    synced_count = 0
    skipped_count = 0

    for open_row in open_rows:
        parent_order_id = str(open_row.get("broker_parent_order_id", "")).strip()
        symbol = str(open_row.get("symbol", "")).strip().upper()

        if not parent_order_id:
            results.append({
                "symbol": symbol,
                "synced": False,
                "reason": "missing_parent_order_id",
            })
            skipped_count += 1
            continue

        try:
            sync_result = sync_order_by_id(parent_order_id)
        except Exception as e:
            log_exception(
                "Paper trade sync failed",
                e,
                component="sync_service",
                operation="execute_sync_paper_trades",
                symbol=symbol,
                parent_order_id=parent_order_id,
            )
            results.append({
                "symbol": symbol,
                "parent_order_id": parent_order_id,
                "synced": False,
                "reason": "sync_exception",
                "details": str(e),
            })
            skipped_count += 1
            continue

        exit_event = str(sync_result.get("exit_event", "")).strip().upper()
        if not exit_event:
            results.append({
                "symbol": symbol,
                "parent_order_id": parent_order_id,
                "synced": False,
                "reason": "still_open",
                "parent_status": sync_result.get("parent_status", ""),
                "take_profit_status": sync_result.get("take_profit_status", ""),
                "stop_loss_status": sync_result.get("stop_loss_status", ""),
            })
            skipped_count += 1
            continue

        if paper_trade_exit_already_logged(parent_order_id, exit_event):
            results.append({
                "symbol": symbol,
                "parent_order_id": parent_order_id,
                "synced": False,
                "reason": "exit_already_logged",
                "exit_event": exit_event,
            })
            skipped_count += 1
            continue

        timestamp_utc = datetime.now(timezone.utc).isoformat()

        try:
            exit_timestamp = _resolve_exit_timestamp(sync_result, timestamp_utc, parse_iso_utc)
            exit_timestamp_utc = exit_timestamp.astimezone(timezone.utc).isoformat() if exit_timestamp else timestamp_utc

            entry_price = open_row.get("entry_price", "")
            stop_price = open_row.get("stop_price", "")
            target_price = open_row.get("target_price", "")
            exit_price = sync_result.get("exit_price", "")
            direction = infer_direction(entry_price, exit_price, stop_price, target_price, open_row.get("side", ""))
            lifecycle_side = resolve_lifecycle_side(open_row, direction)
            broker_name = str(open_row.get("broker", "") or "ALPACA").strip().upper() or "ALPACA"
            trade_source = str(open_row.get("trade_source", f"{broker_name}_PAPER")).strip().upper() or f"{broker_name}_PAPER"

            append_trade_log({
                "timestamp_utc": exit_timestamp_utc,
                "event_type": exit_event,
                "symbol": symbol,
                "name": open_row.get("name", ""),
                "mode": open_row.get("mode", ""),
                "trade_source": trade_source,
                "broker": broker_name,
                "broker_order_id": parent_order_id,
                "broker_parent_order_id": parent_order_id,
                "broker_status": sync_result.get("exit_status", sync_result.get("parent_status", "")),
                "broker_filled_qty": sync_result.get("exit_filled_qty", ""),
                "broker_filled_avg_price": sync_result.get("exit_filled_avg_price", ""),
                "broker_exit_order_id": sync_result.get("exit_order_id", ""),
                "shares": open_row.get("shares", ""),
                "entry_price": open_row.get("entry_price", ""),
                "stop_price": open_row.get("stop_price", ""),
                "target_price": open_row.get("target_price", ""),
                "exit_price": sync_result.get("exit_price", ""),
                "exit_reason": sync_result.get("exit_reason", exit_event),
                "status": "CLOSED",
                "notes": f"Paper trade exit synced from Alpaca. exit_event={exit_event}",
                "linked_signal_timestamp_utc": open_row.get("linked_signal_timestamp_utc", ""),
                "linked_signal_entry": open_row.get("linked_signal_entry", ""),
                "linked_signal_stop": open_row.get("linked_signal_stop", ""),
                "linked_signal_target": open_row.get("linked_signal_target", ""),
                "linked_signal_confidence": open_row.get("linked_signal_confidence", ""),
                "inferred_stop_hit": "",
                "inferred_target_hit": "",
                "inferred_first_level_hit": "",
                "inferred_analysis_start_utc": "",
                "inferred_analysis_end_utc": "",
            })
            safe_insert_trade_event(
                event_time=exit_timestamp,
                event_type=exit_event,
                symbol=symbol,
                side=lifecycle_side,
                shares=to_float_or_none(open_row.get("shares", "")),
                price=to_float_or_none(sync_result.get("exit_price", "")),
                mode=str(open_row.get("mode", "") or ""),
                broker=broker_name,
                order_id=str(sync_result.get("exit_order_id", "") or parent_order_id),
                parent_order_id=parent_order_id,
                status="CLOSED",
            )
            safe_insert_broker_order(
                order_id=str(sync_result.get("exit_order_id", "") or parent_order_id),
                broker=broker_name,
                symbol=symbol,
                side=lifecycle_side,
                order_type="exit",
                status=str(sync_result.get("exit_status", sync_result.get("parent_status", "")) or ""),
                qty=to_float_or_none(open_row.get("shares", "")),
                filled_qty=to_float_or_none(sync_result.get("exit_filled_qty", "")),
                avg_fill_price=to_float_or_none(sync_result.get("exit_filled_avg_price", "")),
                submitted_at=exit_timestamp,
                filled_at=exit_timestamp,
            )

            shares_value = open_row.get("shares", "")
            entry_timestamp_utc = str(open_row.get("timestamp_utc", "")).strip()
            entry_timestamp = parse_iso_utc(entry_timestamp_utc) if entry_timestamp_utc else None
            linked_signal_timestamp_utc = str(open_row.get("linked_signal_timestamp_utc", "")).strip()
            broker_order_id = str(open_row.get("broker_order_id", "") or parent_order_id)
            broker_parent_order_id = str(open_row.get("broker_parent_order_id", "") or parent_order_id)
            realized_pnl = compute_realized_pnl(entry_price, exit_price, shares_value, direction)
            realized_pnl_percent = compute_realized_pnl_percent(entry_price, exit_price, direction)
            duration_minutes = compute_duration_minutes(entry_timestamp, exit_timestamp)
            trade_key = normalize_trade_key(symbol, broker_parent_order_id, broker_order_id)

            upsert_trade_lifecycle(
                trade_key=trade_key,
                symbol=symbol,
                mode=str(open_row.get("mode", "") or ""),
                side=lifecycle_side,
                direction=direction,
                status="CLOSED",
                entry_time=entry_timestamp,
                entry_price=to_float_or_none(entry_price),
                exit_time=exit_timestamp,
                exit_price=to_float_or_none(exit_price),
                stop_price=to_float_or_none(stop_price),
                target_price=to_float_or_none(target_price),
                exit_reason=str(sync_result.get("exit_reason", "") or exit_event),
                shares=to_float_or_none(shares_value),
                realized_pnl=realized_pnl,
                realized_pnl_percent=realized_pnl_percent,
                duration_minutes=duration_minutes,
                signal_timestamp=parse_iso_utc(linked_signal_timestamp_utc) if linked_signal_timestamp_utc else None,
                signal_entry=to_float_or_none(open_row.get("linked_signal_entry", "")),
                signal_stop=to_float_or_none(open_row.get("linked_signal_stop", "")),
                signal_target=to_float_or_none(open_row.get("linked_signal_target", "")),
                signal_confidence=to_float_or_none(open_row.get("linked_signal_confidence", "")),
                broker=broker_name,
                order_id=broker_order_id,
                parent_order_id=broker_parent_order_id,
                exit_order_id=str(sync_result.get("exit_order_id", "") or parent_order_id),
            )
        except Exception as e:
            log_exception(
                "Paper trade exit log write failed",
                e,
                component="sync_service",
                operation="execute_sync_paper_trades",
                symbol=symbol,
                parent_order_id=parent_order_id,
            )
            results.append({
                "symbol": symbol,
                "parent_order_id": parent_order_id,
                "synced": False,
                "reason": "log_write_failed",
                "details": str(e),
            })
            skipped_count += 1
            continue

        synced_count += 1
        results.append({
            "symbol": symbol,
            "parent_order_id": parent_order_id,
            "synced": True,
            "exit_event": exit_event,
            "exit_price": sync_result.get("exit_price", ""),
            "exit_order_id": sync_result.get("exit_order_id", ""),
            "parent_status": sync_result.get("parent_status", ""),
            "exit_status": sync_result.get("exit_status", sync_result.get("parent_status", "")),
        })

    auto_healed_positions: list[dict[str, Any]] = []
    auto_heal_errors: list[dict[str, Any]] = []

    if get_open_positions and close_position:
        try:
            leftover_positions = get_open_positions() or []
        except Exception as e:
            leftover_positions = []
            auto_heal_errors.append({
                "symbol": "",
                "reason": "open_positions_read_failed",
                "details": str(e),
            })

        # Build DB open symbol set (source of truth)
        db_open_symbols = {
            str(row.get("symbol", "")).strip().upper()
            for row in open_rows or []
            if str(row.get("symbol", "")).strip()
        }

        for position in leftover_positions:
            try:
                symbol = str(position.get("symbol", "")).strip().upper()
                qty = position.get("qty")
                side = str(position.get("side", "")).strip().lower()
                if not symbol:
                    continue

                # Only auto-heal orphan positions (present in broker but not in DB open trades)
                if symbol in db_open_symbols:
                    continue

                close_result = close_position(symbol)
                auto_healed_positions.append({
                    "symbol": symbol,
                    "qty": qty,
                    "side": side,
                    "closed": True,
                    "result": close_result,
                })
            except Exception as e:
                auto_heal_errors.append({
                    "symbol": str(position.get("symbol", "")).strip().upper(),
                    "reason": "auto_heal_close_failed",
                    "details": str(e),
                })

    return {
        "ok": True,
        "open_paper_trade_count": len(open_rows),
        "synced_count": synced_count,
        "skipped_count": skipped_count,
        "results": results,
        "auto_healed_count": len(auto_healed_positions),
        "auto_healed_positions": auto_healed_positions,
        "auto_heal_error_count": len(auto_heal_errors),
        "auto_heal_errors": auto_heal_errors,
    }
