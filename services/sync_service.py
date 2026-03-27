from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable



def execute_sync_paper_trades(
    *,
    get_open_paper_trades: Callable[[], list[dict[str, Any]]],
    sync_order_by_id: Callable[[str], dict[str, Any]],
    paper_trade_exit_already_logged: Callable[[str, str], bool],
    append_trade_log: Callable[[dict[str, Any]], None],
    safe_insert_trade_event: Callable[..., None],
    safe_insert_broker_order: Callable[..., None],
    parse_iso_utc: Callable[[str], Any],
    to_float_or_none: Callable[[Any], float | None],
) -> dict[str, Any] | tuple[dict[str, Any], int]:
    try:
        open_rows = get_open_paper_trades()
    except Exception as e:
        print(f"Open paper trade read failed: {e}", flush=True)
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
            print(f"Paper trade sync failed for {symbol} / {parent_order_id}: {e}", flush=True)
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
            append_trade_log({
                "timestamp_utc": timestamp_utc,
                "event_type": exit_event,
                "symbol": symbol,
                "name": open_row.get("name", ""),
                "mode": open_row.get("mode", ""),
                "trade_source": "ALPACA_PAPER",
                "broker": "ALPACA",
                "broker_order_id": parent_order_id,
                "broker_parent_order_id": parent_order_id,
                "broker_status": sync_result.get("parent_status", ""),
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
                event_time=parse_iso_utc(timestamp_utc),
                event_type=exit_event,
                symbol=symbol,
                side=None,
                shares=to_float_or_none(open_row.get("shares", "")),
                price=to_float_or_none(sync_result.get("exit_price", "")),
                mode=str(open_row.get("mode", "") or ""),
                order_id=str(sync_result.get("exit_order_id", "") or parent_order_id),
                parent_order_id=parent_order_id,
                status="CLOSED",
            )
            safe_insert_broker_order(
                order_id=str(sync_result.get("exit_order_id", "") or parent_order_id),
                symbol=symbol,
                side=None,
                order_type="exit",
                status=str(sync_result.get("parent_status", "") or ""),
                qty=to_float_or_none(open_row.get("shares", "")),
                filled_qty=to_float_or_none(sync_result.get("exit_filled_qty", "")),
                avg_fill_price=to_float_or_none(sync_result.get("exit_filled_avg_price", "")),
                submitted_at=parse_iso_utc(timestamp_utc),
                filled_at=parse_iso_utc(timestamp_utc),
            )
        except Exception as e:
            print(f"Paper trade exit log write failed for {symbol} / {parent_order_id}: {e}", flush=True)
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
        })

    return {
        "ok": True,
        "open_paper_trade_count": len(open_rows),
        "synced_count": synced_count,
        "skipped_count": skipped_count,
        "results": results,
    }
