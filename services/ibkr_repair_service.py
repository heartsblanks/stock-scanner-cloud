from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from core.trade_math import compute_duration_minutes, compute_realized_pnl, compute_realized_pnl_percent


def repair_ibkr_stale_closes(
    *,
    target_date: str,
    get_stale_ibkr_closed_trade_lifecycles: Callable[..., list[dict[str, Any]]],
    sync_order_by_id_for_broker: Callable[[str, str], dict[str, Any]],
    upsert_trade_lifecycle: Callable[..., None],
    safe_insert_broker_order: Callable[..., None],
    parse_iso_utc: Callable[[str], Any],
    to_float_or_none: Callable[[Any], float | None],
) -> dict[str, Any]:
    stale_rows = get_stale_ibkr_closed_trade_lifecycles(target_date=target_date, limit=100)
    results: list[dict[str, Any]] = []
    repaired_count = 0
    skipped_count = 0

    for row in stale_rows:
        parent_order_id = str(row.get("parent_order_id", "") or row.get("order_id", "")).strip()
        symbol = str(row.get("symbol", "")).strip().upper()
        if not parent_order_id:
            skipped_count += 1
            results.append({
                "symbol": symbol,
                "repaired": False,
                "reason": "missing_parent_order_id",
            })
            continue

        try:
            sync_result = sync_order_by_id_for_broker("IBKR", parent_order_id) or {}
        except Exception as exc:
            skipped_count += 1
            results.append({
                "symbol": symbol,
                "parent_order_id": parent_order_id,
                "repaired": False,
                "reason": "sync_exception",
                "details": str(exc),
            })
            continue

        exit_order_id = str(sync_result.get("exit_order_id", "") or "").strip()
        exit_price = to_float_or_none(sync_result.get("exit_price"))
        exit_time_raw = str(sync_result.get("exit_filled_at", "") or sync_result.get("exit_time", "") or "").strip()
        if not exit_order_id or exit_price is None or not exit_time_raw:
            skipped_count += 1
            results.append({
                "symbol": symbol,
                "parent_order_id": parent_order_id,
                "repaired": False,
                "reason": "repair_data_unavailable",
                "exit_order_id": exit_order_id,
                "exit_price": sync_result.get("exit_price"),
            })
            continue

        try:
            exit_time = parse_iso_utc(exit_time_raw)
        except Exception as exc:
            skipped_count += 1
            results.append({
                "symbol": symbol,
                "parent_order_id": parent_order_id,
                "repaired": False,
                "reason": "invalid_exit_time",
                "details": str(exc),
                "exit_time": exit_time_raw,
            })
            continue

        direction = str(row.get("direction", "") or "").strip().upper()
        side = str(row.get("side", "") or "").strip().upper()
        shares = row.get("shares")
        entry_price = row.get("entry_price")
        exit_reason = str(sync_result.get("exit_reason", "") or "BROKER_FILLED_EXIT_REPAIRED").strip() or "BROKER_FILLED_EXIT_REPAIRED"
        realized_pnl = compute_realized_pnl(entry_price, exit_price, shares, direction)
        realized_pnl_percent = compute_realized_pnl_percent(entry_price, exit_price, direction)
        duration_minutes = compute_duration_minutes(row.get("entry_time"), exit_time)

        upsert_trade_lifecycle(
            trade_key=str(row.get("trade_key", "") or parent_order_id),
            symbol=symbol,
            mode=str(row.get("mode", "") or ""),
            side=side,
            direction=direction,
            status="CLOSED",
            entry_time=row.get("entry_time"),
            entry_price=to_float_or_none(entry_price),
            exit_time=exit_time,
            exit_price=exit_price,
            stop_price=to_float_or_none(row.get("stop_price")),
            target_price=to_float_or_none(row.get("target_price")),
            exit_reason=exit_reason,
            shares=to_float_or_none(shares),
            realized_pnl=realized_pnl,
            realized_pnl_percent=realized_pnl_percent,
            duration_minutes=duration_minutes,
            signal_timestamp=row.get("signal_timestamp"),
            signal_entry=to_float_or_none(row.get("signal_entry")),
            signal_stop=to_float_or_none(row.get("signal_stop")),
            signal_target=to_float_or_none(row.get("signal_target")),
            signal_confidence=to_float_or_none(row.get("signal_confidence")),
            broker="IBKR",
            order_id=str(row.get("order_id", "") or parent_order_id),
            parent_order_id=parent_order_id,
            exit_order_id=exit_order_id,
        )
        safe_insert_broker_order(
            order_id=exit_order_id,
            broker="IBKR",
            symbol=symbol,
            side="SELL" if side == "BUY" else "BUY",
            order_type="exit",
            status=str(sync_result.get("exit_status", "Filled") or "Filled"),
            qty=to_float_or_none(shares),
            filled_qty=to_float_or_none(sync_result.get("exit_filled_qty")) or to_float_or_none(shares),
            avg_fill_price=to_float_or_none(sync_result.get("exit_filled_avg_price")) or exit_price,
            submitted_at=exit_time,
            filled_at=exit_time,
        )

        repaired_count += 1
        results.append({
            "symbol": symbol,
            "parent_order_id": parent_order_id,
            "repaired": True,
            "exit_order_id": exit_order_id,
            "exit_price": exit_price,
            "exit_reason": exit_reason,
        })

    return {
        "ok": True,
        "target_date": target_date,
        "stale_row_count": len(stale_rows),
        "repaired_count": repaired_count,
        "skipped_count": skipped_count,
        "results": results,
        "noop": len(stale_rows) == 0,
    }
