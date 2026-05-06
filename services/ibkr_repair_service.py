from __future__ import annotations

from datetime import datetime
import time
from typing import Any, Callable

from core.trade_math import compute_duration_minutes, compute_realized_pnl, compute_realized_pnl_percent

_TERMINAL_UNVERIFIED_EXIT_REASON = "BROKER_CLOSE_UNVERIFIED_NO_FILL_DATA"
_TIME_STOP_PENDING_EXIT_REASON = "TIME_STOP_CLOSE_REQUESTED_PENDING_FILL_SYNC"
_UNVERIFIED_SOURCE_REASONS = {
    "",
    "MANUAL_CLOSE",
    "STALE_OPEN_RECONCILED",
    "BROKER_POSITION_FLAT_PENDING_FILL_SYNC",
    _TIME_STOP_PENDING_EXIT_REASON,
}


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_reason(value: Any) -> str:
    return _normalize_text(value).upper()


def _exit_reason_after_confirmed_fill(row: dict[str, Any]) -> str:
    if _normalize_reason(row.get("exit_reason")) == _TIME_STOP_PENDING_EXIT_REASON:
        return "TIME_STOP"
    return "BROKER_FILLED_EXIT_REPAIRED"


def _build_lifecycle_repair_payload(
    *,
    row: dict[str, Any],
    exit_order_id: str,
    exit_price: float,
    exit_time: datetime,
    exit_reason: str,
    exit_status: str,
    to_float_or_none: Callable[[Any], float | None],
) -> dict[str, Any]:
    direction = str(row.get("direction", "") or "").strip().upper()
    side = str(row.get("side", "") or "").strip().upper()
    shares = row.get("shares")
    entry_price = row.get("entry_price")
    realized_pnl = compute_realized_pnl(entry_price, exit_price, shares, direction)
    realized_pnl_percent = compute_realized_pnl_percent(entry_price, exit_price, direction)
    duration_minutes = compute_duration_minutes(row.get("entry_time"), exit_time)

    return {
        "trade_key": str(row.get("trade_key", "") or exit_order_id),
        "symbol": str(row.get("symbol", "") or "").strip().upper(),
        "mode": str(row.get("mode", "") or ""),
        "side": side,
        "direction": direction,
        "status": "CLOSED",
        "entry_time": row.get("entry_time"),
        "entry_price": to_float_or_none(entry_price),
        "exit_time": exit_time,
        "exit_price": exit_price,
        "stop_price": to_float_or_none(row.get("stop_price")),
        "target_price": to_float_or_none(row.get("target_price")),
        "exit_reason": exit_reason,
        "shares": to_float_or_none(shares),
        "realized_pnl": realized_pnl,
        "realized_pnl_percent": realized_pnl_percent,
        "duration_minutes": duration_minutes,
        "signal_timestamp": row.get("signal_timestamp"),
        "signal_entry": to_float_or_none(row.get("signal_entry")),
        "signal_stop": to_float_or_none(row.get("signal_stop")),
        "signal_target": to_float_or_none(row.get("signal_target")),
        "signal_confidence": to_float_or_none(row.get("signal_confidence")),
        "broker": "IBKR",
        "order_id": str(row.get("order_id", "") or row.get("parent_order_id", "") or exit_order_id),
        "parent_order_id": str(row.get("parent_order_id", "") or row.get("order_id", "") or exit_order_id),
        "exit_order_id": exit_order_id,
        "broker_order_status": exit_status,
    }


def _repair_payload_from_trade_event(
    *,
    row: dict[str, Any],
    get_latest_exit_trade_event_for_parent_order_id: Callable[[str, str | None, str | None], dict[str, Any] | None] | None,
    parse_iso_utc: Callable[[str], Any],
    to_float_or_none: Callable[[Any], float | None],
) -> dict[str, Any] | None:
    if get_latest_exit_trade_event_for_parent_order_id is None:
        return None

    parent_order_id = _normalize_text(row.get("parent_order_id") or row.get("order_id"))
    if not parent_order_id:
        return None

    symbol = _normalize_text(row.get("symbol")).upper()
    try:
        event = get_latest_exit_trade_event_for_parent_order_id(parent_order_id, "IBKR", symbol) or {}
    except TypeError:
        # Backward compatibility for call sites/tests still passing the older 2-arg callback.
        event = get_latest_exit_trade_event_for_parent_order_id(parent_order_id, "IBKR") or {}
    if not event:
        return None

    exit_price = to_float_or_none(event.get("price"))
    event_time = event.get("event_time")
    if exit_price is None or event_time is None:
        return None

    exit_time = event_time
    if not isinstance(exit_time, datetime):
        exit_time_raw = str(event_time or "").strip()
        if not exit_time_raw:
            return None
        exit_time = parse_iso_utc(exit_time_raw)

    event_type = str(event.get("event_type", "") or "").strip().upper()
    entry_price = to_float_or_none(row.get("entry_price"))
    resolved_exit_reason = event_type or "BROKER_EXIT_EVENT_REPAIRED"
    if resolved_exit_reason == "MANUAL_CLOSE" and entry_price is not None and exit_price == entry_price:
        resolved_exit_reason = _TERMINAL_UNVERIFIED_EXIT_REASON

    return _build_lifecycle_repair_payload(
        row=row,
        exit_order_id=_normalize_text(event.get("order_id") or row.get("exit_order_id") or parent_order_id),
        exit_price=exit_price,
        exit_time=exit_time,
        exit_reason=resolved_exit_reason,
        exit_status=str(event.get("status", "") or "Filled"),
        to_float_or_none=to_float_or_none,
    )


def _repair_payload_from_existing_lifecycle(
    *,
    row: dict[str, Any],
    parse_iso_utc: Callable[[str], Any],
    to_float_or_none: Callable[[Any], float | None],
) -> dict[str, Any] | None:
    exit_time = row.get("exit_time")
    exit_price = to_float_or_none(row.get("exit_price"))
    entry_price = to_float_or_none(row.get("entry_price"))

    if exit_price is None or exit_time in (None, ""):
        return None
    if entry_price is None:
        return None
    if exit_price == entry_price:
        return None

    parsed_exit_time = exit_time
    if not isinstance(parsed_exit_time, datetime):
        parsed_exit_time_raw = str(exit_time or "").strip()
        if not parsed_exit_time_raw:
            return None
        parsed_exit_time = parse_iso_utc(parsed_exit_time_raw)

    existing_exit_reason = _normalize_text(row.get("exit_reason"))
    if existing_exit_reason.upper() == _TIME_STOP_PENDING_EXIT_REASON:
        existing_exit_reason = "TIME_STOP"
    elif existing_exit_reason.upper() in {
        "STALE_OPEN_RECONCILED",
        "BROKER_POSITION_FLAT_PENDING_FILL_SYNC",
    }:
        existing_exit_reason = "BROKER_FILLED_EXIT_REPAIRED"

    return _build_lifecycle_repair_payload(
        row=row,
        exit_order_id=_normalize_text(row.get("exit_order_id") or row.get("parent_order_id") or row.get("order_id")),
        exit_price=exit_price,
        exit_time=parsed_exit_time,
        exit_reason=existing_exit_reason or "BROKER_FILLED_EXIT_REPAIRED",
        exit_status="reconciled_closed",
        to_float_or_none=to_float_or_none,
    )


def _terminal_unverified_payload_from_existing_lifecycle(
    *,
    row: dict[str, Any],
    parse_iso_utc: Callable[[str], Any],
    to_float_or_none: Callable[[Any], float | None],
) -> dict[str, Any] | None:
    exit_time = row.get("exit_time")
    exit_price = to_float_or_none(row.get("exit_price"))
    entry_price = to_float_or_none(row.get("entry_price"))

    if exit_price is None or exit_time in (None, ""):
        return None
    if entry_price is None:
        return None
    if exit_price != entry_price:
        return None

    existing_exit_reason = _normalize_reason(row.get("exit_reason"))
    if existing_exit_reason not in _UNVERIFIED_SOURCE_REASONS:
        return None

    parsed_exit_time = exit_time
    if not isinstance(parsed_exit_time, datetime):
        parsed_exit_time_raw = str(exit_time or "").strip()
        if not parsed_exit_time_raw:
            return None
        parsed_exit_time = parse_iso_utc(parsed_exit_time_raw)

    return _build_lifecycle_repair_payload(
        row=row,
        exit_order_id=_normalize_text(row.get("exit_order_id") or row.get("parent_order_id") or row.get("order_id")),
        exit_price=exit_price,
        exit_time=parsed_exit_time,
        exit_reason=_TERMINAL_UNVERIFIED_EXIT_REASON,
        exit_status="unverified_closed",
        to_float_or_none=to_float_or_none,
    )


def _repair_sync_order_ids(row: dict[str, Any], parent_order_id: str) -> list[str]:
    candidates = [
        row.get("exit_order_id"),
        parent_order_id,
        row.get("order_id"),
    ]
    normalized: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        order_id = _normalize_text(candidate)
        if not order_id or order_id in seen:
            continue
        seen.add(order_id)
        normalized.append(order_id)
    return normalized


def _direct_exit_sync_result(
    *,
    row: dict[str, Any],
    sync_result: dict[str, Any],
    synced_order_id: str,
    parent_order_id: str,
    to_float_or_none: Callable[[Any], float | None],
) -> dict[str, Any] | None:
    exit_order_id = _normalize_text(row.get("exit_order_id"))
    if not exit_order_id or synced_order_id != exit_order_id:
        return None
    if exit_order_id in {_normalize_text(parent_order_id), _normalize_text(row.get("order_id"))}:
        return None
    if _normalize_text(sync_result.get("status")).lower() == "unknown":
        return None

    exit_price = (
        to_float_or_none(sync_result.get("exit_filled_avg_price"))
        or to_float_or_none(sync_result.get("exit_price"))
        or to_float_or_none(sync_result.get("filled_avg_price"))
        or to_float_or_none(sync_result.get("avg_fill_price"))
        or to_float_or_none(sync_result.get("entry_filled_avg_price"))
    )
    if exit_price is None:
        return None

    exit_time = (
        _normalize_text(sync_result.get("exit_filled_at"))
        or _normalize_text(sync_result.get("exit_time"))
        or _normalize_text(sync_result.get("filled_at"))
        or _normalize_text(sync_result.get("entry_filled_at"))
        or _normalize_text(row.get("exit_time"))
        or _normalize_text(row.get("updated_at"))
    )
    if not exit_time:
        return None

    filled_qty = (
        sync_result.get("exit_filled_qty")
        or sync_result.get("filled_qty")
        or sync_result.get("entry_filled_qty")
        or row.get("shares")
    )

    return {
        **sync_result,
        "exit_order_id": exit_order_id,
        "exit_price": exit_price,
        "exit_filled_avg_price": exit_price,
        "exit_filled_qty": filled_qty,
        "exit_filled_at": exit_time,
        "exit_status": sync_result.get("exit_status") or sync_result.get("status") or "Filled",
        "exit_reason": _exit_reason_after_confirmed_fill(row),
        "direct_exit_order_sync": True,
    }


def _repair_payload_from_sync_result(
    *,
    row: dict[str, Any],
    sync_result: dict[str, Any],
    synced_order_id: str,
    parent_order_id: str,
    parse_iso_utc: Callable[[str], Any],
    to_float_or_none: Callable[[Any], float | None],
) -> dict[str, Any] | None:
    normalized_sync_result = dict(sync_result or {})
    direct_exit_result = _direct_exit_sync_result(
        row=row,
        sync_result=normalized_sync_result,
        synced_order_id=synced_order_id,
        parent_order_id=parent_order_id,
        to_float_or_none=to_float_or_none,
    )
    if direct_exit_result is not None:
        normalized_sync_result = direct_exit_result

    exit_order_id = _normalize_text(normalized_sync_result.get("exit_order_id"))
    exit_price = to_float_or_none(normalized_sync_result.get("exit_price"))
    exit_time_raw = (
        _normalize_text(normalized_sync_result.get("exit_filled_at"))
        or _normalize_text(normalized_sync_result.get("exit_time"))
        or _normalize_text(normalized_sync_result.get("filled_at"))
    )
    if not exit_order_id or exit_price is None or not exit_time_raw:
        return None

    has_fill_evidence = (
        (to_float_or_none(normalized_sync_result.get("exit_filled_qty")) or 0) > 0
        or to_float_or_none(normalized_sync_result.get("exit_filled_avg_price")) is not None
        or bool(_normalize_text(normalized_sync_result.get("exit_filled_at")))
        or bool(normalized_sync_result.get("direct_exit_order_sync"))
    )
    raw_exit_reason = _normalize_text(normalized_sync_result.get("exit_reason"))
    normalized_exit_reason = raw_exit_reason.upper()
    if has_fill_evidence:
        resolved_exit_reason = _exit_reason_after_confirmed_fill(row)
    elif normalized_exit_reason in _UNVERIFIED_SOURCE_REASONS:
        resolved_exit_reason = _TERMINAL_UNVERIFIED_EXIT_REASON
    else:
        resolved_exit_reason = raw_exit_reason or _TERMINAL_UNVERIFIED_EXIT_REASON

    return _build_lifecycle_repair_payload(
        row=row,
        exit_order_id=exit_order_id,
        exit_price=exit_price,
        exit_time=parse_iso_utc(exit_time_raw),
        exit_reason=resolved_exit_reason,
        exit_status=_normalize_text(normalized_sync_result.get("exit_status")) or "Filled",
        to_float_or_none=to_float_or_none,
    )


def repair_ibkr_stale_closes(
    *,
    target_date: str,
    get_stale_ibkr_closed_trade_lifecycles: Callable[..., list[dict[str, Any]]],
    sync_order_by_id_for_broker: Callable[[str, str], dict[str, Any]],
    get_latest_exit_trade_event_for_parent_order_id: Callable[[str, str | None, str | None], dict[str, Any] | None] | None = None,
    upsert_trade_lifecycle: Callable[..., None],
    safe_insert_broker_order: Callable[..., None],
    parse_iso_utc: Callable[[str], Any],
    to_float_or_none: Callable[[Any], float | None],
    max_duration_seconds: float = 30.0,
    current_time_fn: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    stale_rows = get_stale_ibkr_closed_trade_lifecycles(target_date=target_date, limit=100)
    results: list[dict[str, Any]] = []
    repaired_count = 0
    skipped_count = 0
    started_at = current_time_fn()

    for row in stale_rows:
        if max_duration_seconds > 0 and (current_time_fn() - started_at) >= max_duration_seconds:
            skipped_count += 1
            results.append({
                "symbol": "",
                "repaired": False,
                "reason": "repair_time_budget_exceeded",
                "details": f"Stopped after {max_duration_seconds:.1f}s to avoid monopolizing the service",
            })
            break

        parent_order_id = _normalize_text(row.get("parent_order_id") or row.get("order_id"))
        symbol = _normalize_text(row.get("symbol")).upper()
        if not parent_order_id:
            skipped_count += 1
            results.append({
                "symbol": symbol,
                "repaired": False,
                "reason": "missing_parent_order_id",
            })
            continue

        repair_payload: dict[str, Any] | None = None
        sync_result: dict[str, Any] = {}
        sync_order_id = ""
        sync_attempted_order_ids: list[str] = []
        sync_exception: Exception | None = None
        invalid_exit_time_error: Exception | None = None
        invalid_exit_time_value = ""

        repair_payload = _repair_payload_from_trade_event(
            row=row,
            get_latest_exit_trade_event_for_parent_order_id=get_latest_exit_trade_event_for_parent_order_id,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
        )

        if repair_payload is None:
            repair_payload = _repair_payload_from_existing_lifecycle(
                row=row,
                parse_iso_utc=parse_iso_utc,
                to_float_or_none=to_float_or_none,
            )

        if repair_payload is None:
            for candidate_order_id in _repair_sync_order_ids(row, parent_order_id):
                sync_attempted_order_ids.append(candidate_order_id)
                try:
                    sync_result = sync_order_by_id_for_broker("IBKR", candidate_order_id) or {}
                    sync_order_id = candidate_order_id
                except Exception as exc:
                    sync_exception = exc
                    continue

                try:
                    repair_payload = _repair_payload_from_sync_result(
                        row=row,
                        sync_result=sync_result,
                        synced_order_id=candidate_order_id,
                        parent_order_id=parent_order_id,
                        parse_iso_utc=parse_iso_utc,
                        to_float_or_none=to_float_or_none,
                    )
                except Exception as exc:
                    invalid_exit_time_error = exc
                    invalid_exit_time_value = (
                        _normalize_text(sync_result.get("exit_filled_at"))
                        or _normalize_text(sync_result.get("exit_time"))
                        or _normalize_text(sync_result.get("filled_at"))
                        or _normalize_text(sync_result.get("entry_filled_at"))
                    )
                    repair_payload = None
                    continue

                if repair_payload is not None:
                    break

            if repair_payload is None and sync_exception is not None and not sync_result:
                skipped_count += 1
                results.append({
                    "symbol": symbol,
                    "parent_order_id": parent_order_id,
                    "repaired": False,
                    "reason": "sync_exception",
                    "details": str(sync_exception),
                    "attempted_order_ids": sync_attempted_order_ids,
                })
                continue

            if repair_payload is None and invalid_exit_time_error is not None:
                skipped_count += 1
                results.append({
                    "symbol": symbol,
                    "parent_order_id": parent_order_id,
                    "repaired": False,
                    "reason": "invalid_exit_time",
                    "details": str(invalid_exit_time_error),
                    "exit_time": invalid_exit_time_value,
                    "attempted_order_ids": sync_attempted_order_ids,
                })
                continue

        if repair_payload is None:
            repair_payload = _terminal_unverified_payload_from_existing_lifecycle(
                row=row,
                parse_iso_utc=parse_iso_utc,
                to_float_or_none=to_float_or_none,
            )

        if repair_payload is None:
            skipped_count += 1
            results.append({
                "symbol": symbol,
                "parent_order_id": parent_order_id,
                "repaired": False,
                "reason": "repair_data_unavailable",
                "exit_order_id": sync_result.get("exit_order_id", ""),
                "exit_price": sync_result.get("exit_price"),
                "synced_order_id": sync_order_id,
                "attempted_order_ids": sync_attempted_order_ids,
            })
            continue

        upsert_trade_lifecycle(
            trade_key=repair_payload["trade_key"],
            symbol=repair_payload["symbol"],
            mode=repair_payload["mode"],
            side=repair_payload["side"],
            direction=repair_payload["direction"],
            status=repair_payload["status"],
            entry_time=repair_payload["entry_time"],
            entry_price=repair_payload["entry_price"],
            exit_time=repair_payload["exit_time"],
            exit_price=repair_payload["exit_price"],
            stop_price=repair_payload["stop_price"],
            target_price=repair_payload["target_price"],
            exit_reason=repair_payload["exit_reason"],
            shares=repair_payload["shares"],
            realized_pnl=repair_payload["realized_pnl"],
            realized_pnl_percent=repair_payload["realized_pnl_percent"],
            duration_minutes=repair_payload["duration_minutes"],
            signal_timestamp=repair_payload["signal_timestamp"],
            signal_entry=repair_payload["signal_entry"],
            signal_stop=repair_payload["signal_stop"],
            signal_target=repair_payload["signal_target"],
            signal_confidence=repair_payload["signal_confidence"],
            broker="IBKR",
            order_id=repair_payload["order_id"],
            parent_order_id=repair_payload["parent_order_id"],
            exit_order_id=repair_payload["exit_order_id"],
        )
        if repair_payload["exit_reason"] != _TERMINAL_UNVERIFIED_EXIT_REASON:
            safe_insert_broker_order(
                order_id=repair_payload["exit_order_id"],
                broker="IBKR",
                symbol=repair_payload["symbol"],
                side="SELL" if repair_payload["side"] == "BUY" else "BUY",
                order_type="exit",
                status=repair_payload["broker_order_status"],
                qty=repair_payload["shares"],
                filled_qty=to_float_or_none(sync_result.get("exit_filled_qty")) or repair_payload["shares"],
                avg_fill_price=to_float_or_none(sync_result.get("exit_filled_avg_price")) or repair_payload["exit_price"],
                submitted_at=repair_payload["exit_time"],
                filled_at=repair_payload["exit_time"],
            )

        repaired_count += 1
        results.append({
            "symbol": repair_payload["symbol"],
            "parent_order_id": parent_order_id,
            "repaired": True,
            "exit_order_id": repair_payload["exit_order_id"],
            "exit_price": repair_payload["exit_price"],
            "exit_reason": repair_payload["exit_reason"],
            "synced_order_id": sync_order_id,
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
