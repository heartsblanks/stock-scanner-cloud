from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any, Callable
from core.logging_utils import log_exception
from core.logging_utils import log_info
from core.logging_utils import log_warning
from core.trade_math import (
    compute_duration_minutes,
    compute_realized_pnl,
    compute_realized_pnl_percent,
    infer_direction,
    normalize_trade_key,
    resolve_lifecycle_side,
)


def _classify_ibkr_bridge_issue(exc: Exception, *, broker_name: str) -> tuple[str, str | None]:
    if str(broker_name or "").strip().upper() != "IBKR":
        return "sync_exception", None

    message = str(exc)
    lowered = message.lower()
    if "ibkr bridge timeout" in lowered:
        return "bridge_timeout", message
    if "ibkr bridge request failed" in lowered:
        return "bridge_request_failed", message
    return "sync_exception", None


def _resolve_exit_timestamp(sync_result: dict[str, Any], timestamp_utc: str, parse_iso_utc: Callable[[str], Any]):
    for key in ("exit_filled_at", "exit_time", "filled_at", "updated_at"):
        raw_value = str(sync_result.get(key, "") or "").strip()
        if raw_value:
            try:
                return parse_iso_utc(raw_value)
            except Exception:
                pass
    return parse_iso_utc(timestamp_utc)


def _sync_time_budget_seconds(*, broker_name: str) -> float | None:
    normalized = str(broker_name or "").strip().upper()
    env_name = "IBKR_SYNC_BATCH_TIME_BUDGET_SECONDS" if normalized == "IBKR" else "PAPER_SYNC_BATCH_TIME_BUDGET_SECONDS"
    raw_value = str(os.getenv(env_name, "")).strip()
    if not raw_value:
        return 90.0 if normalized == "IBKR" else None
    try:
        value = float(raw_value)
    except Exception:
        return 90.0 if normalized == "IBKR" else None
    if value <= 0:
        return None
    return value


def _normalize_ibkr_client_order_id(value: Any) -> str:
    return str(value or "").strip()


def _expected_ibkr_client_order_id(
    open_row: dict[str, Any],
    *,
    to_float_or_none: Callable[[Any], float | None],
) -> str:
    symbol = str(open_row.get("symbol", "") or "").strip().upper()
    if not symbol:
        return ""

    direction = str(open_row.get("direction", "") or "").strip().upper()
    if not direction:
        inferred_direction = infer_direction(
            open_row.get("entry_price", ""),
            "",
            open_row.get("stop_price", ""),
            open_row.get("target_price", ""),
            open_row.get("side", ""),
        )
        direction = str(inferred_direction or "").strip().upper()
    if direction not in {"LONG", "SHORT"}:
        return ""

    entry_price = to_float_or_none(open_row.get("entry_price", ""))
    shares = to_float_or_none(open_row.get("shares", ""))
    if entry_price is None or shares is None or shares <= 0:
        return ""

    try:
        entry_basis = int(round(entry_price * 10000))
        share_count = int(round(shares))
    except Exception:
        return ""
    return f"scanner-{symbol}-{direction}-{entry_basis}-{share_count}"


def _validate_ibkr_sync_identity(
    *,
    open_row: dict[str, Any],
    sync_result: dict[str, Any],
    to_float_or_none: Callable[[Any], float | None],
) -> tuple[bool, str | None]:
    open_symbol = str(open_row.get("symbol", "") or "").strip().upper()
    sync_symbol = str(sync_result.get("symbol", "") or "").strip().upper()
    if open_symbol and sync_symbol and open_symbol != sync_symbol:
        return False, f"symbol_mismatch:{open_symbol}->{sync_symbol}"

    actual_client_order_id = _normalize_ibkr_client_order_id(sync_result.get("client_order_id"))
    expected_client_order_id = _expected_ibkr_client_order_id(open_row, to_float_or_none=to_float_or_none)
    if actual_client_order_id and expected_client_order_id and actual_client_order_id != expected_client_order_id:
        return False, f"client_order_id_mismatch:{expected_client_order_id}->{actual_client_order_id}"

    stored_entry_price = to_float_or_none(open_row.get("entry_price", ""))
    synced_entry_price = to_float_or_none(
        sync_result.get("entry_filled_avg_price", "") or sync_result.get("entry_price", "")
    )
    if stored_entry_price is not None and synced_entry_price is not None and stored_entry_price > 0:
        allowed_delta = max(0.5, stored_entry_price * 0.03)
        if abs(stored_entry_price - synced_entry_price) > allowed_delta:
            return False, f"entry_price_mismatch:{stored_entry_price}->{synced_entry_price}"

    stored_shares = to_float_or_none(open_row.get("shares", ""))
    synced_shares = to_float_or_none(sync_result.get("entry_filled_qty", "") or sync_result.get("shares", ""))
    if stored_shares is not None and synced_shares is not None and stored_shares > 0:
        allowed_share_delta = max(1.0, stored_shares * 0.05)
        if abs(stored_shares - synced_shares) > allowed_share_delta:
            return False, f"entry_qty_mismatch:{stored_shares}->{synced_shares}"

    return True, None


def _sort_open_rows_for_sync(open_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    non_ibkr_rows: list[dict[str, Any]] = []
    ibkr_rows: list[dict[str, Any]] = []

    for row in open_rows or []:
        broker_name = str(row.get("broker", "") or "ALPACA").strip().upper() or "ALPACA"
        if broker_name == "IBKR":
            ibkr_rows.append(row)
        else:
            non_ibkr_rows.append(row)

    def ibkr_sort_key(row: dict[str, Any]) -> tuple[int, str, str]:
        timestamp = str(
            row.get("timestamp_utc")
            or row.get("entry_time")
            or row.get("created_at")
            or ""
        ).strip()
        parent_order_id = str(row.get("broker_parent_order_id", "") or row.get("parent_order_id", "") or "").strip()
        return (0 if timestamp else 1, timestamp, parent_order_id)

    return non_ibkr_rows + sorted(ibkr_rows, key=ibkr_sort_key)


def _sort_open_rows_for_sync_with_broker_state(
    *,
    open_rows: list[dict[str, Any]],
    get_open_positions: Callable[[], list[dict[str, Any]]] | None,
    get_open_positions_for_broker: Callable[[str], list[dict[str, Any]]] | None,
    get_open_state_for_broker: Callable[[str], dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    sorted_rows = _sort_open_rows_for_sync(open_rows)
    ibkr_rows = [
        row for row in sorted_rows
        if (str(row.get("broker", "") or "ALPACA").strip().upper() or "ALPACA") == "IBKR"
    ]
    if not ibkr_rows:
        return sorted_rows

    try:
        broker_open_state = _read_broker_open_state(
            broker_name="IBKR",
            get_open_positions=get_open_positions,
            get_open_positions_for_broker=get_open_positions_for_broker,
            get_open_state_for_broker=get_open_state_for_broker,
        )
    except Exception:
        return sorted_rows

    broker_open_symbols = {
        str(position.get("symbol", "")).strip().upper()
        for position in list(broker_open_state.get("positions") or [])
        if str(position.get("symbol", "")).strip()
    }
    broker_open_orders = list(broker_open_state.get("orders") or [])

    def ibkr_priority_key(row: dict[str, Any]) -> tuple[int, int, str, str]:
        symbol = str(row.get("symbol", "")).strip().upper()
        parent_order_id = str(row.get("broker_parent_order_id", "") or row.get("parent_order_id", "") or "").strip()
        has_related_order = any(
            str(order.get("symbol", "")).strip().upper() == symbol
            or str(order.get("id", "")).strip() == parent_order_id
            or str(order.get("parent_id", "")).strip() == parent_order_id
            for order in broker_open_orders
        )
        likely_missing_from_broker = symbol not in broker_open_symbols and not has_related_order
        timestamp = str(
            row.get("timestamp_utc")
            or row.get("entry_time")
            or row.get("created_at")
            or ""
        ).strip()
        return (
            0 if likely_missing_from_broker else 1,
            0 if timestamp else 1,
            timestamp,
            parent_order_id,
        )

    prioritized_ibkr_rows = sorted(ibkr_rows, key=ibkr_priority_key)
    non_ibkr_rows = [row for row in sorted_rows if row not in ibkr_rows]
    return non_ibkr_rows + prioritized_ibkr_rows


def _read_broker_open_symbols(
    *,
    broker_name: str,
    get_open_positions: Callable[[], list[dict[str, Any]]] | None,
    get_open_positions_for_broker: Callable[[str], list[dict[str, Any]]] | None,
) -> set[str]:
    if get_open_positions_for_broker is not None:
        positions = get_open_positions_for_broker(broker_name) or []
    elif get_open_positions is not None:
        positions = get_open_positions() or []
    else:
        positions = []
    return {str(position.get("symbol", "")).strip().upper() for position in positions if str(position.get("symbol", "")).strip()}


def _read_broker_open_state(
    *,
    broker_name: str,
    get_open_positions: Callable[[], list[dict[str, Any]]] | None,
    get_open_positions_for_broker: Callable[[str], list[dict[str, Any]]] | None,
    get_open_state_for_broker: Callable[[str], dict[str, Any]] | None,
) -> dict[str, Any]:
    if get_open_state_for_broker is not None:
        open_state = get_open_state_for_broker(broker_name) or {}
        if isinstance(open_state, dict):
            return {
                "positions": list(open_state.get("positions") or []),
                "orders": list(open_state.get("orders") or []),
            }
    positions = []
    if get_open_positions_for_broker is not None:
        positions = get_open_positions_for_broker(broker_name) or []
    elif get_open_positions is not None:
        positions = get_open_positions() or []
    return {
        "positions": list(positions or []),
        "orders": [],
    }


def _build_ibkr_stale_reconciled_sync_result(
    *,
    open_row: dict[str, Any],
    parent_order_id: str,
    sync_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    source = sync_result or {}
    exit_price = str(source.get("exit_price", "") or "").strip()
    exit_filled_avg_price = str(source.get("exit_filled_avg_price", "") or "").strip()
    exit_reason = str(source.get("exit_reason", "") or "STALE_OPEN_RECONCILED").strip() or "STALE_OPEN_RECONCILED"
    return {
        **source,
        "exit_event": "MANUAL_CLOSE",
        "exit_reason": exit_reason,
        "exit_status": str(source.get("exit_status", "") or "reconciled_closed"),
        "exit_order_id": str(source.get("exit_order_id", "") or parent_order_id),
        "exit_filled_qty": str(source.get("exit_filled_qty", "") or open_row.get("shares", "")),
        "exit_price": exit_price,
        "exit_filled_avg_price": exit_filled_avg_price or exit_price,
    }


def _reconcile_ibkr_stale_open_trade(
    *,
    broker_name: str,
    symbol: str,
    parent_order_id: str,
    open_row: dict[str, Any],
    cached_open_state_by_broker: dict[str, dict[str, Any]],
    get_open_positions: Callable[[], list[dict[str, Any]]] | None,
    get_open_positions_for_broker: Callable[[str], list[dict[str, Any]]] | None,
    get_open_state_for_broker: Callable[[str], dict[str, Any]] | None,
    sync_result: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], bool]:
    if broker_name != "IBKR":
        return sync_result or {}, False

    try:
        if broker_name not in cached_open_state_by_broker:
            log_info(
                "Reading IBKR broker open state for stale reconciliation",
                component="sync_service",
                operation="execute_sync_paper_trades",
                symbol=symbol,
                broker=broker_name,
                parent_order_id=parent_order_id,
            )
            cached_open_state_by_broker[broker_name] = _read_broker_open_state(
                broker_name=broker_name,
                get_open_positions=get_open_positions,
                get_open_positions_for_broker=get_open_positions_for_broker,
                get_open_state_for_broker=get_open_state_for_broker,
            )
        broker_open_state = cached_open_state_by_broker[broker_name]
    except Exception as e:
        log_exception(
            "IBKR open-state read failed during stale reconciliation",
            e,
            component="sync_service",
            operation="execute_sync_paper_trades",
            symbol=symbol,
            parent_order_id=parent_order_id,
        )
        broker_open_state = {
            "positions": [{"symbol": symbol}],
            "orders": [],
        }

    broker_open_positions = list(broker_open_state.get("positions") or [])
    broker_open_orders = list(broker_open_state.get("orders") or [])
    broker_open_symbols = {
        str(position.get("symbol", "")).strip().upper()
        for position in broker_open_positions
        if str(position.get("symbol", "")).strip()
    }
    related_orders = [
        order for order in broker_open_orders
        if (
            str(order.get("symbol", "")).strip().upper() == symbol
            or str(order.get("id", "")).strip() == parent_order_id
            or str(order.get("parent_id", "")).strip() == parent_order_id
        )
    ]

    log_info(
        "Evaluated IBKR stale reconciliation broker snapshot",
        component="sync_service",
        operation="execute_sync_paper_trades",
        symbol=symbol,
        broker=broker_name,
        parent_order_id=parent_order_id,
        broker_open_symbol_count=len(broker_open_symbols),
        broker_has_symbol=symbol in broker_open_symbols,
        broker_related_order_count=len(related_orders),
        broker_open_symbols_sample=",".join(sorted(list(broker_open_symbols))[:10]),
    )

    if symbol in broker_open_symbols or related_orders:
        log_info(
            "IBKR stale reconciliation skipped because symbol or related orders still appear open on broker snapshot",
            component="sync_service",
            operation="execute_sync_paper_trades",
            symbol=symbol,
            broker=broker_name,
            parent_order_id=parent_order_id,
            broker_has_symbol=symbol in broker_open_symbols,
            broker_related_order_count=len(related_orders),
        )
        return sync_result or {}, False

    reconciled_sync_result = _build_ibkr_stale_reconciled_sync_result(
        open_row=open_row,
        parent_order_id=parent_order_id,
        sync_result=sync_result,
    )
    log_info(
        "IBKR stale open trade reconciled closed from broker state",
        component="sync_service",
        operation="execute_sync_paper_trades",
        symbol=symbol,
        broker=broker_name,
        parent_order_id=parent_order_id,
    )
    return reconciled_sync_result, True



def execute_sync_paper_trades(
    *,
    get_open_paper_trades: Callable[[], list[dict[str, Any]]],
    sync_order_by_id: Callable[[str], dict[str, Any]] | None = None,
    sync_order_by_id_for_broker: Callable[[str, str], dict[str, Any]] | None = None,
    paper_trade_exit_already_logged: Callable[[str, str], bool],
    append_trade_log: Callable[[dict[str, Any]], None],
    safe_insert_trade_event: Callable[..., None],
    safe_insert_broker_order: Callable[..., None],
    upsert_trade_lifecycle: Callable[..., None],
    parse_iso_utc: Callable[[str], Any],
    to_float_or_none: Callable[[Any], float | None],
    get_open_positions: Callable[[], list[dict[str, Any]]] | None = None,
    get_open_positions_for_broker: Callable[[str], list[dict[str, Any]]] | None = None,
    get_open_state_for_broker: Callable[[str], dict[str, Any]] | None = None,
    close_position: Callable[[str], Any] | None = None,
    close_position_for_broker: Callable[[str, str], Any] | None = None,
) -> dict[str, Any] | tuple[dict[str, Any], int]:
    try:
        open_rows = _sort_open_rows_for_sync_with_broker_state(
            open_rows=get_open_paper_trades(),
            get_open_positions=get_open_positions,
            get_open_positions_for_broker=get_open_positions_for_broker,
            get_open_state_for_broker=get_open_state_for_broker,
        )
    except Exception as e:
        log_exception("Open paper trade read failed", e, component="sync_service", operation="execute_sync_paper_trades")
        return {"ok": False, "error": f"open paper trade read failed: {e}"}, 500

    results: list[dict[str, Any]] = []
    synced_count = 0
    skipped_count = 0
    cached_open_state_by_broker: dict[str, dict[str, Any]] = {}
    batch_started_at = time.monotonic()
    partial = False
    stopped_reason: str | None = None

    for open_row in open_rows:
        parent_order_id = str(open_row.get("broker_parent_order_id", "")).strip()
        symbol = str(open_row.get("symbol", "")).strip().upper()
        broker_name = str(open_row.get("broker", "") or "ALPACA").strip().upper() or "ALPACA"
        time_budget_seconds = _sync_time_budget_seconds(broker_name=broker_name)

        if time_budget_seconds is not None and (time.monotonic() - batch_started_at) >= time_budget_seconds:
            partial = True
            stopped_reason = f"{broker_name.lower()}_batch_time_budget_exceeded"
            log_warning(
                "Paper trade sync stopped after broker batch time budget was exceeded",
                component="sync_service",
                operation="execute_sync_paper_trades",
                broker=broker_name,
                time_budget_seconds=time_budget_seconds,
                elapsed_seconds=round(time.monotonic() - batch_started_at, 3),
                symbol=symbol,
                parent_order_id=parent_order_id,
            )
            results.append({
                "symbol": symbol,
                "broker": broker_name,
                "parent_order_id": parent_order_id,
                "synced": False,
                "reason": "batch_time_budget_exceeded",
                "details": f"{broker_name} sync batch stopped after {time_budget_seconds:.1f}s",
            })
            skipped_count += 1
            break

        if not parent_order_id:
            results.append({
                "symbol": symbol,
                "broker": broker_name,
                "synced": False,
                "reason": "missing_parent_order_id",
            })
            skipped_count += 1
            continue

        try:
            if sync_order_by_id_for_broker is not None:
                sync_result = sync_order_by_id_for_broker(broker_name, parent_order_id)
            elif sync_order_by_id is not None:
                sync_result = sync_order_by_id(parent_order_id)
            else:
                raise RuntimeError("sync_order_by_id is not configured")
            stale_reconciled = False
        except Exception as e:
            failure_reason, bridge_issue = _classify_ibkr_bridge_issue(e, broker_name=broker_name)
            recovered_from_timeout = False
            stale_reconciled = False
            if broker_name == "IBKR":
                log_info(
                    "IBKR sync exception classified",
                    component="sync_service",
                    operation="execute_sync_paper_trades",
                    symbol=symbol,
                    parent_order_id=parent_order_id,
                    broker=broker_name,
                    failure_reason=failure_reason,
                    bridge_issue=bridge_issue,
                )
            if broker_name == "IBKR" and failure_reason == "bridge_timeout":
                reconciled_sync_result, stale_reconciled = _reconcile_ibkr_stale_open_trade(
                    broker_name=broker_name,
                    symbol=symbol,
                    parent_order_id=parent_order_id,
                    open_row=open_row,
                    cached_open_state_by_broker=cached_open_state_by_broker,
                    get_open_positions=get_open_positions,
                    get_open_positions_for_broker=get_open_positions_for_broker,
                    get_open_state_for_broker=get_open_state_for_broker,
                    sync_result={},
                )
                if stale_reconciled:
                    sync_result = reconciled_sync_result
                    recovered_from_timeout = bool(str(sync_result.get("exit_event", "")).strip().upper())
                    log_info(
                        "IBKR timeout recovery attempted via stale reconciliation",
                        component="sync_service",
                        operation="execute_sync_paper_trades",
                        symbol=symbol,
                        parent_order_id=parent_order_id,
                        broker=broker_name,
                        stale_reconciled=stale_reconciled,
                        recovered_from_timeout=recovered_from_timeout,
                        recovered_exit_event=sync_result.get("exit_event", ""),
                        recovered_exit_reason=sync_result.get("exit_reason", ""),
                    )

            if not recovered_from_timeout:
                log_exception(
                    "Paper trade sync failed",
                    e,
                    component="sync_service",
                    operation="execute_sync_paper_trades",
                    symbol=symbol,
                    parent_order_id=parent_order_id,
                    broker=broker_name,
                    failure_reason=failure_reason,
                    bridge_issue=bridge_issue,
                )
                results.append({
                    "symbol": symbol,
                    "broker": broker_name,
                    "parent_order_id": parent_order_id,
                    "synced": False,
                    "reason": failure_reason,
                    "details": str(e),
                    "bridge_issue": bridge_issue,
                })
                skipped_count += 1
                continue

        identity_conflict_reason = None
        if broker_name == "IBKR":
            sync_identity_valid, identity_conflict_reason = _validate_ibkr_sync_identity(
                open_row=open_row,
                sync_result=sync_result,
                to_float_or_none=to_float_or_none,
            )
            if not sync_identity_valid:
                log_warning(
                    "IBKR sync result rejected due to identity mismatch",
                    component="sync_service",
                    operation="execute_sync_paper_trades",
                    symbol=symbol,
                    parent_order_id=parent_order_id,
                    broker=broker_name,
                    identity_conflict_reason=identity_conflict_reason,
                    sync_symbol=sync_result.get("symbol", ""),
                    sync_client_order_id=sync_result.get("client_order_id", ""),
                    sync_entry_price=sync_result.get("entry_filled_avg_price", "") or sync_result.get("entry_price", ""),
                    sync_entry_qty=sync_result.get("entry_filled_qty", "") or sync_result.get("shares", ""),
                )
                sync_result = {
                    "id": parent_order_id,
                    "status": "unknown",
                    "parent_status": "unknown",
                    "message": "IBKR sync identity mismatch",
                    "identity_conflict": True,
                    "identity_conflict_reason": identity_conflict_reason,
                }

        exit_event = str(sync_result.get("exit_event", "")).strip().upper()
        if not exit_event and broker_name == "IBKR":
            sync_status = str(sync_result.get("status", "")).strip().lower()
            parent_status = str(sync_result.get("parent_status", "")).strip().lower()
            is_unknown_parent = sync_status == "unknown" or parent_status == "unknown"
            log_info(
                "IBKR sync returned without exit event",
                component="sync_service",
                operation="execute_sync_paper_trades",
                symbol=symbol,
                parent_order_id=parent_order_id,
                broker=broker_name,
                sync_status=sync_status,
                parent_status=parent_status,
                take_profit_status=str(sync_result.get("take_profit_status", "")).strip().lower(),
                stop_loss_status=str(sync_result.get("stop_loss_status", "")).strip().lower(),
                is_unknown_parent=is_unknown_parent,
                identity_conflict=bool(sync_result.get("identity_conflict")),
                identity_conflict_reason=str(sync_result.get("identity_conflict_reason", "") or ""),
            )
            if is_unknown_parent:
                sync_result, stale_reconciled = _reconcile_ibkr_stale_open_trade(
                    broker_name=broker_name,
                    symbol=symbol,
                    parent_order_id=parent_order_id,
                    open_row=open_row,
                    cached_open_state_by_broker=cached_open_state_by_broker,
                    get_open_positions=get_open_positions,
                    get_open_positions_for_broker=get_open_positions_for_broker,
                    get_open_state_for_broker=get_open_state_for_broker,
                    sync_result=sync_result,
                )
                if stale_reconciled:
                    exit_event = "MANUAL_CLOSE"
                    log_info(
                        "IBKR unknown-parent recovery succeeded via stale reconciliation",
                        component="sync_service",
                        operation="execute_sync_paper_trades",
                        symbol=symbol,
                        parent_order_id=parent_order_id,
                        broker=broker_name,
                        exit_event=exit_event,
                        exit_reason=sync_result.get("exit_reason", ""),
                    )

        if not exit_event:
            if broker_name == "IBKR":
                log_warning(
                    "IBKR trade remains open on broker after sync",
                    component="sync_service",
                    operation="execute_sync_paper_trades",
                    symbol=symbol,
                    parent_order_id=parent_order_id,
                    parent_status=sync_result.get("parent_status", ""),
                    take_profit_status=sync_result.get("take_profit_status", ""),
                    stop_loss_status=sync_result.get("stop_loss_status", ""),
                )
            results.append({
                "symbol": symbol,
                "broker": broker_name,
                "parent_order_id": parent_order_id,
                "synced": False,
                "reason": "identity_conflict" if sync_result.get("identity_conflict") else "still_open",
                "parent_status": sync_result.get("parent_status", ""),
                "take_profit_status": sync_result.get("take_profit_status", ""),
                "stop_loss_status": sync_result.get("stop_loss_status", ""),
                "identity_conflict_reason": sync_result.get("identity_conflict_reason", ""),
            })
            skipped_count += 1
            continue

        exit_already_logged = paper_trade_exit_already_logged(parent_order_id, exit_event)
        if exit_already_logged:
            log_info(
                "Paper trade exit event already logged; proceeding with lifecycle repair only",
                component="sync_service",
                operation="execute_sync_paper_trades",
                symbol=symbol,
                parent_order_id=parent_order_id,
                broker=broker_name,
                exit_event=exit_event,
                stale_reconciled=stale_reconciled,
            )

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
            trade_source = str(open_row.get("trade_source", f"{broker_name}_PAPER")).strip().upper() or f"{broker_name}_PAPER"

            if not exit_already_logged:
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
                    "notes": f"Paper trade exit synced from {broker_name}. exit_event={exit_event}",
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
            trade_key = normalize_trade_key(symbol, broker_parent_order_id, broker_order_id, broker_name)

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
                "broker": broker_name,
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
            "broker": broker_name,
            "parent_order_id": parent_order_id,
            "synced": True,
            "exit_event": exit_event,
            "exit_price": sync_result.get("exit_price", ""),
            "exit_order_id": sync_result.get("exit_order_id", ""),
            "parent_status": sync_result.get("parent_status", ""),
            "exit_status": sync_result.get("exit_status", sync_result.get("parent_status", "")),
            "stale_reconciled": stale_reconciled,
            "exit_already_logged": exit_already_logged,
        })

    auto_healed_positions: list[dict[str, Any]] = []
    auto_heal_errors: list[dict[str, Any]] = []

    broker_names = sorted(
        {
            str(row.get("broker", "") or "ALPACA").strip().upper() or "ALPACA"
            for row in (open_rows or [])
        }
    )
    if not broker_names and (get_open_positions_for_broker or get_open_positions):
        broker_names = ["ALPACA"]

    for broker_name in broker_names:
        try:
            if get_open_positions_for_broker is not None:
                leftover_positions = get_open_positions_for_broker(broker_name) or []
            elif get_open_positions is not None:
                leftover_positions = get_open_positions() or []
            else:
                leftover_positions = []
        except Exception as e:
            leftover_positions = []
            auto_heal_errors.append({
                "broker": broker_name,
                "symbol": "",
                "reason": "open_positions_read_failed",
                "details": str(e),
            })

        db_open_symbols = {
            str(row.get("symbol", "")).strip().upper()
            for row in (open_rows or [])
            if str(row.get("symbol", "")).strip()
            and (str(row.get("broker", "") or "ALPACA").strip().upper() or "ALPACA") == broker_name
        }

        for position in leftover_positions:
            try:
                symbol = str(position.get("symbol", "")).strip().upper()
                qty = position.get("qty")
                side = str(position.get("side", "")).strip().lower()
                if not symbol:
                    continue
                if symbol in db_open_symbols:
                    continue

                if close_position_for_broker is not None:
                    close_result = close_position_for_broker(broker_name, symbol)
                elif close_position is not None:
                    close_result = close_position(symbol)
                else:
                    raise RuntimeError("close_position is not configured")

                auto_healed_positions.append({
                    "broker": broker_name,
                    "symbol": symbol,
                    "qty": qty,
                    "side": side,
                    "closed": True,
                    "result": close_result,
                })
            except Exception as e:
                auto_heal_errors.append({
                    "broker": broker_name,
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
        "partial": partial,
        "stopped_reason": stopped_reason,
        "auto_healed_count": len(auto_healed_positions),
        "auto_healed_positions": auto_healed_positions,
        "auto_heal_error_count": len(auto_heal_errors),
        "auto_heal_errors": auto_heal_errors,
    }
