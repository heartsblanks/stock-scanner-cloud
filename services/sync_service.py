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


def _resolve_confirmed_exit_timestamp(sync_result: dict[str, Any], parse_iso_utc: Callable[[str], Any]):
    for key in ("exit_filled_at", "exit_time", "filled_at"):
        raw_value = str(sync_result.get(key, "") or "").strip()
        if not raw_value:
            continue
        try:
            return parse_iso_utc(raw_value)
        except Exception:
            continue
    return None


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


def _int_env(name: str, default: int) -> int:
    raw_value = str(os.getenv(name, "")).strip()
    if not raw_value:
        return default
    try:
        return int(raw_value)
    except Exception:
        return default


def _normalize_ibkr_client_order_id(value: Any) -> str:
    return str(value or "").strip()


def _parse_sync_timestamp(raw_value: Any) -> datetime | None:
    text = str(raw_value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _first_sync_timestamp(sync_result: dict[str, Any], keys: tuple[str, ...]) -> datetime | None:
    for key in keys:
        parsed = _parse_sync_timestamp(sync_result.get(key))
        if parsed is not None:
            return parsed
    return None


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

    open_entry_time = _first_sync_timestamp(
        open_row,
        ("entry_time", "timestamp_utc", "created_at"),
    )
    synced_exit_time = _first_sync_timestamp(
        sync_result,
        ("exit_filled_at", "exit_time", "filled_at", "updated_at"),
    )
    if open_entry_time is not None and synced_exit_time is not None:
        # Protect against IBKR order-id reuse returning stale fills from a prior session.
        if synced_exit_time < open_entry_time:
            return False, f"exit_time_before_entry:{open_entry_time.isoformat()}->{synced_exit_time.isoformat()}"

    synced_exit_price = to_float_or_none(
        sync_result.get("exit_filled_avg_price", "") or sync_result.get("exit_price", "")
    )
    if stored_entry_price is not None and synced_exit_price is not None and stored_entry_price > 0:
        max_multiplier_raw = str(os.getenv("IBKR_SYNC_EXIT_PRICE_MAX_MULTIPLIER", "3.0")).strip()
        min_multiplier_raw = str(os.getenv("IBKR_SYNC_EXIT_PRICE_MIN_MULTIPLIER", "0.2")).strip()
        try:
            max_multiplier = max(float(max_multiplier_raw), 1.0)
        except Exception:
            max_multiplier = 3.0
        try:
            min_multiplier = max(min(float(min_multiplier_raw), 1.0), 0.01)
        except Exception:
            min_multiplier = 0.2
        ratio = synced_exit_price / stored_entry_price
        if ratio > max_multiplier or ratio < min_multiplier:
            return False, f"exit_price_ratio_outlier:{round(ratio, 6)}"

    return True, None


def _sort_open_rows_for_sync(open_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    non_ibkr_rows: list[dict[str, Any]] = []
    ibkr_rows: list[dict[str, Any]] = []

    for row in open_rows or []:
        broker_name = str(row.get("broker", "") or "IBKR").strip().upper() or "IBKR"
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


def _open_row_recency_rank(row: dict[str, Any]) -> tuple[float, int, str]:
    parsed_timestamp = _first_sync_timestamp(
        row,
        ("updated_at", "entry_time", "timestamp_utc", "created_at"),
    )
    timestamp_score = parsed_timestamp.timestamp() if parsed_timestamp is not None else float("-inf")

    row_id_raw = str(row.get("id", "")).strip()
    try:
        row_id_score = int(row_id_raw)
    except Exception:
        row_id_score = -1

    fallback_timestamp = str(
        row.get("updated_at")
        or row.get("entry_time")
        or row.get("timestamp_utc")
        or row.get("created_at")
        or ""
    ).strip()
    return (timestamp_score, row_id_score, fallback_timestamp)


def _dedupe_open_rows_by_parent_order(
    open_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    deduped_rows: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    index_by_parent_key: dict[tuple[str, str], int] = {}

    for row in open_rows or []:
        broker_name = str(row.get("broker", "") or "IBKR").strip().upper() or "IBKR"
        parent_order_id = str(
            row.get("broker_parent_order_id")
            or row.get("parent_order_id")
            or row.get("broker_order_id")
            or ""
        ).strip()
        if not parent_order_id:
            deduped_rows.append(row)
            continue

        dedupe_key = (broker_name, parent_order_id)
        existing_index = index_by_parent_key.get(dedupe_key)
        if existing_index is None:
            index_by_parent_key[dedupe_key] = len(deduped_rows)
            deduped_rows.append(row)
            continue

        existing_row = deduped_rows[existing_index]
        existing_rank = _open_row_recency_rank(existing_row)
        current_rank = _open_row_recency_rank(row)
        keep_current = current_rank >= existing_rank
        dropped_row = existing_row if keep_current else row
        if keep_current:
            deduped_rows[existing_index] = row

        duplicates.append({
            "broker": broker_name,
            "parent_order_id": parent_order_id,
            "symbol": str(dropped_row.get("symbol", "")).strip().upper(),
            "trade_key": str(dropped_row.get("trade_key", "")).strip(),
            "dropped_trade_id": str(dropped_row.get("id", "")).strip(),
            "reason": "duplicate_parent_order_id",
        })

    return deduped_rows, duplicates


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
        if (str(row.get("broker", "") or "IBKR").strip().upper() or "IBKR") == "IBKR"
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
    has_fill_data = bool(exit_filled_avg_price or exit_price) and bool(
        str(source.get("exit_filled_at", "") or source.get("exit_time", "") or source.get("filled_at", "")).strip()
    )
    default_exit_reason = "BROKER_FILLED_EXIT_REPAIRED" if has_fill_data else "BROKER_POSITION_FLAT_PENDING_FILL_SYNC"
    exit_reason = str(source.get("exit_reason", "") or default_exit_reason).strip() or default_exit_reason
    return {
        **source,
        "exit_event": "MANUAL_CLOSE" if has_fill_data else "",
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
    sync_orders_by_ids_for_broker: Callable[[str, list[str]], dict[str, dict[str, Any]]] | None = None,
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
        sorted_open_rows = _sort_open_rows_for_sync_with_broker_state(
            open_rows=get_open_paper_trades(),
            get_open_positions=get_open_positions,
            get_open_positions_for_broker=get_open_positions_for_broker,
            get_open_state_for_broker=get_open_state_for_broker,
        )
    except Exception as e:
        log_exception("Open paper trade read failed", e, component="sync_service", operation="execute_sync_paper_trades")
        return {"ok": False, "error": f"open paper trade read failed: {e}"}, 500

    open_rows, deduped_duplicates = _dedupe_open_rows_by_parent_order(sorted_open_rows)
    if deduped_duplicates:
        log_info(
            "Deduped duplicate OPEN paper-trade rows by broker parent order id before sync",
            component="sync_service",
            operation="execute_sync_paper_trades",
            duplicate_count=len(deduped_duplicates),
            kept_count=len(open_rows),
            raw_count=len(sorted_open_rows),
            sample_parent_order_ids=",".join(
                [
                    str(item.get("parent_order_id", "")).strip()
                    for item in deduped_duplicates[:10]
                    if str(item.get("parent_order_id", "")).strip()
                ]
            ),
        )

    results: list[dict[str, Any]] = []
    synced_count = 0
    skipped_count = len(deduped_duplicates)
    for duplicate in deduped_duplicates:
        results.append({
            "symbol": duplicate.get("symbol", ""),
            "broker": duplicate.get("broker", "IBKR"),
            "parent_order_id": duplicate.get("parent_order_id", ""),
            "synced": False,
            "reason": "duplicate_parent_order_deduped",
            "dropped_trade_id": duplicate.get("dropped_trade_id", ""),
        })

    cached_open_state_by_broker: dict[str, dict[str, Any]] = {}
    batch_started_at = time.monotonic()
    partial = False
    stopped_reason: str | None = None
    ibkr_sync_max_per_run = max(_int_env("IBKR_SYNC_MAX_PER_RUN", 8), 0)
    ibkr_timeout_circuit_threshold = max(_int_env("IBKR_SYNC_TIMEOUT_CIRCUIT_THRESHOLD", 3), 1)
    ibkr_timeout_circuit_cooldown_seconds = max(_int_env("IBKR_SYNC_TIMEOUT_CIRCUIT_COOLDOWN_SECONDS", 30), 1)
    ibkr_timeout_streak = 0
    ibkr_cooldown_until_monotonic: float | None = None
    ibkr_sync_attempted = 0
    ibkr_batch_sync_parent_ids: set[str] = set()
    ibkr_batch_sync_results: dict[str, dict[str, Any]] = {}

    if sync_orders_by_ids_for_broker is not None:
        ibkr_batch_candidate_parent_ids: list[str] = []
        seen_parent_ids: set[str] = set()
        for row in open_rows:
            broker_name = str(row.get("broker", "") or "IBKR").strip().upper() or "IBKR"
            if broker_name != "IBKR":
                continue
            parent_order_id = str(row.get("broker_parent_order_id", "")).strip()
            if not parent_order_id or parent_order_id in seen_parent_ids:
                continue
            seen_parent_ids.add(parent_order_id)
            if ibkr_sync_max_per_run > 0 and len(ibkr_batch_candidate_parent_ids) >= ibkr_sync_max_per_run:
                break
            ibkr_batch_candidate_parent_ids.append(parent_order_id)

        if ibkr_batch_candidate_parent_ids:
            try:
                batch_fetch_started_at = time.monotonic()
                fetched_batch_results = sync_orders_by_ids_for_broker("IBKR", ibkr_batch_candidate_parent_ids) or {}
                if not isinstance(fetched_batch_results, dict):
                    raise RuntimeError("IBKR batch sync returned non-dict payload")
                ibkr_batch_sync_parent_ids = set(ibkr_batch_candidate_parent_ids)
                ibkr_sync_attempted = len(ibkr_batch_sync_parent_ids)
                for parent_order_id, sync_row in fetched_batch_results.items():
                    normalized_parent_order_id = str(parent_order_id).strip()
                    if not normalized_parent_order_id:
                        continue
                    if isinstance(sync_row, dict):
                        ibkr_batch_sync_results[normalized_parent_order_id] = sync_row
                log_info(
                    "IBKR batch sync prefetch completed",
                    component="sync_service",
                    operation="execute_sync_paper_trades",
                    requested_count=len(ibkr_batch_candidate_parent_ids),
                    received_count=len(ibkr_batch_sync_results),
                    duration_ms=int((time.monotonic() - batch_fetch_started_at) * 1000),
                )
            except Exception as batch_error:
                log_exception(
                    "IBKR batch sync prefetch failed; falling back to per-order sync",
                    batch_error,
                    component="sync_service",
                    operation="execute_sync_paper_trades",
                    requested_count=len(ibkr_batch_candidate_parent_ids),
                )
                ibkr_batch_sync_parent_ids = set()
                ibkr_batch_sync_results = {}
                ibkr_sync_attempted = 0

    for open_row in open_rows:
        parent_order_id = str(open_row.get("broker_parent_order_id", "")).strip()
        symbol = str(open_row.get("symbol", "")).strip().upper()
        broker_name = str(open_row.get("broker", "") or "IBKR").strip().upper() or "IBKR"
        time_budget_seconds = _sync_time_budget_seconds(broker_name=broker_name)
        use_ibkr_batch_sync_result = broker_name == "IBKR" and parent_order_id in ibkr_batch_sync_parent_ids

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

        if broker_name == "IBKR" and not use_ibkr_batch_sync_result:
            now_monotonic = time.monotonic()
            if (
                ibkr_cooldown_until_monotonic is not None
                and now_monotonic < ibkr_cooldown_until_monotonic
            ):
                remaining_seconds = max(0.0, ibkr_cooldown_until_monotonic - now_monotonic)
                log_warning(
                    "IBKR sync skipped while timeout cooldown is active",
                    component="sync_service",
                    operation="execute_sync_paper_trades",
                    symbol=symbol,
                    parent_order_id=parent_order_id,
                    broker=broker_name,
                    ibkr_timeout_streak=ibkr_timeout_streak,
                    cooldown_remaining_seconds=round(remaining_seconds, 3),
                )
                results.append({
                    "symbol": symbol,
                    "broker": broker_name,
                    "parent_order_id": parent_order_id,
                    "synced": False,
                    "reason": "bridge_cooldown_active",
                    "details": f"IBKR sync cooldown active ({remaining_seconds:.1f}s remaining)",
                    "ibkr_timeout_streak": ibkr_timeout_streak,
                })
                skipped_count += 1
                continue

            if ibkr_sync_max_per_run > 0 and ibkr_sync_attempted >= ibkr_sync_max_per_run:
                log_warning(
                    "IBKR sync skipped after per-run sync cap was reached",
                    component="sync_service",
                    operation="execute_sync_paper_trades",
                    symbol=symbol,
                    parent_order_id=parent_order_id,
                    broker=broker_name,
                    ibkr_sync_attempted=ibkr_sync_attempted,
                    ibkr_sync_max_per_run=ibkr_sync_max_per_run,
                )
                results.append({
                    "symbol": symbol,
                    "broker": broker_name,
                    "parent_order_id": parent_order_id,
                    "synced": False,
                    "reason": "sync_cap_reached",
                    "details": f"IBKR sync cap reached ({ibkr_sync_attempted}/{ibkr_sync_max_per_run})",
                })
                skipped_count += 1
                continue

        try:
            if use_ibkr_batch_sync_result:
                sync_result = dict(ibkr_batch_sync_results.get(parent_order_id) or {
                    "id": parent_order_id,
                    "status": "unknown",
                    "message": "Order was not returned by IBKR batch sync prefetch.",
                })
            else:
                if broker_name == "IBKR":
                    ibkr_sync_attempted += 1
                if sync_order_by_id_for_broker is not None:
                    sync_result = sync_order_by_id_for_broker(broker_name, parent_order_id)
                elif sync_order_by_id is not None:
                    sync_result = sync_order_by_id(parent_order_id)
                else:
                    raise RuntimeError("sync_order_by_id is not configured")
            stale_reconciled = False
            if broker_name == "IBKR":
                ibkr_timeout_streak = 0
                ibkr_cooldown_until_monotonic = None
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
                if failure_reason == "bridge_timeout":
                    ibkr_timeout_streak += 1
                    if ibkr_timeout_streak >= ibkr_timeout_circuit_threshold:
                        ibkr_cooldown_until_monotonic = (
                            time.monotonic() + float(ibkr_timeout_circuit_cooldown_seconds)
                        )
                        log_warning(
                            "IBKR timeout circuit opened after consecutive bridge timeouts",
                            component="sync_service",
                            operation="execute_sync_paper_trades",
                            symbol=symbol,
                            parent_order_id=parent_order_id,
                            broker=broker_name,
                            ibkr_timeout_streak=ibkr_timeout_streak,
                            ibkr_timeout_circuit_threshold=ibkr_timeout_circuit_threshold,
                            ibkr_timeout_circuit_cooldown_seconds=ibkr_timeout_circuit_cooldown_seconds,
                        )
                else:
                    ibkr_timeout_streak = 0
                    ibkr_cooldown_until_monotonic = None
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

        if broker_name == "IBKR":
            confirmed_exit_timestamp = _resolve_confirmed_exit_timestamp(sync_result, parse_iso_utc)
            confirmed_exit_price = to_float_or_none(
                sync_result.get("exit_filled_avg_price", "") or sync_result.get("exit_price", "")
            )
            if confirmed_exit_timestamp is None or confirmed_exit_price is None:
                log_warning(
                    "IBKR close detected but fill evidence is incomplete; lifecycle close skipped",
                    component="sync_service",
                    operation="execute_sync_paper_trades",
                    symbol=symbol,
                    parent_order_id=parent_order_id,
                    broker=broker_name,
                    stale_reconciled=stale_reconciled,
                    exit_event=exit_event,
                    exit_filled_at=sync_result.get("exit_filled_at", ""),
                    exit_time=sync_result.get("exit_time", ""),
                    exit_filled_avg_price=sync_result.get("exit_filled_avg_price", ""),
                    exit_price=sync_result.get("exit_price", ""),
                )
                results.append({
                    "symbol": symbol,
                    "broker": broker_name,
                    "parent_order_id": parent_order_id,
                    "synced": False,
                    "reason": "missing_exit_fill_data",
                    "stale_reconciled": stale_reconciled,
                    "exit_event": exit_event,
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
            exit_timestamp = _resolve_confirmed_exit_timestamp(sync_result, parse_iso_utc)
            if exit_timestamp is None:
                exit_timestamp = _resolve_exit_timestamp(sync_result, timestamp_utc, parse_iso_utc)
            exit_timestamp_utc = exit_timestamp.astimezone(timezone.utc).isoformat() if exit_timestamp else timestamp_utc

            entry_price = open_row.get("entry_price", "")
            stop_price = open_row.get("stop_price", "")
            target_price = open_row.get("target_price", "")
            exit_price = sync_result.get("exit_filled_avg_price", "") or sync_result.get("exit_price", "")
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
            str(row.get("broker", "") or "IBKR").strip().upper() or "IBKR"
            for row in (open_rows or [])
        }
    )
    if not broker_names and (get_open_positions_for_broker or get_open_positions):
        broker_names = ["IBKR"]

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
            and (str(row.get("broker", "") or "IBKR").strip().upper() or "IBKR") == broker_name
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
        "raw_open_paper_trade_count": len(sorted_open_rows),
        "open_paper_trade_count": len(open_rows),
        "deduped_duplicate_open_rows": len(deduped_duplicates),
        "synced_count": synced_count,
        "skipped_count": skipped_count,
        "results": results,
        "partial": partial,
        "stopped_reason": stopped_reason,
        "auto_healed_count": len(auto_healed_positions),
        "auto_healed_positions": auto_healed_positions,
        "auto_heal_error_count": len(auto_heal_errors),
        "auto_heal_errors": auto_heal_errors,
        "ibkr_sync_attempted": ibkr_sync_attempted,
        "ibkr_sync_max_per_run": ibkr_sync_max_per_run,
        "ibkr_timeout_streak": ibkr_timeout_streak,
        "ibkr_cooldown_active": bool(
            ibkr_cooldown_until_monotonic is not None and time.monotonic() < ibkr_cooldown_until_monotonic
        ),
        "ibkr_batch_sync_prefetched": len(ibkr_batch_sync_parent_ids),
    }
