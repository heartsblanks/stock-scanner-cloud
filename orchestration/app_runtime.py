from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any, Callable

from core.logging_utils import log_info


def _truthy_env(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "true" if default else "false")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _scheduled_chunk_size() -> int:
    value = _to_int(os.getenv("PAPER_SCHEDULED_MAX_SYMBOLS_PER_SCAN", "8"), 8)
    return max(1, value)


def _normalized_symbols(symbols: Any) -> list[str]:
    if not isinstance(symbols, (list, tuple, set)):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        normalized_symbol = str(symbol or "").strip().upper()
        if normalized_symbol and normalized_symbol not in seen:
            normalized.append(normalized_symbol)
            seen.add(normalized_symbol)
    return normalized


def _chunk_symbols(symbols: list[str], chunk_size: int) -> list[list[str]]:
    return [symbols[index : index + chunk_size] for index in range(0, len(symbols), chunk_size)]


def _prioritize_symbols(symbols: list[str], priority_symbols: list[str]) -> list[str]:
    if not symbols or not priority_symbols:
        return symbols
    allowed = set(symbols)
    prioritized: list[str] = []
    seen: set[str] = set()
    for symbol in priority_symbols:
        normalized_symbol = str(symbol or "").strip().upper()
        if normalized_symbol in allowed and normalized_symbol not in seen:
            prioritized.append(normalized_symbol)
            seen.add(normalized_symbol)
    prioritized.extend(symbol for symbol in symbols if symbol not in seen)
    return prioritized


def _count_from_result(result: dict[str, Any], key: str) -> int:
    value = result.get(key)
    if value is None and isinstance(result.get("paper_trade_result"), dict):
        value = result["paper_trade_result"].get(key)
    return _to_int(value, 0)


def _paper_trade_reason(result: dict[str, Any]) -> str:
    paper_result = result.get("paper_trade_result")
    if isinstance(paper_result, dict):
        return str(paper_result.get("reason") or "")
    return ""


def _run_all_eligible_scheduled_mode(
    payload: dict[str, Any],
    *,
    mode: str,
    run_single_shadow_scan: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    try:
        from services.symbol_eligibility_service import resolve_session_symbol_allowlist

        symbol_allowlist = resolve_session_symbol_allowlist(mode=mode)
    except Exception as exc:
        log_info(
            "Scheduled all-eligible scan allowlist lookup failed; falling back to single scan",
            component="app_runtime",
            operation="handle_scan_request",
            mode=mode,
            error=str(exc),
        )
        symbol_allowlist = {
            "filter_applied": False,
            "reason": "allowlist_lookup_failed",
            "allowed_symbols": None,
            "error": str(exc),
        }

    allowed_symbols = _normalized_symbols(symbol_allowlist.get("allowed_symbols"))
    if not bool(symbol_allowlist.get("filter_applied")) or not allowed_symbols:
        mode_payload = dict(payload)
        mode_payload["mode"] = mode
        mode_response = run_single_shadow_scan(mode_payload)
        mode_result = mode_response if isinstance(mode_response, dict) else {"ok": False, "error": "invalid_ibkr_shadow_response"}
        mode_result.setdefault("mode", mode)
        mode_result["scheduled_scan_all_eligible"] = False
        mode_result["scheduled_scan_all_eligible_fallback_reason"] = str(symbol_allowlist.get("reason") or "empty_allowlist")
        return mode_result

    watch_ready_symbols: list[str] = []
    if _truthy_env("PAPER_WATCH_READY_ENABLED", True):
        try:
            from storage import get_recent_watch_ready_symbols

            watch_ready_symbols = get_recent_watch_ready_symbols(
                mode=mode,
                broker="IBKR",
                max_age_minutes=_to_int(os.getenv("PAPER_WATCH_READY_MAX_AGE_MINUTES", "20"), 20),
                limit=_to_int(os.getenv("PAPER_WATCH_READY_PRIORITY_LIMIT", "20"), 20),
            )
            allowed_symbols = _prioritize_symbols(allowed_symbols, watch_ready_symbols)
        except Exception as exc:
            log_info(
                "Scheduled scan watch-ready prioritization skipped",
                component="app_runtime",
                operation="handle_scan_request",
                mode=mode,
                error=str(exc),
            )

    chunk_size = _scheduled_chunk_size()
    chunks = _chunk_symbols(allowed_symbols, chunk_size)
    chunk_results: list[dict[str, Any]] = []
    total_candidates = 0
    total_placed = 0
    total_skipped = 0
    placed_symbols: set[str] = set()

    cycle_started_at = datetime.now(UTC)
    for chunk_index, chunk_symbols in enumerate(chunks):
        chunk_started_at = datetime.now(UTC)
        chunk_payload = dict(payload)
        chunk_payload["mode"] = mode
        chunk_payload["allowed_symbols"] = chunk_symbols
        chunk_payload["symbol_allowlist"] = {
            **symbol_allowlist,
            "allowed_symbols": chunk_symbols,
            "allowed_count": len(chunk_symbols),
            "chunking_applied": True,
            "chunk_size": chunk_size,
            "chunk_index": chunk_index,
            "chunk_count": len(chunks),
            "slot_index": chunk_index,
            "original_allowed_count": len(allowed_symbols),
            "chunk_start": chunk_index * chunk_size,
            "chunk_end": min(len(allowed_symbols), (chunk_index + 1) * chunk_size),
            "watch_ready_priority_symbols": watch_ready_symbols,
        }
        if placed_symbols:
            chunk_payload["scheduled_cycle_placed_symbols"] = sorted(placed_symbols)

        chunk_response = run_single_shadow_scan(chunk_payload)
        chunk_finished_at = datetime.now(UTC)
        chunk_result = chunk_response if isinstance(chunk_response, dict) else {"ok": False, "error": "invalid_ibkr_shadow_response"}
        chunk_candidate_count = _count_from_result(chunk_result, "candidate_count")
        chunk_placed_count = _count_from_result(chunk_result, "placed_count")
        chunk_skipped_count = _count_from_result(chunk_result, "skipped_count")
        total_candidates += chunk_candidate_count
        total_placed += chunk_placed_count
        total_skipped += chunk_skipped_count

        paper_result = chunk_result.get("paper_trade_result")
        if isinstance(paper_result, dict):
            for item in paper_result.get("results", []) or []:
                if isinstance(item, dict) and item.get("placed"):
                    symbol = str(item.get("symbol") or "").strip().upper()
                    if symbol:
                        placed_symbols.add(symbol)

        chunk_summary = {
            "ok": bool(chunk_result.get("ok", False)),
            "mode": mode,
            "chunk_index": chunk_index,
            "chunk_count": len(chunks),
            "chunk_size": chunk_size,
            "symbol_count": len(chunk_symbols),
            "symbols": chunk_symbols,
            "duration_ms": int((chunk_finished_at - chunk_started_at).total_seconds() * 1000),
            "candidate_count": chunk_candidate_count,
            "placed_count": chunk_placed_count,
            "skipped_count": chunk_skipped_count,
            "scan_id": chunk_result.get("scan_id"),
            "reason": _paper_trade_reason(chunk_result),
            "error": chunk_result.get("error"),
        }
        chunk_results.append(chunk_summary)
        log_info(
            "Scheduled all-eligible scan chunk completed",
            component="app_runtime",
            operation="handle_scan_request",
            mode=mode,
            chunk_index=chunk_index,
            chunk_count=len(chunks),
            symbol_count=len(chunk_symbols),
            candidate_count=chunk_candidate_count,
            placed_count=chunk_placed_count,
            skipped_count=chunk_skipped_count,
            duration_ms=chunk_summary["duration_ms"],
            ok=chunk_summary["ok"],
            error=chunk_summary["error"],
        )

    return {
        "ok": all(bool(chunk.get("ok", False)) for chunk in chunk_results),
        "mode": mode,
        "scheduled_scan_all_eligible": True,
        "chunk_size": chunk_size,
        "chunk_count": len(chunks),
        "completed_chunk_count": len(chunk_results),
        "original_allowed_count": len(allowed_symbols),
        "candidate_count": total_candidates,
        "placed_count": total_placed,
        "skipped_count": total_skipped,
        "placed_symbols": sorted(placed_symbols),
        "symbol_allowlist": {
            "filter_applied": bool(symbol_allowlist.get("filter_applied")),
            "mode": mode,
            "requested_session_date": symbol_allowlist.get("requested_session_date"),
            "source_session_date": symbol_allowlist.get("source_session_date"),
            "fallback_used": bool(symbol_allowlist.get("fallback_used", False)),
            "allowed_count": len(allowed_symbols),
            "excluded_count": _to_int(symbol_allowlist.get("excluded_count", 0), 0),
            "chunking_applied": True,
            "chunk_size": chunk_size,
            "chunk_count": len(chunks),
            "original_allowed_count": len(allowed_symbols),
            "watch_ready_priority_symbols": watch_ready_symbols,
        },
        "per_chunk_results": chunk_results,
        "duration_ms": int((datetime.now(UTC) - cycle_started_at).total_seconds() * 1000),
    }


def execute_scan_pipeline(
    payload: dict[str, Any],
    *,
    broker_name: str,
    run_handle_scan_request: Callable[..., Any],
    execute_full_scan: Callable[..., Any],
    market_time_check: Callable[..., Any],
    build_scan_id: Callable[..., str],
    market_phase_from_timestamp: Callable[..., str],
    append_signal_log: Callable[[dict], None],
    safe_insert_paper_trade_attempt: Callable[..., None],
    safe_insert_scan_run: Callable[..., None],
    parse_iso_utc: Callable[..., Any],
    run_scan_fn: Callable[..., Any],
    trade_to_dict: Callable[..., dict],
    debug_to_dict: Callable[..., dict],
    paper_candidate_from_evaluation: Callable[[dict], dict | None],
    evaluate_symbol: Callable[..., Any],
    get_latest_open_trade_fn: Callable[[str], dict | None],
    is_symbol_in_paper_cooldown: Callable[[str, str], tuple[bool, str]],
    place_paper_orders_fn: Callable[..., Any],
    append_trade_log: Callable[[dict], None],
    safe_insert_trade_event: Callable[..., None],
    safe_insert_broker_order: Callable[..., None],
    to_float_or_none: Callable[..., float | None],
    min_confidence: float,
    upsert_trade_lifecycle: Callable[..., None],
    resolve_account_size_fn: Callable[[dict[str, Any]], float],
):
    return run_handle_scan_request(
        payload,
        get_current_open_position_state=lambda: None,  # replaced by caller below
        get_risk_exposure_summary=lambda: None,  # replaced by caller below
        execute_full_scan=execute_full_scan,
        market_time_check=market_time_check,
        build_scan_id=build_scan_id,
        market_phase_from_timestamp=market_phase_from_timestamp,
        append_signal_log=append_signal_log,
        safe_insert_paper_trade_attempt=safe_insert_paper_trade_attempt,
        safe_insert_scan_run=safe_insert_scan_run,
        parse_iso_utc=parse_iso_utc,
        run_scan=run_scan_fn,
        trade_to_dict=trade_to_dict,
        debug_to_dict=debug_to_dict,
        paper_candidate_from_evaluation=paper_candidate_from_evaluation,
        evaluate_symbol=evaluate_symbol,
        get_latest_open_paper_trade_for_symbol=get_latest_open_trade_fn,
        is_symbol_in_paper_cooldown=is_symbol_in_paper_cooldown,
        place_paper_orders_from_trade=place_paper_orders_fn,
        append_trade_log=append_trade_log,
        safe_insert_trade_event=safe_insert_trade_event,
        safe_insert_broker_order=safe_insert_broker_order,
        to_float_or_none=to_float_or_none,
        min_confidence=min_confidence,
        upsert_trade_lifecycle=upsert_trade_lifecycle,
        resolve_account_size=resolve_account_size_fn,
        active_broker=broker_name,
    )


def build_scan_pipeline_runner(
    *,
    run_handle_scan_request: Callable[..., Any],
    execute_full_scan: Callable[..., Any],
    market_time_check: Callable[..., Any],
    build_scan_id: Callable[..., str],
    market_phase_from_timestamp: Callable[..., str],
    append_signal_log: Callable[[dict], None],
    safe_insert_paper_trade_attempt: Callable[..., None],
    safe_insert_scan_run: Callable[..., None],
    parse_iso_utc: Callable[..., Any],
    trade_to_dict: Callable[..., dict],
    debug_to_dict: Callable[..., dict],
    evaluate_symbol: Callable[..., Any],
    is_symbol_in_paper_cooldown: Callable[[str, str], tuple[bool, str]],
    append_trade_log: Callable[[dict], None],
    safe_insert_trade_event: Callable[..., None],
    safe_insert_broker_order: Callable[..., None],
    to_float_or_none: Callable[..., float | None],
    min_confidence: float,
    upsert_trade_lifecycle: Callable[..., None],
):
    def runner(
        payload: dict[str, Any],
        *,
        broker_name: str,
        run_scan_fn: Callable[..., Any],
        resolve_account_size_fn: Callable[[dict[str, Any]], float],
        get_current_open_position_state_fn: Callable[[], Any],
        get_risk_exposure_summary_fn: Callable[[], dict[str, Any]],
        get_latest_open_trade_fn: Callable[[str], dict | None],
        place_paper_orders_fn: Callable[..., Any],
        paper_candidate_from_evaluation_fn: Callable[[dict], dict | None],
    ):
        return run_handle_scan_request(
            payload,
            get_current_open_position_state=get_current_open_position_state_fn,
            get_risk_exposure_summary=get_risk_exposure_summary_fn,
            execute_full_scan=execute_full_scan,
            market_time_check=market_time_check,
            build_scan_id=build_scan_id,
            market_phase_from_timestamp=market_phase_from_timestamp,
            append_signal_log=append_signal_log,
            safe_insert_paper_trade_attempt=safe_insert_paper_trade_attempt,
            safe_insert_scan_run=safe_insert_scan_run,
            parse_iso_utc=parse_iso_utc,
            run_scan=run_scan_fn,
            trade_to_dict=trade_to_dict,
            debug_to_dict=debug_to_dict,
            paper_candidate_from_evaluation=paper_candidate_from_evaluation_fn,
            evaluate_symbol=evaluate_symbol,
            get_latest_open_paper_trade_for_symbol=get_latest_open_trade_fn,
            is_symbol_in_paper_cooldown=is_symbol_in_paper_cooldown,
            place_paper_orders_from_trade=place_paper_orders_fn,
            append_trade_log=append_trade_log,
            safe_insert_trade_event=safe_insert_trade_event,
            safe_insert_broker_order=safe_insert_broker_order,
            to_float_or_none=to_float_or_none,
            min_confidence=min_confidence,
            upsert_trade_lifecycle=upsert_trade_lifecycle,
            resolve_account_size=resolve_account_size_fn,
            active_broker=broker_name,
        )

    return runner


def run_ibkr_shadow_scans(
    payload: dict[str, Any],
    *,
    ibkr_scheduled_mode_order: list[str],
    run_single_shadow_scan: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    scan_source = str((payload or {}).get("scan_source", "") or "").strip().upper()
    paper_trade_requested = bool((payload or {}).get("paper_trade"))
    scheduled_shadow = scan_source == "SCHEDULED"
    modes = list(ibkr_scheduled_mode_order) if scheduled_shadow else [str(payload.get("mode", "primary")).strip().lower()]

    per_mode_results: list[dict[str, Any]] = []
    total_candidates = 0
    total_placed = 0
    total_skipped = 0

    scan_all_eligible = scheduled_shadow and paper_trade_requested and _truthy_env("PAPER_SCHEDULED_SCAN_ALL_ELIGIBLE", False)

    for mode in modes:
        if scan_all_eligible:
            mode_result = _run_all_eligible_scheduled_mode(
                payload,
                mode=mode,
                run_single_shadow_scan=run_single_shadow_scan,
            )
        else:
            mode_payload = dict(payload)
            mode_payload["mode"] = mode
            mode_response = run_single_shadow_scan(mode_payload)
            mode_result = mode_response if isinstance(mode_response, dict) else {"ok": False, "error": "invalid_ibkr_shadow_response", "mode": mode}
        mode_result.setdefault("mode", mode)
        per_mode_results.append(mode_result)

        total_candidates += _count_from_result(mode_result, "candidate_count")
        total_placed += _count_from_result(mode_result, "placed_count")
        total_skipped += _count_from_result(mode_result, "skipped_count")

        log_info(
            "IBKR shadow scan completed",
            component="app_runtime",
            operation="handle_scan_request",
            broker="IBKR",
            ok=bool(mode_result.get("ok", False)),
            mode=mode,
            error=mode_result.get("error"),
            candidate_count=mode_result.get("candidate_count"),
            placed_count=mode_result.get("placed_count"),
            skipped_count=mode_result.get("skipped_count"),
            scan_id=mode_result.get("scan_id"),
        )

    return {
        "ok": all(bool(result.get("ok", False)) for result in per_mode_results),
        "scheduled_all_modes": scheduled_shadow,
        "scheduled_scan_all_eligible": scan_all_eligible,
        "mode_count": len(modes),
        "modes": modes,
        "candidate_count": total_candidates,
        "placed_count": total_placed,
        "skipped_count": total_skipped,
        "per_mode_results": per_mode_results,
    }


def handle_scan_request(
    payload: dict[str, Any],
    *,
    run_ibkr_scan: Callable[[dict[str, Any]], Any],
    shadow_mode_enabled: bool,
    ibkr_bridge_enabled: bool,
    run_ibkr_shadow_scans: Callable[[dict[str, Any]], dict[str, Any]],
):
    def run_broker_scan(
        broker_name: str,
        broker_scan_fn: Callable[[dict[str, Any]], Any],
    ) -> tuple[Any, dict[str, Any]]:
        started_at = datetime.now(UTC)
        response = broker_scan_fn(dict(payload))
        finished_at = datetime.now(UTC)
        timing = {
            "started_at_utc": started_at.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "finished_at_utc": finished_at.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "duration_ms": int((finished_at - started_at).total_seconds() * 1000),
        }
        log_info(
            "Broker scan completed",
            component="app_runtime",
            operation="handle_scan_request",
            broker=broker_name,
            ok=bool(response.get("ok", False)) if isinstance(response, dict) else False,
            duration_ms=timing["duration_ms"],
            started_at_utc=timing["started_at_utc"],
            finished_at_utc=timing["finished_at_utc"],
            scan_id=response.get("scan_id") if isinstance(response, dict) else None,
        )
        return response, timing

    paper_trade_requested = bool(isinstance(payload, dict) and payload.get("paper_trade"))
    del run_ibkr_scan, shadow_mode_enabled

    if paper_trade_requested and not ibkr_bridge_enabled:
        return {
            "ok": False,
            "paper_trade": True,
            "placed_count": 0,
            "candidate_count": 0,
            "skipped_count": 0,
            "results": [],
            "reason": "ibkr_bridge_unavailable",
            "details": "IBKR execution is required, but the IBKR bridge is not active for this request.",
        }

    ibkr_response, ibkr_timing = run_broker_scan("IBKR", run_ibkr_shadow_scans)
    if isinstance(ibkr_response, dict):
        ibkr_response.setdefault("paper_trade", paper_trade_requested)
        ibkr_response["execution_broker"] = "IBKR"
        ibkr_response["parallel_runs"] = {
            "ibkr": {
                "ok": bool(ibkr_response.get("ok", False)),
                **(ibkr_timing or {}),
            },
        }
    return ibkr_response


def run_scheduled_scan_wrapper(
    payload: dict[str, Any],
    *,
    now_ny: datetime,
    run_scheduled_scan_wrapper_fn: Callable[..., Any],
    build_scheduled_scan_payload: Callable[..., dict[str, Any]],
    scheduled_mode_order: list[str],
    handle_scan_request_fn: Callable[[dict[str, Any]], Any],
):
    return run_scheduled_scan_wrapper_fn(
        payload,
        now_ny=now_ny,
        build_scheduled_scan_payload=lambda scan_payload, now_ny=None: build_scheduled_scan_payload(
            scan_payload,
            now_ny=now_ny,
            mode_order=scheduled_mode_order,
        ),
        handle_scan_request_fn=handle_scan_request_fn,
    )


def handle_sync_paper_trades(
    *,
    run_handle_sync_paper_trades: Callable[..., Any],
    execute_sync_paper_trades: Callable[..., Any],
    get_open_paper_trades: Callable[[], list[dict[str, Any]]],
    sync_order_by_id: Callable[..., Any],
    sync_order_by_id_for_broker: Callable[..., Any],
    sync_orders_by_ids_for_broker: Callable[..., Any] | None,
    paper_trade_exit_already_logged: Callable[[str, str], bool],
    append_trade_log: Callable[[dict], None],
    safe_insert_trade_event: Callable[..., None],
    safe_insert_broker_order: Callable[..., None],
    parse_iso_utc: Callable[..., Any],
    to_float_or_none: Callable[..., float | None],
    upsert_trade_lifecycle: Callable[..., None],
    get_open_positions: Callable[..., Any],
    close_position: Callable[..., Any],
    get_open_positions_for_broker_name: Callable[..., Any],
    get_open_state_for_broker_name: Callable[..., Any],
    close_position_for_broker_name: Callable[..., Any],
):
    return run_handle_sync_paper_trades(
        execute_sync_paper_trades=execute_sync_paper_trades,
        get_open_paper_trades=get_open_paper_trades,
        sync_order_by_id=sync_order_by_id,
        sync_order_by_id_for_broker=sync_order_by_id_for_broker,
        sync_orders_by_ids_for_broker=sync_orders_by_ids_for_broker,
        paper_trade_exit_already_logged=paper_trade_exit_already_logged,
        append_trade_log=append_trade_log,
        safe_insert_trade_event=safe_insert_trade_event,
        safe_insert_broker_order=safe_insert_broker_order,
        parse_iso_utc=parse_iso_utc,
        to_float_or_none=to_float_or_none,
        upsert_trade_lifecycle=upsert_trade_lifecycle,
        get_open_positions=get_open_positions,
        close_position=close_position,
        get_open_positions_for_broker=get_open_positions_for_broker_name,
        get_open_state_for_broker=get_open_state_for_broker_name,
        close_position_for_broker=close_position_for_broker_name,
    )


def close_all_paper_positions_for_broker(
    broker,
    *,
    run_close_all_paper_positions: Callable[..., Any],
    execute_close_all_paper_positions: Callable[..., Any],
    get_managed_open_paper_trades_for_eod_close_for_broker: Callable[..., Any],
    safe_insert_broker_order: Callable[..., None],
    append_trade_log: Callable[[dict], None],
    safe_insert_trade_event: Callable[..., None],
    upsert_trade_lifecycle: Callable[..., None],
    to_float_or_none: Callable[..., float | None],
    parse_iso_utc: Callable[..., Any],
):
    return run_close_all_paper_positions(
        execute_close_all_paper_positions=execute_close_all_paper_positions,
        get_open_positions=broker.get_open_positions,
        get_managed_open_paper_trades_for_eod_close=lambda: get_managed_open_paper_trades_for_eod_close_for_broker(broker),
        cancel_open_orders_for_symbol=broker.cancel_open_orders_for_symbol,
        close_position=broker.close_position,
        get_order_by_id=broker.get_order_by_id,
        sync_order_by_id=broker.sync_order_by_id,
        safe_insert_broker_order=safe_insert_broker_order,
        append_trade_log=append_trade_log,
        safe_insert_trade_event=safe_insert_trade_event,
        upsert_trade_lifecycle=upsert_trade_lifecycle,
        to_float_or_none=to_float_or_none,
        parse_iso_utc=parse_iso_utc,
    )


def close_all_paper_positions(
    *,
    ibkr_broker,
    shadow_mode_enabled: bool,
    ibkr_bridge_enabled: bool,
    close_all_paper_positions_for_broker_fn: Callable[[Any], Any],
):
    del shadow_mode_enabled

    if not ibkr_bridge_enabled:
        return {
            "ok": False,
            "reason": "ibkr_bridge_unavailable",
            "details": "IBKR close requested while bridge is unavailable.",
            "position_count": 0,
            "closed_count": 0,
            "skipped_count": 0,
        }

    return close_all_paper_positions_for_broker_fn(ibkr_broker)
