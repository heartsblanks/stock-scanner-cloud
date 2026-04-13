from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from core.logging_utils import log_info


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
    scheduled_shadow = scan_source == "SCHEDULED"
    modes = list(ibkr_scheduled_mode_order) if scheduled_shadow else [str(payload.get("mode", "primary")).strip().lower()]

    per_mode_results: list[dict[str, Any]] = []
    total_candidates = 0
    total_placed = 0
    total_skipped = 0

    for mode in modes:
        mode_payload = dict(payload)
        mode_payload["mode"] = mode
        mode_response = run_single_shadow_scan(mode_payload)
        mode_result = mode_response if isinstance(mode_response, dict) else {"ok": False, "error": "invalid_ibkr_shadow_response", "mode": mode}
        mode_result.setdefault("mode", mode)
        per_mode_results.append(mode_result)

        total_candidates += int(float(mode_result.get("candidate_count", 0) or 0))
        total_placed += int(float(mode_result.get("placed_count", 0) or 0))
        total_skipped += int(float(mode_result.get("skipped_count", 0) or 0))

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
    run_alpaca_scan: Callable[[dict[str, Any]], Any],
    shadow_mode_enabled: bool,
    ibkr_bridge_enabled: bool,
    run_ibkr_shadow_scans: Callable[[dict[str, Any]], dict[str, Any]],
):
    alpaca_response = run_alpaca_scan(payload)

    if not (isinstance(payload, dict) and payload.get("paper_trade") and shadow_mode_enabled and ibkr_bridge_enabled):
        return alpaca_response

    ibkr_response = run_ibkr_shadow_scans(payload)

    if isinstance(alpaca_response, tuple) or isinstance(ibkr_response, tuple):
        return alpaca_response

    if isinstance(alpaca_response, dict):
        alpaca_response["parallel_runs"] = {
            "alpaca": {"ok": bool(alpaca_response.get("ok", False))},
            "ibkr": {"ok": bool(ibkr_response.get("ok", False)) if isinstance(ibkr_response, dict) else False},
        }
        alpaca_response["shadow_ibkr"] = ibkr_response
    return alpaca_response


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
    close_position_for_broker_name: Callable[..., Any],
):
    return run_handle_sync_paper_trades(
        execute_sync_paper_trades=execute_sync_paper_trades,
        get_open_paper_trades=get_open_paper_trades,
        sync_order_by_id=sync_order_by_id,
        sync_order_by_id_for_broker=sync_order_by_id_for_broker,
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
        safe_insert_broker_order=safe_insert_broker_order,
        append_trade_log=append_trade_log,
        safe_insert_trade_event=safe_insert_trade_event,
        upsert_trade_lifecycle=upsert_trade_lifecycle,
        to_float_or_none=to_float_or_none,
        parse_iso_utc=parse_iso_utc,
    )


def close_all_paper_positions(
    *,
    alpaca_broker,
    ibkr_broker,
    shadow_mode_enabled: bool,
    ibkr_bridge_enabled: bool,
    close_all_paper_positions_for_broker_fn: Callable[[Any], Any],
):
    alpaca_result = close_all_paper_positions_for_broker_fn(alpaca_broker)

    if not shadow_mode_enabled or not ibkr_bridge_enabled:
        return alpaca_result

    ibkr_result = close_all_paper_positions_for_broker_fn(ibkr_broker)

    if isinstance(alpaca_result, tuple):
        return alpaca_result

    if isinstance(ibkr_result, tuple):
        alpaca_result["shadow_ibkr_close"] = ibkr_result[0]
        alpaca_result["shadow_ibkr_close_status_code"] = ibkr_result[1]
        return alpaca_result

    aggregated = dict(alpaca_result or {})
    aggregated["shadow_ibkr_close"] = ibkr_result
    aggregated["combined_position_count"] = int(alpaca_result.get("position_count", 0) or 0) + int(ibkr_result.get("position_count", 0) or 0)
    aggregated["combined_closed_count"] = int(alpaca_result.get("closed_count", 0) or 0) + int(ibkr_result.get("closed_count", 0) or 0)
    aggregated["combined_skipped_count"] = int(alpaca_result.get("skipped_count", 0) or 0) + int(ibkr_result.get("skipped_count", 0) or 0)
    return aggregated
