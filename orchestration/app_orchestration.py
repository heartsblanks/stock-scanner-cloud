from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from core.logging_utils import log_exception


def handle_sync_paper_trades(
    *,
    execute_sync_paper_trades: Callable[..., Any],
    get_open_paper_trades: Callable[[], list[dict[str, Any]]],
    sync_order_by_id: Callable[[str], dict[str, Any]],
    paper_trade_exit_already_logged: Callable[[str, str], bool],
    append_trade_log: Callable[[dict[str, Any]], None],
    safe_insert_trade_event: Callable[..., None],
    safe_insert_broker_order: Callable[..., None],
    parse_iso_utc: Callable[[str], Any],
    to_float_or_none: Callable[[Any], float | None],
    upsert_trade_lifecycle: Callable[..., None],
    get_open_positions: Callable[[], list[dict[str, Any]]],
    close_position: Callable[[str], Any],
):
    return execute_sync_paper_trades(
        get_open_paper_trades=get_open_paper_trades,
        sync_order_by_id=sync_order_by_id,
        paper_trade_exit_already_logged=paper_trade_exit_already_logged,
        append_trade_log=append_trade_log,
        safe_insert_trade_event=safe_insert_trade_event,
        safe_insert_broker_order=safe_insert_broker_order,
        parse_iso_utc=parse_iso_utc,
        to_float_or_none=to_float_or_none,
        upsert_trade_lifecycle=upsert_trade_lifecycle,
        get_open_positions=get_open_positions,
        close_position=close_position,
    )


def handle_scan_request(
    payload: dict[str, Any] | None,
    *,
    get_current_open_position_state: Callable[[], tuple[int, float]],
    get_risk_exposure_summary: Callable[[], dict[str, Any]],
    execute_full_scan: Callable[..., Any],
    market_time_check: Callable[..., Any],
    build_scan_id: Callable[[str, str], str],
    market_phase_from_timestamp: Callable[[str], str],
    append_signal_log: Callable[[dict[str, Any]], None],
    safe_insert_paper_trade_attempt: Callable[..., None],
    safe_insert_scan_run: Callable[..., None],
    parse_iso_utc: Callable[[str], Any],
    run_scan: Callable[..., Any],
    trade_to_dict: Callable[[Any], dict[str, Any]],
    debug_to_dict: Callable[[Any], dict[str, Any]],
    paper_candidate_from_evaluation: Callable[[Any], Any],
    evaluate_symbol: Callable[..., Any],
    get_latest_open_paper_trade_for_symbol: Callable[[str], dict[str, Any] | None],
    is_symbol_in_paper_cooldown: Callable[[str, str], tuple[bool, str]],
    place_paper_orders_from_trade: Callable[..., Any],
    append_trade_log: Callable[[dict[str, Any]], None],
    safe_insert_trade_event: Callable[..., None],
    safe_insert_broker_order: Callable[..., None],
    to_float_or_none: Callable[[Any], float | None],
    min_confidence: float,
    upsert_trade_lifecycle: Callable[..., None],
):
    scan_payload = dict(payload or {})
    if scan_payload.get("paper_trade"):
        if "current_open_positions" not in scan_payload or "current_open_exposure" not in scan_payload:
            current_open_positions, current_open_exposure = get_current_open_position_state()
            scan_payload.setdefault("current_open_positions", current_open_positions)
            scan_payload.setdefault("current_open_exposure", current_open_exposure)

        try:
            risk_summary = get_risk_exposure_summary()
            scan_payload.setdefault("daily_realized_pnl", risk_summary.get("daily_realized_pnl", 0.0))
            scan_payload.setdefault("daily_unrealized_pnl", risk_summary.get("daily_unrealized_pnl", 0.0))
        except Exception as exc:
            log_exception("Failed to inject risk summary into scan payload", exc, component="app_orchestration", operation="handle_scan_request")
            scan_payload.setdefault("daily_realized_pnl", 0.0)
            scan_payload.setdefault("daily_unrealized_pnl", 0.0)

    return execute_full_scan(
        scan_payload,
        market_time_check=market_time_check,
        build_scan_id=build_scan_id,
        market_phase_from_timestamp=market_phase_from_timestamp,
        append_signal_log=append_signal_log,
        safe_insert_paper_trade_attempt=safe_insert_paper_trade_attempt,
        safe_insert_scan_run=safe_insert_scan_run,
        parse_iso_utc=parse_iso_utc,
        run_scan=run_scan,
        trade_to_dict=trade_to_dict,
        debug_to_dict=debug_to_dict,
        paper_candidate_from_evaluation=paper_candidate_from_evaluation,
        evaluate_symbol=evaluate_symbol,
        get_latest_open_paper_trade_for_symbol=get_latest_open_paper_trade_for_symbol,
        is_symbol_in_paper_cooldown=is_symbol_in_paper_cooldown,
        place_paper_orders_from_trade=place_paper_orders_from_trade,
        append_trade_log=append_trade_log,
        safe_insert_trade_event=safe_insert_trade_event,
        safe_insert_broker_order=safe_insert_broker_order,
        to_float_or_none=to_float_or_none,
        MIN_CONFIDENCE=min_confidence,
        upsert_trade_lifecycle=upsert_trade_lifecycle,
    )


def run_scheduled_paper_scan_wrapper(
    payload: dict[str, Any] | None,
    *,
    now_ny: datetime,
    build_scheduled_scan_payload: Callable[[dict[str, Any], datetime | None], dict[str, Any]],
    handle_scan_request_fn: Callable[[dict[str, Any]], Any],
):
    try:
        scheduled_payload = build_scheduled_scan_payload(payload or {}, now_ny=now_ny)
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
        }, 400

    return handle_scan_request_fn(scheduled_payload)


def close_all_paper_positions(
    *,
    execute_close_all_paper_positions: Callable[..., Any],
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
):
    result = execute_close_all_paper_positions(
        get_open_positions=get_open_positions,
        get_managed_open_paper_trades_for_eod_close=get_managed_open_paper_trades_for_eod_close,
        cancel_open_orders_for_symbol=cancel_open_orders_for_symbol,
        close_position=close_position,
        get_order_by_id=get_order_by_id,
        safe_insert_broker_order=safe_insert_broker_order,
        append_trade_log=append_trade_log,
        safe_insert_trade_event=safe_insert_trade_event,
        upsert_trade_lifecycle=upsert_trade_lifecycle,
        to_float_or_none=to_float_or_none,
        parse_iso_utc=parse_iso_utc,
    )
    if isinstance(result, tuple):
        body, status_code = result
        raise RuntimeError(body.get("error", f"close_all_paper_positions failed with status {status_code}"))
    return result


def build_reconcile_now_response(
    *,
    run_reconciliation: Callable[[], dict[str, Any]],
    upload_file_to_gcs: Callable[..., Any],
    reconciliation_bucket: str,
    reconciliation_object: str,
    safe_insert_reconciliation_run: Callable[..., None],
):
    result = run_reconciliation()

    try:
        upload_file_to_gcs(
            bucket_name=reconciliation_bucket,
            source_file_path=result.get("file_path"),
            destination_blob_name=reconciliation_object,
        )
    except Exception as upload_err:
        from core.logging_utils import log_warning

        log_warning("GCS upload failed", component="app_orchestration", operation="reconcile_now", error=str(upload_err))

    try:
        mismatch_count = int(result.get("mismatch_count", 0) or 0)
        total_rows = int(result.get("total_rows", 0) or 0)
        matched_count = max(total_rows - mismatch_count, 0)
        now_utc = datetime.now(timezone.utc)
        safe_insert_reconciliation_run(
            run_time=now_utc,
            matched_count=matched_count,
            unmatched_count=mismatch_count,
            mismatch_count=mismatch_count,
            severity=result.get("severity"),
            run_started_at=now_utc,
            run_completed_at=now_utc,
            notes=f"file_path={result.get('file_path')}",
        )
    except Exception as db_err:
        log_exception("DB reconciliation summary insert failed", db_err, component="app_orchestration", operation="reconcile_now")

    return {
        "ok": True,
        "message": "Reconciliation completed",
        "result": result,
    }


def build_reconciliation_runs_response(
    *,
    limit: int,
    get_reconciliation_runs: Callable[[int], list[dict[str, Any]]],
) -> dict[str, Any]:
    rows = get_reconciliation_runs(limit=limit)
    normalized_rows = []
    for row in rows:
        mismatch_count = row.get("mismatch_count")
        if mismatch_count is None:
            mismatch_count = row.get("unmatched_count") or 0

        severity = row.get("severity")
        if not severity:
            if mismatch_count == 0:
                severity = "OK"
            elif mismatch_count <= 5:
                severity = "WARNING"
            else:
                severity = "CRITICAL"

        normalized_rows.append({
            **row,
            "mismatch_count": mismatch_count,
            "severity": severity,
        })

    return {
        "ok": True,
        "rows": normalized_rows,
        "count": len(normalized_rows),
        "limit": limit,
    }
