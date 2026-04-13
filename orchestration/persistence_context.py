import os
from typing import Any

from core.logging_utils import log_exception
from orchestration.paper_trade_context import (
    find_best_signal_match as context_find_best_signal_match,
    find_latest_open_trade as context_find_latest_open_trade,
    get_current_open_position_state as context_get_current_open_position_state,
    get_latest_open_paper_trade_for_symbol as context_get_latest_open_paper_trade_for_symbol,
    get_latest_paper_close_event_for_symbol as context_get_latest_paper_close_event_for_symbol,
    get_managed_open_paper_trades_for_eod_close as context_get_managed_open_paper_trades_for_eod_close,
    get_open_paper_trades as context_get_open_paper_trades,
    get_risk_exposure_summary as context_get_risk_exposure_summary,
    infer_first_level_hit as context_infer_first_level_hit,
    is_symbol_in_paper_cooldown as context_is_symbol_in_paper_cooldown,
    paper_trade_exit_already_logged as context_paper_trade_exit_already_logged,
    read_trade_rows_for_date as context_read_trade_rows_for_date,
)
from orchestration.scan_context import parse_iso_utc, to_float_or_none
from storage import (
    insert_broker_order,
    insert_paper_trade_attempt,
    insert_reconciliation_detail,
    insert_reconciliation_run,
    insert_scan_run,
    insert_signal_log,
    insert_trade_event,
)


def env_flag(name: str, default: str = "true") -> bool:
    value = str(os.getenv(name, default)).strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


ENABLE_DB_LOGGING = env_flag("ENABLE_DB_LOGGING", "true")


def safe_insert_scan_run(**kwargs) -> None:
    if not ENABLE_DB_LOGGING:
        return
    try:
        insert_scan_run(**kwargs)
    except Exception as exc:
        log_exception("DB scan run write failed", exc, component="persistence_context", operation="insert_scan_run")


def safe_insert_signal_log(**kwargs) -> None:
    if not ENABLE_DB_LOGGING:
        return
    try:
        insert_signal_log(**kwargs)
    except Exception as exc:
        log_exception("DB signal log write failed", exc, component="persistence_context", operation="insert_signal_log")


def safe_insert_trade_event(**kwargs) -> None:
    if not ENABLE_DB_LOGGING:
        return
    try:
        insert_trade_event(**kwargs)
    except Exception as exc:
        log_exception("DB trade event write failed", exc, component="persistence_context", operation="insert_trade_event")


def safe_insert_paper_trade_attempt(**kwargs) -> None:
    if not ENABLE_DB_LOGGING:
        return
    try:
        insert_paper_trade_attempt(**kwargs)
    except Exception as exc:
        log_exception(
            "DB paper trade attempt write failed",
            exc,
            component="persistence_context",
            operation="insert_paper_trade_attempt",
        )


def safe_insert_broker_order(**kwargs) -> None:
    if not ENABLE_DB_LOGGING:
        return
    try:
        insert_broker_order(**kwargs)
    except Exception as exc:
        log_exception("DB broker order write failed", exc, component="persistence_context", operation="insert_broker_order")


def safe_insert_reconciliation_run(**kwargs) -> None:
    if not ENABLE_DB_LOGGING:
        return
    try:
        insert_reconciliation_run(**kwargs)
    except Exception as exc:
        log_exception("DB reconciliation write failed", exc, component="persistence_context", operation="insert_reconciliation_run")


def safe_insert_reconciliation_detail(**kwargs) -> None:
    if not ENABLE_DB_LOGGING:
        return
    try:
        insert_reconciliation_detail(**kwargs)
    except Exception as exc:
        log_exception(
            "DB reconciliation detail write failed",
            exc,
            component="persistence_context",
            operation="insert_reconciliation_detail",
        )


def append_signal_log(row: dict) -> None:
    safe_insert_signal_log(
        timestamp_utc=parse_iso_utc(str(row.get("timestamp_utc", ""))),
        scan_id=str(row.get("scan_id", "")).strip() or None,
        scan_source=str(row.get("scan_source", "")).strip() or None,
        market_phase=str(row.get("market_phase", "")).strip() or None,
        scan_execution_time_ms=int(row.get("scan_execution_time_ms")) if row.get("scan_execution_time_ms") not in (None, "") else None,
        mode=str(row.get("mode", "")).strip() or None,
        account_size=to_float_or_none(row.get("account_size")),
        current_open_positions=int(float(row.get("current_open_positions"))) if row.get("current_open_positions") not in (None, "") else None,
        current_open_exposure=to_float_or_none(row.get("current_open_exposure")),
        timing_ok=bool(row.get("timing_ok")) if row.get("timing_ok") is not None else None,
        source=str(row.get("source", "")).strip() or None,
        trade_count=int(row.get("trade_count")) if row.get("trade_count") not in (None, "") else None,
        top_name=str(row.get("top_name", "")).strip() or None,
        top_symbol=str(row.get("top_symbol", "")).strip().upper() or None,
        current_price=to_float_or_none(row.get("current_price")),
        entry=to_float_or_none(row.get("entry")),
        stop=to_float_or_none(row.get("stop")),
        target=to_float_or_none(row.get("target")),
        shares=to_float_or_none(row.get("shares")),
        confidence=to_float_or_none(row.get("confidence")),
        reason=str(row.get("reason", "")).strip() or None,
        benchmark_sp500=to_float_or_none(row.get("benchmark_sp500")),
        benchmark_nasdaq=to_float_or_none(row.get("benchmark_nasdaq")),
        paper_trade_enabled=bool(row.get("paper_trade_enabled")) if row.get("paper_trade_enabled") is not None else None,
        paper_trade_candidate_count=int(row.get("paper_trade_candidate_count")) if row.get("paper_trade_candidate_count") not in (None, "") else None,
        paper_trade_long_candidate_count=int(row.get("paper_trade_long_candidate_count")) if row.get("paper_trade_long_candidate_count") not in (None, "") else None,
        paper_trade_short_candidate_count=int(row.get("paper_trade_short_candidate_count")) if row.get("paper_trade_short_candidate_count") not in (None, "") else None,
        paper_trade_placed_count=int(row.get("paper_trade_placed_count")) if row.get("paper_trade_placed_count") not in (None, "") else None,
        paper_trade_placed_long_count=int(row.get("paper_trade_placed_long_count")) if row.get("paper_trade_placed_long_count") not in (None, "") else None,
        paper_trade_placed_short_count=int(row.get("paper_trade_placed_short_count")) if row.get("paper_trade_placed_short_count") not in (None, "") else None,
        paper_candidate_symbols=str(row.get("paper_candidate_symbols", "")).strip() or None,
        paper_candidate_confidences=str(row.get("paper_candidate_confidences", "")).strip() or None,
        paper_skipped_symbols=str(row.get("paper_skipped_symbols", "")).strip() or None,
        paper_skip_reasons=str(row.get("paper_skip_reasons", "")).strip() or None,
        paper_placed_symbols=str(row.get("paper_placed_symbols", "")).strip() or None,
        paper_trade_ids=str(row.get("paper_trade_ids", "")).strip() or None,
    )


def append_trade_log(row: dict) -> None:
    del row
    return None


def read_trade_rows_for_date(target_date: str) -> list[dict]:
    return context_read_trade_rows_for_date(target_date)


def paper_trade_exit_already_logged(parent_order_id: str, exit_event: str) -> bool:
    return context_paper_trade_exit_already_logged(parent_order_id, exit_event)


def get_open_paper_trades() -> list[dict]:
    return context_get_open_paper_trades()


def get_managed_open_paper_trades_for_eod_close() -> list[dict]:
    return context_get_managed_open_paper_trades_for_eod_close()


def get_managed_open_paper_trades_for_eod_close_for_broker(broker) -> list[dict]:
    return context_get_managed_open_paper_trades_for_eod_close(broker=broker)


def get_current_open_position_state() -> tuple[int, float]:
    return context_get_current_open_position_state()


def get_risk_exposure_summary() -> dict:
    return context_get_risk_exposure_summary()


def get_latest_open_paper_trade_for_symbol(symbol: str) -> dict | None:
    return context_get_latest_open_paper_trade_for_symbol(symbol)


def get_latest_paper_close_event_for_symbol(symbol: str) -> dict | None:
    return context_get_latest_paper_close_event_for_symbol(symbol)


def is_symbol_in_paper_cooldown(symbol: str, now_utc: str) -> tuple[bool, str]:
    return context_is_symbol_in_paper_cooldown(symbol, now_utc)


def find_best_signal_match(symbol: str, actual_entry_price: float | None, open_timestamp_utc: str) -> dict | None:
    return context_find_best_signal_match(symbol, actual_entry_price, open_timestamp_utc)


def find_latest_open_trade(symbol: str, trade_source: str | None = None, broker_parent_order_id: str | None = None) -> dict | None:
    return context_find_latest_open_trade(symbol, trade_source=trade_source, broker_parent_order_id=broker_parent_order_id)


def infer_first_level_hit(open_row: dict, close_timestamp_utc: str) -> dict:
    return context_infer_first_level_hit(open_row, close_timestamp_utc)
