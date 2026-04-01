from __future__ import annotations

from datetime import datetime
from typing import Any, Callable


def _normalize_handler_result(result: Any) -> dict[str, Any]:
    if isinstance(result, tuple) and len(result) == 2:
        body, status_code = result
        return {
            "ok": status_code < 400,
            "status_code": status_code,
            "body": body,
        }
    if isinstance(result, dict):
        return {
            "ok": bool(result.get("ok", True)),
            "status_code": 200,
            "body": result,
        }
    return {
        "ok": True,
        "status_code": 200,
        "body": {"result": result},
    }


def is_weekday(now_ny: datetime) -> bool:
    return now_ny.weekday() < 5


def should_run_market_sync(now_ny: datetime) -> bool:
    if not is_weekday(now_ny):
        return False
    if now_ny.hour == 15 and now_ny.minute == 55:
        return False
    if now_ny.hour == 9:
        return now_ny.minute in {35, 45, 55}
    return 10 <= now_ny.hour <= 15 and now_ny.minute in {5, 15, 25, 35, 45, 55}


def should_run_market_scan(now_ny: datetime) -> bool:
    if not is_weekday(now_ny):
        return False
    if now_ny.hour == 15 and now_ny.minute == 55:
        return False
    if now_ny.hour == 9:
        return now_ny.minute in {35, 45, 55}
    return 10 <= now_ny.hour <= 15 and now_ny.minute in {5, 15, 25, 35, 45, 55}


def should_run_eod_close(now_ny: datetime) -> bool:
    return is_weekday(now_ny) and now_ny.hour == 15 and now_ny.minute == 55


def build_market_ops_plan(now_ny: datetime) -> list[str]:
    if should_run_eod_close(now_ny):
        return ["close"]

    actions: list[str] = []
    if should_run_market_sync(now_ny):
        actions.append("sync")
    if should_run_market_scan(now_ny):
        actions.append("scan")
    return actions


def execute_market_ops(
    *,
    now_ny: datetime,
    run_sync: Callable[[], Any],
    run_scan: Callable[[dict[str, Any]], Any],
    run_close: Callable[[], Any],
) -> dict[str, Any]:
    actions = build_market_ops_plan(now_ny)
    results: dict[str, Any] = {}

    for action in actions:
        if action == "sync":
            results[action] = _normalize_handler_result(run_sync())
        elif action == "scan":
            results[action] = _normalize_handler_result(run_scan({}))
        elif action == "close":
            results[action] = _normalize_handler_result(run_close())

    return {
        "ok": all(item.get("ok", False) for item in results.values()) if results else True,
        "scheduler": "market-ops",
        "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
        "actions": actions,
        "action_count": len(actions),
        "results": results,
        "noop": not actions,
    }


def execute_post_close_ops(
    *,
    now_ny: datetime,
    run_sync: Callable[[], Any],
    run_reconcile: Callable[[], Any],
    run_trade_analysis: Callable[[], Any],
    run_signal_analysis: Callable[[], Any],
    run_snapshot_export: Callable[[], Any],
) -> dict[str, Any]:
    results = {
        "sync": _normalize_handler_result(run_sync()),
        "reconcile": _normalize_handler_result(run_reconcile()),
        "analyze_paper_trades": _normalize_handler_result(run_trade_analysis()),
        "analyze_signals": _normalize_handler_result(run_signal_analysis()),
        "export_daily_snapshot": _normalize_handler_result(run_snapshot_export()),
    }
    return {
        "ok": all(item.get("ok", False) for item in results.values()),
        "scheduler": "daily-post-close",
        "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
        "actions": list(results.keys()),
        "action_count": len(results),
        "results": results,
    }


def execute_maintenance_ops(
    *,
    now_ny: datetime,
    prune_logs: Callable[[int], int],
    retention_days: int = 30,
) -> dict[str, Any]:
    deleted_count = prune_logs(retention_days)
    return {
        "ok": True,
        "scheduler": "maintenance",
        "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
        "actions": ["prune_alpaca_api_logs"],
        "action_count": 1,
        "results": {
            "prune_alpaca_api_logs": {
                "ok": True,
                "status_code": 200,
                "body": {
                    "retention_days": retention_days,
                    "deleted_count": deleted_count,
                },
            }
        },
    }
