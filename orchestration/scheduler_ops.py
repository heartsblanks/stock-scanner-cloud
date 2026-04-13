from __future__ import annotations

from datetime import datetime
from typing import Any, Callable


def _normalize_handler_result(result: Any) -> dict[str, Any]:
    if isinstance(result, tuple) and len(result) == 2:
        body, status_code = result
        if isinstance(status_code, int):
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
    run_ibkr_stale_close_repair: Callable[[], Any] | None,
    run_reconcile: Callable[[], Any],
    run_trade_analysis: Callable[[], Any],
    run_signal_analysis: Callable[[], Any],
    run_snapshot_export: Callable[[], Any],
    run_mode_ranking_refresh: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    results = {
        "sync": _normalize_handler_result(run_sync()),
    }
    if run_ibkr_stale_close_repair is not None:
        results["repair_ibkr_stale_closes"] = _normalize_handler_result(run_ibkr_stale_close_repair())
    results["reconcile"] = _normalize_handler_result(run_reconcile())
    results["analyze_paper_trades"] = _normalize_handler_result(run_trade_analysis())
    results["analyze_signals"] = _normalize_handler_result(run_signal_analysis())
    results["export_daily_snapshot"] = _normalize_handler_result(run_snapshot_export())
    if run_mode_ranking_refresh is not None:
        results["refresh_mode_rankings"] = _normalize_handler_result(run_mode_ranking_refresh())
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


def execute_ibkr_vm_control(
    *,
    now_ny: datetime,
    action: str,
    is_trading_day: bool,
    holiday_message: str | None,
    get_instance_status: Callable[[], str | None],
    start_instance: Callable[[], Any],
    stop_instance: Callable[[], Any],
    force: bool = False,
) -> dict[str, Any]:
    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {"start", "stop"}:
        raise ValueError("action must be 'start' or 'stop'")

    instance_status = get_instance_status() or "UNKNOWN"

    if normalized_action == "start" and not force and not is_trading_day:
        return {
            "ok": True,
            "scheduler": "ibkr-vm-control",
            "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
            "action": normalized_action,
            "results": {},
            "instance_status_before": instance_status,
            "instance_status_after": instance_status,
            "noop": True,
            "skipped": True,
            "reason": holiday_message or "NYSE market is closed today.",
        }

    if normalized_action == "start" and instance_status == "RUNNING":
        return {
            "ok": True,
            "scheduler": "ibkr-vm-control",
            "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
            "action": normalized_action,
            "results": {},
            "instance_status_before": instance_status,
            "instance_status_after": instance_status,
            "noop": True,
            "skipped": False,
            "reason": "IBKR VM is already running.",
        }

    if normalized_action == "stop" and instance_status in {"TERMINATED", "STOPPED"}:
        return {
            "ok": True,
            "scheduler": "ibkr-vm-control",
            "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
            "action": normalized_action,
            "results": {},
            "instance_status_before": instance_status,
            "instance_status_after": instance_status,
            "noop": True,
            "skipped": False,
            "reason": "IBKR VM is already stopped.",
        }

    operation_result = start_instance() if normalized_action == "start" else stop_instance()
    normalized_result = _normalize_handler_result(operation_result)

    return {
        "ok": normalized_result.get("ok", False),
        "scheduler": "ibkr-vm-control",
        "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
        "action": normalized_action,
        "results": {normalized_action: normalized_result},
        "instance_status_before": instance_status,
        "instance_status_after": "STARTING" if normalized_action == "start" else "STOPPING",
        "noop": False,
        "skipped": False,
        "force": force,
    }


def execute_ibkr_login_alert(
    *,
    now_ny: datetime,
    get_ibkr_operational_status: Callable[[], dict[str, Any]],
    telegram_alerts_enabled: bool,
    send_telegram_alert: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    if not telegram_alerts_enabled:
        return {
            "ok": True,
            "scheduler": "ibkr-login-alert",
            "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
            "noop": True,
            "reason": "telegram_alerts_disabled",
        }

    if not is_weekday(now_ny):
        return {
            "ok": True,
            "scheduler": "ibkr-login-alert",
            "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
            "noop": True,
            "reason": "outside_trading_day",
        }

    total_minutes = (now_ny.hour * 60) + now_ny.minute
    if total_minutes < (9 * 60) or total_minutes > ((16 * 60) + 5):
        return {
            "ok": True,
            "scheduler": "ibkr-login-alert",
            "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
            "noop": True,
            "reason": "outside_alert_window",
        }

    status = get_ibkr_operational_status() or {}
    if not bool(status.get("enabled")):
        return {
            "ok": True,
            "scheduler": "ibkr-login-alert",
            "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
            "noop": True,
            "reason": "ibkr_disabled",
            "ibkr_status": status,
        }

    if not bool(status.get("login_required")):
        return {
            "ok": True,
            "scheduler": "ibkr-login-alert",
            "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
            "noop": True,
            "reason": "login_not_required",
            "ibkr_status": status,
        }

    state = str(status.get("state", "")).strip().upper() or "LOGIN_REQUIRED"
    message = (
        f"IBKR login required. "
        f"State={state}. "
        f"Cloud Run cannot use the Gateway until you log in."
    )
    alert_result = send_telegram_alert(
        alert_key="ibkr-login-required",
        message=message,
        payload={
            "state": state,
            "account_ok": bool(status.get("account_ok")),
            "bridge_health_ok": bool(status.get("bridge_health_ok")),
            "market_data_ok": bool(status.get("market_data_ok")),
            "position_count": int(status.get("position_count", 0) or 0),
            "errors": list(status.get("errors") or []),
        },
    )
    return {
        "ok": bool(alert_result.get("ok", False)),
        "scheduler": "ibkr-login-alert",
        "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
        "noop": False,
        "reason": "login_required_alert_attempted",
        "ibkr_status": status,
        "alert_result": alert_result,
    }
