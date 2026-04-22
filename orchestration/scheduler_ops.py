from __future__ import annotations

import os
import time
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


def _run_action_safely(action_name: str, handler: Callable[[], Any]) -> dict[str, Any]:
    try:
        return _normalize_handler_result(handler())
    except Exception as exc:
        return {
            "ok": False,
            "status_code": 500,
            "body": {
                "ok": False,
                "action": action_name,
                "error": str(exc),
            },
        }


def is_weekday(now_ny: datetime) -> bool:
    return now_ny.weekday() < 5


def _low_call_mode_enabled() -> bool:
    raw = str(os.getenv("IBKR_LOW_CALL_MODE", "false")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def should_run_market_sync(now_ny: datetime) -> bool:
    if not is_weekday(now_ny):
        return False
    if now_ny.hour == 15 and now_ny.minute == 55:
        return False
    if should_run_market_scan(now_ny):
        return False
    if _low_call_mode_enabled():
        # Low-call mode: keep sync on a 30-minute cadence.
        if now_ny.hour == 9:
            return now_ny.minute == 30
        if now_ny.hour == 15:
            return now_ny.minute in {0, 30, 50}
        return 10 <= now_ny.hour <= 14 and now_ny.minute in {0, 30}
    if now_ny.hour == 9:
        return now_ny.minute in {30, 35, 40, 50, 55}
    if now_ny.hour == 15:
        return now_ny.minute in {0, 10, 20, 30, 40, 50}
    return 10 <= now_ny.hour <= 14 and now_ny.minute in {0, 10, 20, 30, 40, 50}


def should_run_market_scan(now_ny: datetime) -> bool:
    if not is_weekday(now_ny):
        return False
    if now_ny.hour == 15 and now_ny.minute in {50, 55}:
        return False
    if now_ny.hour == 9:
        return now_ny.minute in {45, 55}
    if now_ny.hour == 15:
        return now_ny.minute in {5, 15, 25, 35, 45}
    return 10 <= now_ny.hour <= 14 and now_ny.minute in {5, 15, 25, 35, 45, 55}


def should_run_periodic_health_probe(now_ny: datetime) -> bool:
    if _low_call_mode_enabled():
        # Low-call mode: disable periodic health probes.
        # Health is still captured via pre-close prep and health_on_failure.
        return False
    if not is_weekday(now_ny):
        return False
    if now_ny.hour == 9:
        return now_ny.minute == 30
    if 10 <= now_ny.hour <= 15:
        return now_ny.minute in {0, 30}
    return False


def should_run_eod_close(now_ny: datetime) -> bool:
    return is_weekday(now_ny) and now_ny.hour == 15 and now_ny.minute == 55


def should_run_pre_close_prep(now_ny: datetime) -> bool:
    return is_weekday(now_ny) and now_ny.hour == 15 and now_ny.minute == 50


def build_market_ops_plan(now_ny: datetime) -> list[str]:
    if should_run_eod_close(now_ny):
        return ["close"]
    if should_run_pre_close_prep(now_ny):
        return ["sync", "health", "pre_close_prep"]

    actions: list[str] = []
    if should_run_market_scan(now_ny):
        actions.append("scan")
    elif should_run_market_sync(now_ny):
        actions.append("sync")
    if should_run_periodic_health_probe(now_ny):
        actions.append("health")
    if actions and actions[0] == "scan" and "sync" in actions:
        actions = [action for action in actions if action != "sync"]
    if actions and actions[0] == "sync" and "scan" in actions:
        actions = [action for action in actions if action != "scan"]
    deduped_actions: list[str] = []
    for action in actions:
        if action not in deduped_actions:
            deduped_actions.append(action)
    return deduped_actions


def execute_market_ops(
    *,
    now_ny: datetime,
    run_sync: Callable[[], Any],
    run_scan: Callable[[dict[str, Any]], Any],
    run_close: Callable[[], Any],
    run_pre_close_prep: Callable[[], Any] | None = None,
    run_health_probe: Callable[[], Any] | None = None,
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
        elif action == "health" and run_health_probe is not None:
            results[action] = _normalize_handler_result(run_health_probe())
        elif action == "pre_close_prep" and run_pre_close_prep is not None:
            results[action] = _normalize_handler_result(run_pre_close_prep())

    primary_action = next((action for action in actions if action in {"sync", "scan"}), None)
    if (
        primary_action is not None
        and run_health_probe is not None
        and not bool(results.get(primary_action, {}).get("ok", False))
        and "health" not in results
    ):
        results["health_on_failure"] = _normalize_handler_result(run_health_probe())

    return {
        "ok": all(item.get("ok", False) for item in results.values()) if results else True,
        "scheduler": "market-ops",
        "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
        "actions": actions,
        "action_count": len(actions),
        "results": results,
        "noop": not actions,
    }


def execute_pre_close_prep(
    *,
    now_ny: datetime,
    get_ibkr_operational_status: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    status = get_ibkr_operational_status() or {}
    return {
        "ok": bool(status.get("ok", False)),
        "scheduler": "pre-close-prep",
        "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
        "ready_for_close": bool(status.get("enabled")) and not bool(status.get("login_required")) and str(status.get("state", "")).strip().upper() == "READY",
        "ibkr_status": status,
    }


def execute_post_close_ops(
    *,
    now_ny: datetime,
    run_sync: Callable[[], Any],
    run_symbol_eligibility_refresh: Callable[[], Any] | None = None,
    run_ibkr_stale_close_repair: Callable[[], Any] | None,
    run_reconcile: Callable[[], Any],
    run_trade_analysis: Callable[[], Any],
    run_signal_analysis: Callable[[], Any],
    run_mode_ranking_refresh: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    results = {
        "sync": _run_action_safely("sync", run_sync),
    }
    if run_symbol_eligibility_refresh is not None:
        results["refresh_symbol_eligibility"] = _run_action_safely(
            "refresh_symbol_eligibility",
            run_symbol_eligibility_refresh,
        )
    if run_ibkr_stale_close_repair is not None:
        results["repair_ibkr_stale_closes"] = _run_action_safely(
            "repair_ibkr_stale_closes",
            run_ibkr_stale_close_repair,
        )
    results["reconcile"] = _run_action_safely("reconcile", run_reconcile)
    results["analyze_paper_trades"] = _run_action_safely("analyze_paper_trades", run_trade_analysis)
    results["analyze_signals"] = _run_action_safely("analyze_signals", run_signal_analysis)
    if run_mode_ranking_refresh is not None:
        results["refresh_mode_rankings"] = _run_action_safely("refresh_mode_rankings", run_mode_ranking_refresh)
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
    prune_operational_data: Callable[[dict[str, int]], dict[str, dict[str, Any]]] | None = None,
    retention_days: int = 30,
) -> dict[str, Any]:
    deleted_count = prune_logs(retention_days)
    operational_results = (
        prune_operational_data(
            {
                "signal_logs": 45,
                "scan_runs": 45,
                "paper_trade_attempts": 120,
                "broker_orders": 120,
                "reconciliation_details": 120,
                "reconciliation_runs": 120,
            }
        )
        if prune_operational_data is not None
        else {}
    )

    results = {
        "prune_operational_logs": {
            "ok": True,
            "status_code": 200,
            "body": {
                "retention_days": retention_days,
                "deleted_count": deleted_count,
            },
        }
    }
    for table_name, table_result in operational_results.items():
        results[f"prune_{table_name}"] = {
            "ok": True,
            "status_code": 200,
            "body": table_result,
        }

    return {
        "ok": True,
        "scheduler": "maintenance",
        "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
        "actions": list(results.keys()),
        "action_count": len(results),
        "results": results,
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

    if _low_call_mode_enabled():
        try:
            check_interval_minutes = max(1, int(os.getenv("IBKR_LOGIN_ALERT_CHECK_INTERVAL_MINUTES", "30")))
        except Exception:
            check_interval_minutes = 30
        if now_ny.minute % check_interval_minutes != 0:
            return {
                "ok": True,
                "scheduler": "ibkr-login-alert",
                "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
                "noop": True,
                "reason": "check_interval_gate",
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
    if state != "LOGIN_REQUIRED":
        return {
            "ok": True,
            "scheduler": "ibkr-login-alert",
            "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
            "noop": True,
            "reason": "state_not_login_required",
            "ibkr_status": status,
        }
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


def execute_test_day_cycle(
    *,
    now_ny: datetime,
    payload: dict[str, Any] | None,
    run_scan: Callable[[dict[str, Any]], Any],
    run_sync: Callable[[], Any],
    run_close: Callable[[], Any],
    run_post_close_ops: Callable[[], Any] | None = None,
    sleep_fn: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    def _to_bool(value: Any, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"1", "true", "yes", "y", "on"}:
                return True
            if lowered in {"0", "false", "no", "n", "off"}:
                return False
        return bool(value)

    def _to_int(value: Any, default: int, *, minimum: int = 0) -> int:
        try:
            return max(minimum, int(value))
        except Exception:
            return max(minimum, default)

    def _to_float(value: Any, default: float, *, minimum: float = 0.0) -> float:
        try:
            return max(minimum, float(value))
        except Exception:
            return max(minimum, default)

    request_payload = payload or {}
    raw_modes = request_payload.get("modes")
    if isinstance(raw_modes, str):
        modes = [token.strip().lower() for token in raw_modes.split(",") if token.strip()]
    elif isinstance(raw_modes, list):
        modes = [str(token).strip().lower() for token in raw_modes if str(token).strip()]
    else:
        default_mode = str(request_payload.get("mode", "us_test")).strip().lower() or "us_test"
        modes = [default_mode]

    valid_modes = {
        "primary",
        "secondary",
        "third",
        "fourth",
        "fifth",
        "sixth",
        "us_test",
        "europe_test",
        "core_one",
        "core_two",
        "core_three",
    }
    invalid_modes = [mode for mode in modes if mode not in valid_modes]
    if invalid_modes:
        return {
            "ok": False,
            "scheduler": "test-day-cycle",
            "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
            "error": "invalid_modes",
            "invalid_modes": invalid_modes,
            "valid_modes": sorted(valid_modes),
        }

    scan_rounds = _to_int(request_payload.get("scan_rounds", 1), 1, minimum=1)
    scan_interval_seconds = _to_float(request_payload.get("scan_interval_seconds", 0), 0.0, minimum=0.0)
    run_initial_sync = _to_bool(request_payload.get("run_initial_sync", True), True)
    sync_after_each_scan = _to_bool(request_payload.get("sync_after_each_scan", True), True)
    run_eod_close = _to_bool(request_payload.get("run_eod_close", True), True)
    run_post_close = _to_bool(request_payload.get("run_post_close", True), True)
    paper_trade = _to_bool(request_payload.get("paper_trade", True), True)
    ignore_market_hours = _to_bool(request_payload.get("ignore_market_hours", True), True)
    debug = _to_bool(request_payload.get("debug", False), False)
    disable_strategy_gates = _to_bool(request_payload.get("disable_strategy_gates", False), False)
    # Use MANUAL source so each requested mode is honored in the test cycle.
    # SCHEDULED source fans out to the full ranked mode set inside app_runtime.
    scan_source = "MANUAL"
    scan_payload_overrides = request_payload.get("scan_payload") if isinstance(request_payload.get("scan_payload"), dict) else {}

    actions: list[str] = []
    results: dict[str, Any] = {}
    scan_plan: list[dict[str, Any]] = []
    started_monotonic = time.monotonic()

    def _record_action(action_name: str, action_result: Any) -> None:
        actions.append(action_name)
        results[action_name] = _normalize_handler_result(action_result)

    if run_initial_sync:
        _record_action("initial_sync", run_sync())

    total_scan_steps = scan_rounds * len(modes)
    current_scan_step = 0
    for round_index in range(scan_rounds):
        for mode in modes:
            current_scan_step += 1
            scan_payload: dict[str, Any] = {
                "mode": mode,
                "paper_trade": paper_trade,
                "scan_source": scan_source,
                "ignore_market_hours": ignore_market_hours,
                "debug": debug,
                "disable_strategy_gates": disable_strategy_gates,
            }
            scan_payload.update(scan_payload_overrides)
            scan_payload["mode"] = mode
            scan_payload["paper_trade"] = paper_trade
            scan_payload["scan_source"] = scan_source
            scan_payload["ignore_market_hours"] = ignore_market_hours
            scan_payload["debug"] = debug
            scan_payload["disable_strategy_gates"] = disable_strategy_gates

            scan_action = f"scan_r{round_index + 1}_{mode}"
            _record_action(scan_action, run_scan(scan_payload))
            scan_plan.append(
                {
                    "round": round_index + 1,
                    "mode": mode,
                    "scan_action": scan_action,
                }
            )

            if sync_after_each_scan:
                _record_action(f"sync_r{round_index + 1}_{mode}", run_sync())

            should_sleep = (
                scan_interval_seconds > 0
                and sleep_fn is not None
                and current_scan_step < total_scan_steps
            )
            if should_sleep:
                sleep_fn(scan_interval_seconds)

    if run_eod_close:
        _record_action("eod_close", run_close())

    _record_action("final_sync", run_sync())

    if run_post_close and run_post_close_ops is not None:
        _record_action("post_close", run_post_close_ops())

    elapsed_seconds = round(max(0.0, time.monotonic() - started_monotonic), 3)
    return {
        "ok": all(item.get("ok", False) for item in results.values()) if results else True,
        "scheduler": "test-day-cycle",
        "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
        "actions": actions,
        "action_count": len(actions),
        "scan_plan": scan_plan,
        "scan_rounds": scan_rounds,
        "modes": modes,
        "scan_interval_seconds": scan_interval_seconds,
        "scan_source": scan_source,
        "paper_trade": paper_trade,
        "ignore_market_hours": ignore_market_hours,
        "disable_strategy_gates": disable_strategy_gates,
        "elapsed_seconds": elapsed_seconds,
        "results": results,
    }
