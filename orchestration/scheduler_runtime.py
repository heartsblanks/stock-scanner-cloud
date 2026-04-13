import os
from datetime import datetime
from typing import Any, Callable

import requests


def build_ibkr_operational_status(
    *,
    ibkr_bridge_enabled: Callable[[], bool],
    ibkr_bridge_get: Callable[..., Any],
    account_equity_from_broker_account: Callable[[dict[str, Any] | None], float],
) -> dict[str, Any]:
    status: dict[str, Any] = {
        "ok": True,
        "enabled": ibkr_bridge_enabled(),
        "state": "DISABLED",
        "bridge_health_ok": False,
        "account_ok": False,
        "session_probe_ok": False,
        "market_data_ok": False,
        "login_required": False,
        "message": "",
        "bridge": None,
        "account_id": "",
        "equity": None,
        "market_data_symbol": os.getenv("IBKR_READINESS_SYMBOL", "SPY").strip().upper() or "SPY",
        "market_data_count": 0,
        "position_count": 0,
        "errors": [],
    }

    if not status["enabled"]:
        status["message"] = "IBKR bridge is not configured."
        return status

    bridge_timeout = int(os.getenv("IBKR_BRIDGE_HEALTH_TIMEOUT_SECONDS", "4"))
    account_timeout = int(os.getenv("IBKR_BRIDGE_ACCOUNT_TIMEOUT_SECONDS", "5"))
    positions_timeout = int(os.getenv("IBKR_BRIDGE_POSITIONS_TIMEOUT_SECONDS", "8"))
    market_timeout = int(os.getenv("IBKR_BRIDGE_STATUS_MARKET_DATA_TIMEOUT_SECONDS", "8"))

    try:
        bridge_payload = ibkr_bridge_get("/health", timeout=bridge_timeout) or {}
        status["bridge_health_ok"] = bool(bridge_payload.get("ok"))
        status["bridge"] = bridge_payload.get("ibkr")
    except Exception as exc:
        status["errors"].append(f"bridge_health: {exc}")
        status["state"] = "UNAVAILABLE"
        status["message"] = "IBKR bridge is not reachable."
        return status

    try:
        account_payload = ibkr_bridge_get("/account", timeout=account_timeout) or {}
        equity = account_equity_from_broker_account(account_payload)
        status["account_ok"] = bool(account_payload.get("account_id"))
        status["account_id"] = str(account_payload.get("account_id", "") or "")
        status["equity"] = equity if equity > 0 else None
    except Exception as exc:
        status["errors"].append(f"account: {exc}")

    try:
        positions_payload = ibkr_bridge_get("/positions", timeout=positions_timeout) or []
        status["session_probe_ok"] = True
        status["position_count"] = len(positions_payload)
    except Exception as exc:
        status["errors"].append(f"positions: {exc}")

    try:
        candles = ibkr_bridge_get(
            "/market-data/intraday",
            params={"symbol": status["market_data_symbol"], "interval": "1min", "outputsize": 5},
            timeout=market_timeout,
        ) or []
        status["market_data_count"] = len(candles)
        status["market_data_ok"] = len(candles) > 0
    except Exception as exc:
        status["errors"].append(f"market_data: {exc}")

    session_ready = status["account_ok"] or status["session_probe_ok"]

    if status["bridge_health_ok"] and session_ready and status["market_data_ok"]:
        status["state"] = "READY"
        if status["account_ok"]:
            status["message"] = "IBKR bridge, account, and market data checks passed."
        else:
            status["message"] = "IBKR bridge session and market data checks passed; account summary is slow or unavailable."
        return status

    status["login_required"] = not status["session_probe_ok"]
    if status["bridge_health_ok"] and not session_ready:
        status["state"] = "LOGIN_REQUIRED"
        status["message"] = "IBKR bridge is up, but the account session is not ready."
    elif status["bridge_health_ok"] and session_ready and not status["market_data_ok"]:
        status["state"] = "MARKET_DATA_UNAVAILABLE"
        status["message"] = "IBKR account is up, but market data is not ready."
    else:
        status["state"] = "DEGRADED"
        status["message"] = "IBKR bridge is partially available but not ready for scans."
    return status


def metadata_access_token() -> str:
    response = requests.get(
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
        headers={"Metadata-Flavor": "Google"},
        timeout=10,
    )
    response.raise_for_status()
    payload = response.json()
    token = str(payload.get("access_token", "")).strip()
    if not token:
        raise RuntimeError("Could not resolve GCP access token from metadata server.")
    return token


def ibkr_vm_settings() -> tuple[str, str, str]:
    project = (
        str(os.getenv("IBKR_VM_PROJECT", "")).strip()
        or str(os.getenv("GOOGLE_CLOUD_PROJECT", "")).strip()
        or str(os.getenv("GCP_PROJECT", "")).strip()
        or "stock-scanner-490821"
    )
    zone = str(os.getenv("IBKR_VM_ZONE", "europe-west1-b")).strip() or "europe-west1-b"
    instance = str(os.getenv("IBKR_VM_INSTANCE_NAME", "ibkr-bridge-vm")).strip() or "ibkr-bridge-vm"
    return project, zone, instance


def ibkr_vm_compute_api_request(
    method: str,
    suffix: str,
    *,
    metadata_access_token_fn: Callable[[], str] = metadata_access_token,
    ibkr_vm_settings_fn: Callable[[], tuple[str, str, str]] = ibkr_vm_settings,
    requests_module=requests,
) -> dict:
    access_token = metadata_access_token_fn()
    project, zone, instance = ibkr_vm_settings_fn()
    url = (
        "https://compute.googleapis.com/compute/v1/projects/"
        f"{project}/zones/{zone}/instances/{instance}{suffix}"
    )
    response = requests_module.request(
        method,
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=20,
    )
    response.raise_for_status()
    if not response.content:
        return {}
    return response.json()


def get_ibkr_vm_status(*, compute_api_request_fn: Callable[[str, str], dict] = ibkr_vm_compute_api_request) -> str | None:
    payload = compute_api_request_fn("GET", "")
    return str(payload.get("status", "")).strip().upper() or None


def start_ibkr_vm(
    *,
    log_info: Callable[..., None],
    compute_api_request_fn: Callable[[str, str], dict] = ibkr_vm_compute_api_request,
) -> dict:
    payload = compute_api_request_fn("POST", "/start")
    log_info("requested ibkr vm start", component="scheduler", operation="ibkr-vm-control")
    return payload


def stop_ibkr_vm(
    *,
    log_info: Callable[..., None],
    compute_api_request_fn: Callable[[str, str], dict] = ibkr_vm_compute_api_request,
) -> dict:
    payload = compute_api_request_fn("POST", "/stop")
    log_info("requested ibkr vm stop", component="scheduler", operation="ibkr-vm-control")
    return payload


def run_ibkr_vm_control_scheduler(
    *,
    now_ny: datetime,
    action: str,
    force: bool,
    holiday_and_early_close_status: Callable[[datetime], tuple[Any, Any, Any, Any, Any]],
    execute_ibkr_vm_control: Callable[..., dict[str, Any]],
    get_instance_status: Callable[[], str | None],
    start_instance: Callable[[], dict],
    stop_instance: Callable[[], dict],
) -> dict[str, Any]:
    is_trading_day, _is_early_close, _market_open_ny, _market_close_ny, holiday_message = holiday_and_early_close_status(now_ny)
    return execute_ibkr_vm_control(
        now_ny=now_ny,
        action=action,
        force=force,
        is_trading_day=is_trading_day,
        holiday_message=holiday_message,
        get_instance_status=get_instance_status,
        start_instance=start_instance,
        stop_instance=stop_instance,
    )


def run_ibkr_login_alert_scheduler(
    *,
    now_ny: datetime,
    execute_ibkr_login_alert: Callable[..., dict[str, Any]],
    get_ibkr_operational_status: Callable[[], dict[str, Any]],
    telegram_alerts_enabled: bool,
    send_telegram_alert: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    return execute_ibkr_login_alert(
        now_ny=now_ny,
        get_ibkr_operational_status=get_ibkr_operational_status,
        telegram_alerts_enabled=telegram_alerts_enabled,
        send_telegram_alert=send_telegram_alert,
    )
