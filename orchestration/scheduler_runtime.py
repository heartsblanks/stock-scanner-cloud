import os
from datetime import datetime
from typing import Any, Callable

import requests


def _truthy_env(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, str(default))).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _looks_like_ibkr_auth_issue(error_text: str) -> bool:
    normalized = str(error_text or "").strip().lower()
    if not normalized:
        return False
    markers = (
        "not logged in",
        "login required",
        "log in",
        "logged out",
        "session is not ready",
        "session not ready",
        "authentication",
        "auth",
        "gateway login",
        "please login",
    )
    return any(marker in normalized for marker in markers)


def _looks_like_ibkr_session_unavailable(error_text: str) -> bool:
    normalized = str(error_text or "").strip().lower()
    if not normalized:
        return False
    if "503" not in normalized and "service unavailable" not in normalized:
        return False
    session_markers = (
        "/positions",
        "/account",
        "positions:",
        "account:",
    )
    return any(marker in normalized for marker in session_markers)


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
    low_call_mode = _truthy_env("IBKR_LOW_CALL_MODE", False)
    include_account_probe_default = not low_call_mode
    include_market_data_probe_default = not low_call_mode
    include_account_probe = _truthy_env("IBKR_STATUS_INCLUDE_ACCOUNT_PROBE", include_account_probe_default)
    include_market_data_probe = _truthy_env("IBKR_STATUS_INCLUDE_MARKET_DATA_PROBE", include_market_data_probe_default)

    try:
        bridge_payload = ibkr_bridge_get("/health", timeout=bridge_timeout) or {}
        status["bridge_health_ok"] = bool(bridge_payload.get("ok"))
        status["bridge"] = bridge_payload.get("ibkr")
    except Exception as exc:
        status["errors"].append(f"bridge_health: {exc}")
        status["state"] = "UNAVAILABLE"
        status["message"] = "IBKR bridge is not reachable."
        return status

    if include_account_probe:
        try:
            account_payload = ibkr_bridge_get("/account", timeout=account_timeout) or {}
            equity = account_equity_from_broker_account(account_payload)
            status["account_ok"] = bool(account_payload.get("account_id"))
            status["account_id"] = str(account_payload.get("account_id", "") or "")
            status["equity"] = equity if equity > 0 else None
        except Exception as exc:
            status["errors"].append(f"account: {exc}")
    else:
        status["account_ok"] = False

    try:
        positions_payload = ibkr_bridge_get("/positions", timeout=positions_timeout) or []
        status["session_probe_ok"] = True
        status["position_count"] = len(positions_payload)
    except Exception as exc:
        status["errors"].append(f"positions: {exc}")

    if include_market_data_probe:
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
    else:
        status["market_data_ok"] = bool(status["session_probe_ok"])

    session_ready = status["account_ok"] or status["session_probe_ok"]
    auth_issue_detected = any(_looks_like_ibkr_auth_issue(item) for item in status["errors"])
    session_unavailable_detected = any(_looks_like_ibkr_session_unavailable(item) for item in status["errors"])

    if status["bridge_health_ok"] and session_ready and status["market_data_ok"]:
        status["state"] = "READY"
        if status["account_ok"]:
            status["message"] = "IBKR bridge, account, and market data checks passed."
        else:
            status["message"] = "IBKR bridge session and market data checks passed; account summary is slow or unavailable."
        return status

    status["login_required"] = (
        bool(status["bridge_health_ok"])
        and not bool(session_ready)
        and (auth_issue_detected or session_unavailable_detected)
    )
    if status["bridge_health_ok"] and not session_ready:
        if status["login_required"]:
            status["state"] = "LOGIN_REQUIRED"
            status["message"] = "IBKR bridge is up, but the account session is not ready."
        else:
            status["state"] = "DEGRADED"
            status["message"] = "IBKR bridge is up, but the session probe was inconclusive."
    elif status["bridge_health_ok"] and session_ready and not status["market_data_ok"]:
        status["state"] = "MARKET_DATA_UNAVAILABLE"
        status["message"] = "IBKR account is up, but market data is not ready."
    else:
        status["state"] = "DEGRADED"
        status["message"] = "IBKR bridge is partially available but not ready for scans."
    return status


