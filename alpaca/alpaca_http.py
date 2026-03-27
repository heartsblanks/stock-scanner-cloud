from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any

import requests

from storage import insert_alpaca_api_log

ALPACA_API_KEY_ID = os.getenv("APCA_API_KEY_ID", "")
ALPACA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY", "")
ALPACA_BASE_URL = os.getenv("APCA_BASE_URL", "https://paper-api.alpaca.markets")
ALPACA_HTTP_TIMEOUT_SECONDS = int(os.getenv("ALPACA_HTTP_TIMEOUT_SECONDS", "30"))

# Centralized logging flag
ALPACA_LOG_ENABLED = os.getenv("ALPACA_LOG_ENABLED", "false").lower() == "true"


class AlpacaHttpError(RuntimeError):
    """Raised when an Alpaca HTTP request fails or returns an unexpected payload."""


# Centralized request/response logging helper
def _log_alpaca_call(
    method: str,
    url: str,
    params: Any,
    json_body: Any,
    status: int | None,
    response_text: str | None,
    *,
    success: bool,
    error_message: str | None = None,
    duration_ms: int | None = None,
) -> None:
    if not ALPACA_LOG_ENABLED:
        return
    try:
        insert_alpaca_api_log(
            logged_at=datetime.now(timezone.utc),
            method=method,
            url=url,
            params=params if isinstance(params, dict) else None,
            request_body=json_body if isinstance(json_body, dict) else None,
            status_code=status,
            response_body=response_text[:5000] if response_text else None,
            success=success,
            error_message=error_message,
            duration_ms=duration_ms,
        )
    except Exception as e:
        print(f"Alpaca API DB log failed: {e}")

def alpaca_auth_headers() -> dict[str, str]:
    if not ALPACA_API_KEY_ID or not ALPACA_API_SECRET_KEY:
        raise ValueError("APCA_API_KEY_ID and APCA_API_SECRET_KEY must be set")
    return {
        "APCA-API-KEY-ID": ALPACA_API_KEY_ID,
        "APCA-API-SECRET-KEY": ALPACA_API_SECRET_KEY,
    }

def build_alpaca_url(path: str) -> str:
    normalized_path = "/" + str(path).lstrip("/")
    return f"{ALPACA_BASE_URL.rstrip('/')}{normalized_path}"

def alpaca_request(method: str, path: str, *, params: dict[str, Any] | None = None, json_body: dict[str, Any] | None = None, timeout: int | None = None) -> Any:
    url = build_alpaca_url(path)
    started = time.perf_counter()
    try:
        response = requests.request(
            method=method.upper(),
            url=url,
            headers=alpaca_auth_headers(),
            params=params,
            json=json_body,
            timeout=timeout or ALPACA_HTTP_TIMEOUT_SECONDS,
        )
        duration_ms = int((time.perf_counter() - started) * 1000)
        _log_alpaca_call(
            method.upper(),
            url,
            params,
            json_body,
            response.status_code,
            response.text,
            success=response.ok,
            error_message=None if response.ok else response.text[:1000],
            duration_ms=duration_ms,
        )

        response.raise_for_status()
        if not response.text.strip():
            return None
        try:
            return response.json()
        except ValueError as exc:
            raise AlpacaHttpError(f"Invalid JSON response from Alpaca for {method.upper()} {path}") from exc
    except requests.HTTPError as exc:
        # Already logged above with response details — avoid duplicate log
        raise
    except Exception as exc:
        duration_ms = int((time.perf_counter() - started) * 1000)
        _log_alpaca_call(
            method.upper(),
            url,
            params,
            json_body,
            None,
            None,
            success=False,
            error_message=str(exc),
            duration_ms=duration_ms,
        )
        raise

def alpaca_get(path: str, *, params: dict[str, Any] | None = None, timeout: int | None = None) -> Any:
    return alpaca_request("GET", path, params=params, timeout=timeout)

def alpaca_post(path: str, *, params: dict[str, Any] | None = None, json_body: dict[str, Any] | None = None, timeout: int | None = None) -> Any:
    return alpaca_request("POST", path, params=params, json_body=json_body, timeout=timeout)

def alpaca_delete(path: str, *, params: dict[str, Any] | None = None, json_body: dict[str, Any] | None = None, timeout: int | None = None) -> Any:
    return alpaca_request("DELETE", path, params=params, json_body=json_body, timeout=timeout)