from __future__ import annotations

import os
from typing import Any


def get_ibkr_bridge_base_url() -> str:
    return str(os.getenv("IBKR_BRIDGE_BASE_URL", "")).strip().rstrip("/")


def get_ibkr_bridge_token() -> str:
    return str(os.getenv("IBKR_BRIDGE_TOKEN", "")).strip()


def ibkr_bridge_enabled() -> bool:
    return bool(get_ibkr_bridge_base_url())


def _ibkr_bridge_headers() -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    token = get_ibkr_bridge_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _require_bridge_base_url() -> str:
    base_url = get_ibkr_bridge_base_url()
    if not base_url:
        raise RuntimeError("IBKR_BRIDGE_BASE_URL is not configured")
    return base_url


def ibkr_bridge_request(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> Any:
    import requests

    base_url = _require_bridge_base_url()
    normalized_path = "/" + str(path).lstrip("/")
    response = requests.request(
        method=method.upper(),
        url=f"{base_url}{normalized_path}",
        headers=_ibkr_bridge_headers(),
        params=params,
        json=json_body,
        timeout=timeout or int(os.getenv("IBKR_BRIDGE_TIMEOUT_SECONDS", "30")),
    )
    response.raise_for_status()

    if not response.text.strip():
        return None

    return response.json()


def ibkr_bridge_get(path: str, *, params: dict[str, Any] | None = None, timeout: int | None = None) -> Any:
    return ibkr_bridge_request("GET", path, params=params, timeout=timeout)


def ibkr_bridge_post(
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> Any:
    return ibkr_bridge_request("POST", path, params=params, json_body=json_body, timeout=timeout)

