

"""Thin client facade over centralized Alpaca HTTP helpers (alpaca_http)."""

from __future__ import annotations

from typing import Any

from alpaca.alpaca_http import alpaca_delete, alpaca_get, alpaca_post

class AlpacaClient:
    def get(self, path: str, *, params: dict[str, Any] | None = None, timeout: int | None = None) -> Any:
        return alpaca_get(path, params=params, timeout=timeout)

    def post(self, path: str, *, params: dict[str, Any] | None = None, json_body: dict[str, Any] | None = None, timeout: int | None = None) -> Any:
        return alpaca_post(path, params=params, json_body=json_body, timeout=timeout)

    def delete(self, path: str, *, params: dict[str, Any] | None = None, json_body: dict[str, Any] | None = None, timeout: int | None = None) -> Any:
        return alpaca_delete(path, params=params, json_body=json_body, timeout=timeout)

alpaca_client = AlpacaClient()