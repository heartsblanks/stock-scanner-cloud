from __future__ import annotations

import math
from typing import Any

from brokers.ibkr_bridge_client import ibkr_bridge_get, ibkr_bridge_post, ibkr_bridge_enabled


def _bridge_timeout(env_name: str, default: int) -> int:
    import os

    try:
        return int(os.getenv(env_name, str(default)))
    except Exception:
        return default


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        return None
    if isinstance(value, dict):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    return value


def _compact_trade_for_bridge(trade: dict[str, Any]) -> dict[str, Any]:
    trade_dict = trade if isinstance(trade, dict) else {}
    metrics = trade_dict.get("metrics") if isinstance(trade_dict.get("metrics"), dict) else {}

    compact_metrics = {
        "symbol": metrics.get("symbol"),
        "direction": metrics.get("direction"),
        "entry": metrics.get("entry"),
        "stop": metrics.get("stop"),
        "target": metrics.get("target"),
        "shares": metrics.get("shares"),
        "per_trade_notional": metrics.get("per_trade_notional"),
        "remaining_allocatable_capital": metrics.get("remaining_allocatable_capital"),
        "exchange": metrics.get("exchange"),
        "primary_exchange": metrics.get("primary_exchange"),
        "currency": metrics.get("currency"),
    }

    compact_trade = {
        "name": trade_dict.get("name"),
        "final_reason": trade_dict.get("final_reason"),
        "decision": trade_dict.get("decision"),
        "metrics": compact_metrics,
    }
    return _json_safe_value(compact_trade)

class IbkrPaperBroker:
    name = "ibkr"

    def _ensure_bridge_enabled(self) -> None:
        if not ibkr_bridge_enabled():
            raise NotImplementedError(
                "IBKR paper broker support is not configured yet. "
                "Set IBKR_BRIDGE_BASE_URL after the GCP VM bridge is ready."
            )

    def get_account(self) -> dict[str, Any]:
        self._ensure_bridge_enabled()
        return ibkr_bridge_get("/account", timeout=_bridge_timeout("IBKR_BRIDGE_ACCOUNT_TIMEOUT_SECONDS", 5))

    def get_open_positions(self) -> list[dict[str, Any]]:
        self._ensure_bridge_enabled()
        return ibkr_bridge_get("/positions", timeout=_bridge_timeout("IBKR_BRIDGE_POSITIONS_TIMEOUT_SECONDS", 8)) or []

    def get_open_orders(self) -> list[dict[str, Any]]:
        self._ensure_bridge_enabled()
        return ibkr_bridge_get("/orders/open", timeout=_bridge_timeout("IBKR_BRIDGE_ORDERS_TIMEOUT_SECONDS", 8)) or []

    def get_open_state(self) -> dict[str, Any]:
        self._ensure_bridge_enabled()
        return ibkr_bridge_get("/open-state", timeout=_bridge_timeout("IBKR_BRIDGE_OPEN_STATE_TIMEOUT_SECONDS", 12)) or {}

    def cancel_open_orders_for_symbol(self, symbol: str) -> list[str]:
        self._ensure_bridge_enabled()
        result = ibkr_bridge_post(
            "/orders/cancel-by-symbol",
            json_body={"symbol": symbol},
            timeout=_bridge_timeout("IBKR_BRIDGE_CANCEL_TIMEOUT_SECONDS", 8),
        )
        return list(result.get("canceled_order_ids") or [])

    def close_position(self, symbol: str, cancel_orders: bool = True) -> dict[str, Any]:
        self._ensure_bridge_enabled()
        return ibkr_bridge_post(
            "/positions/close",
            json_body={"symbol": symbol, "cancel_orders": bool(cancel_orders)},
            timeout=_bridge_timeout("IBKR_BRIDGE_CLOSE_TIMEOUT_SECONDS", 35),
        )

    def place_paper_bracket_order_from_trade(
        self,
        trade: dict[str, Any],
        max_notional: float | None = None,
    ) -> dict[str, Any]:
        self._ensure_bridge_enabled()
        payload = {"trade": _compact_trade_for_bridge(trade)}
        if max_notional is not None:
            payload["max_notional"] = max_notional
        return ibkr_bridge_post("/orders/paper-bracket", json_body=payload)

    def sync_order_by_id(self, order_id: str) -> dict[str, Any]:
        self._ensure_bridge_enabled()
        return ibkr_bridge_get(
            f"/orders/{order_id}/sync",
            timeout=_bridge_timeout("IBKR_BRIDGE_ORDER_SYNC_TIMEOUT_SECONDS", 8),
        )

    def sync_orders_by_ids(self, order_ids: list[str]) -> dict[str, dict[str, Any]]:
        self._ensure_bridge_enabled()
        normalized_order_ids = [str(order_id).strip() for order_id in (order_ids or []) if str(order_id).strip()]
        if not normalized_order_ids:
            return {}

        payload = ibkr_bridge_post(
            "/orders/sync-batch",
            json_body={"order_ids": normalized_order_ids},
            timeout=_bridge_timeout("IBKR_BRIDGE_ORDER_SYNC_BATCH_TIMEOUT_SECONDS", 45),
        ) or {}
        rows = list(payload.get("results") or [])

        by_order_id: dict[str, dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            order_id = str(row.get("id", "")).strip()
            if not order_id:
                continue
            by_order_id[order_id] = row

        for order_id in normalized_order_ids:
            if order_id not in by_order_id:
                by_order_id[order_id] = {
                    "id": order_id,
                    "status": "unknown",
                    "message": "Order was not returned by IBKR batch sync response.",
                }
        return by_order_id

    def get_order_by_id(self, order_id: str, nested: bool = False) -> dict[str, Any]:
        self._ensure_bridge_enabled()
        return ibkr_bridge_get(
            f"/orders/{order_id}",
            params={"nested": str(bool(nested)).lower()},
            timeout=_bridge_timeout("IBKR_BRIDGE_ORDER_STATUS_TIMEOUT_SECONDS", 8),
        )
