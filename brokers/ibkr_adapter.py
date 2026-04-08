from __future__ import annotations

from typing import Any

from brokers.ibkr_bridge_client import ibkr_bridge_get, ibkr_bridge_post, ibkr_bridge_enabled

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
        return ibkr_bridge_get("/account")

    def get_open_positions(self) -> list[dict[str, Any]]:
        self._ensure_bridge_enabled()
        return ibkr_bridge_get("/positions") or []

    def get_open_orders(self) -> list[dict[str, Any]]:
        self._ensure_bridge_enabled()
        return ibkr_bridge_get("/orders/open") or []

    def cancel_open_orders_for_symbol(self, symbol: str) -> list[str]:
        self._ensure_bridge_enabled()
        result = ibkr_bridge_post("/orders/cancel-by-symbol", json_body={"symbol": symbol})
        return list(result.get("canceled_order_ids") or [])

    def close_position(self, symbol: str, cancel_orders: bool = True) -> dict[str, Any]:
        self._ensure_bridge_enabled()
        return ibkr_bridge_post(
            "/positions/close",
            json_body={"symbol": symbol, "cancel_orders": bool(cancel_orders)},
        )

    def place_paper_bracket_order_from_trade(
        self,
        trade: dict[str, Any],
        max_notional: float | None = None,
    ) -> dict[str, Any]:
        self._ensure_bridge_enabled()
        payload = {"trade": trade}
        if max_notional is not None:
            payload["max_notional"] = max_notional
        return ibkr_bridge_post("/orders/paper-bracket", json_body=payload)

    def sync_order_by_id(self, order_id: str) -> dict[str, Any]:
        self._ensure_bridge_enabled()
        return ibkr_bridge_get(f"/orders/{order_id}/sync")

    def get_order_by_id(self, order_id: str, nested: bool = False) -> dict[str, Any]:
        self._ensure_bridge_enabled()
        return ibkr_bridge_get(f"/orders/{order_id}", params={"nested": str(bool(nested)).lower()})
