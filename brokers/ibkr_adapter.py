from __future__ import annotations

from typing import Any


class IbkrPaperBroker:
    name = "ibkr"

    def _not_ready(self) -> None:
        raise NotImplementedError(
            "IBKR paper broker support is not implemented yet. "
            "Use PAPER_BROKER=alpaca until the IBKR bridge and adapter are ready."
        )

    def get_account(self) -> dict[str, Any]:
        self._not_ready()

    def get_open_positions(self) -> list[dict[str, Any]]:
        self._not_ready()

    def get_open_orders(self) -> list[dict[str, Any]]:
        self._not_ready()

    def cancel_open_orders_for_symbol(self, symbol: str) -> list[str]:
        self._not_ready()

    def close_position(self, symbol: str, cancel_orders: bool = True) -> dict[str, Any]:
        self._not_ready()

    def place_paper_bracket_order_from_trade(
        self,
        trade: dict[str, Any],
        max_notional: float | None = None,
    ) -> dict[str, Any]:
        self._not_ready()

    def sync_order_by_id(self, order_id: str) -> dict[str, Any]:
        self._not_ready()

    def get_order_by_id(self, order_id: str, nested: bool = False) -> dict[str, Any]:
        self._not_ready()

