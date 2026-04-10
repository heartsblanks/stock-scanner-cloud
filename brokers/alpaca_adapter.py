from __future__ import annotations

from typing import Any

from alpaca.paper import (
    cancel_open_orders_for_symbol,
    close_position,
    get_account,
    get_open_orders,
    get_open_positions,
    place_paper_bracket_order_from_trade,
)
from alpaca.sync import get_order_by_id, sync_order_by_id


class AlpacaPaperBroker:
    name = "alpaca"

    def get_account(self) -> dict[str, Any]:
        return get_account()

    def get_open_positions(self) -> list[dict[str, Any]]:
        return get_open_positions()

    def get_open_orders(self) -> list[dict[str, Any]]:
        return get_open_orders()

    def cancel_open_orders_for_symbol(self, symbol: str) -> list[str]:
        return cancel_open_orders_for_symbol(symbol)

    def close_position(self, symbol: str, cancel_orders: bool = True) -> dict[str, Any]:
        return close_position(symbol, cancel_orders=cancel_orders)

    def place_paper_bracket_order_from_trade(
        self,
        trade: dict[str, Any],
        max_notional: float | None = None,
    ) -> dict[str, Any]:
        return place_paper_bracket_order_from_trade(trade, max_notional=max_notional)

    def sync_order_by_id(self, order_id: str) -> dict[str, Any]:
        return sync_order_by_id(order_id)

    def get_order_by_id(self, order_id: str, nested: bool = False) -> dict[str, Any]:
        return get_order_by_id(order_id, nested=nested)

