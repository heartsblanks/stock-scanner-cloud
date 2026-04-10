from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class PaperBroker(Protocol):
    name: str

    def get_account(self) -> dict[str, Any]:
        ...

    def get_open_positions(self) -> list[dict[str, Any]]:
        ...

    def get_open_orders(self) -> list[dict[str, Any]]:
        ...

    def cancel_open_orders_for_symbol(self, symbol: str) -> list[str]:
        ...

    def close_position(self, symbol: str, cancel_orders: bool = True) -> dict[str, Any]:
        ...

    def place_paper_bracket_order_from_trade(
        self,
        trade: dict[str, Any],
        max_notional: float | None = None,
    ) -> dict[str, Any]:
        ...

    def sync_order_by_id(self, order_id: str) -> dict[str, Any]:
        ...

    def get_order_by_id(self, order_id: str, nested: bool = False) -> dict[str, Any]:
        ...


@dataclass(frozen=True)
class PaperBrokerConfig:
    broker_name: str
    shadow_mode_enabled: bool
    market_data_compare_enabled: bool

