"""Alpaca order operations built on the centralized Alpaca client wrapper."""
from __future__ import annotations

from typing import Any

from alpaca.alpaca_client import alpaca_client


def fetch_orders(
    limit: int = 500,
    *,
    status: str = "all",
    nested: bool = True,
    direction: str = "desc",
) -> list[dict[str, Any]]:
    data = alpaca_client.get(
        "/v2/orders",
        params={
            "status": status,
            "nested": str(nested).lower(),
            "direction": direction,
            "limit": limit,
        },
    )
    if isinstance(data, list):
        return data
    raise ValueError("Unexpected Alpaca orders response format")


def fetch_open_orders(limit: int = 500, *, nested: bool = True, direction: str = "desc") -> list[dict[str, Any]]:
    return fetch_orders(limit=limit, status="open", nested=nested, direction=direction)


def fetch_order_by_id(order_id: str, *, nested: bool = True) -> dict[str, Any]:
    normalized_order_id = str(order_id).strip()
    if not normalized_order_id:
        raise ValueError("order_id is required")

    data = alpaca_client.get(
        f"/v2/orders/{normalized_order_id}",
        params={"nested": str(nested).lower()},
    )
    if isinstance(data, dict):
        return data
    raise ValueError("Unexpected Alpaca order response format")


def cancel_order_by_id(order_id: str) -> None:
    normalized_order_id = str(order_id).strip()
    if not normalized_order_id:
        raise ValueError("order_id is required")

    alpaca_client.delete(f"/v2/orders/{normalized_order_id}")