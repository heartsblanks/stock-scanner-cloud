from __future__ import annotations

from typing import Any

from alpaca.alpaca_client import alpaca_client


def fetch_positions() -> list[dict[str, Any]]:
    data = alpaca_client.get("/v2/positions")
    if isinstance(data, list):
        return data
    raise ValueError("Unexpected Alpaca positions response format")


def close_position_by_symbol(symbol: str, *, cancel_orders: bool = False) -> dict[str, Any]:
    normalized_symbol = str(symbol).strip().upper()
    if not normalized_symbol:
        raise ValueError("symbol is required")

    data = alpaca_client.delete(
        f"/v2/positions/{normalized_symbol}",
        params={"cancel_orders": str(bool(cancel_orders)).lower()},
    )
    if isinstance(data, dict):
        return data
    raise ValueError("Unexpected Alpaca close-position response format")