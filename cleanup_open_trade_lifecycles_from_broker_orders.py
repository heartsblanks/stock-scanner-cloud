

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from db import fetch_all
from storage import upsert_trade_lifecycle


PRESERVE_SYMBOLS = {"NVDA", "PLTR", "SOFI", "TSLA"}


@dataclass
class LifecycleRow:
    id: int
    trade_key: str
    symbol: str
    mode: str
    side: str | None
    direction: str | None
    status: str
    entry_time: datetime | None
    entry_price: float | None
    exit_time: datetime | None
    exit_price: float | None
    stop_price: float | None
    target_price: float | None
    exit_reason: str | None
    shares: float | None
    realized_pnl: float | None
    realized_pnl_percent: float | None
    duration_minutes: float | None
    signal_timestamp: datetime | None
    signal_entry: float | None
    signal_stop: float | None
    signal_target: float | None
    signal_confidence: float | None
    order_id: str | None
    parent_order_id: str | None
    exit_order_id: str | None


@dataclass
class BrokerOrderRow:
    order_id: str
    symbol: str
    side: str | None
    order_type: str | None
    status: str | None
    qty: float | None
    filled_qty: float | None
    avg_fill_price: float | None
    submitted_at: datetime | None
    filled_at: datetime | None
    created_at: datetime | None


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except Exception:
        return None


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_upper(value: Any) -> str:
    return _normalize_text(value).upper()


def _compute_realized_pnl(entry_price: Any, exit_price: Any, shares: Any, direction: Any) -> float | None:
    entry_val = _to_float(entry_price)
    exit_val = _to_float(exit_price)
    shares_val = _to_float(shares)
    direction_text = _normalize_upper(direction)

    if entry_val is None or exit_val is None or shares_val is None:
        return None
    if direction_text == "LONG":
        return round((exit_val - entry_val) * shares_val, 6)
    if direction_text == "SHORT":
        return round((entry_val - exit_val) * shares_val, 6)
    return None


def _compute_realized_pnl_percent(entry_price: Any, exit_price: Any, direction: Any) -> float | None:
    entry_val = _to_float(entry_price)
    exit_val = _to_float(exit_price)
    direction_text = _normalize_upper(direction)

    if entry_val in (None, 0) or exit_val is None:
        return None
    if direction_text == "LONG":
        return round(((exit_val - entry_val) / entry_val) * 100.0, 6)
    if direction_text == "SHORT":
        return round(((entry_val - exit_val) / entry_val) * 100.0, 6)
    return None


def _compute_duration_minutes(entry_time: datetime | None, exit_time: datetime | None) -> float | None:
    if entry_time is None or exit_time is None:
        return None
    try:
        return round((exit_time - entry_time).total_seconds() / 60.0, 2)
    except Exception:
        return None


def _expected_exit_side(side: str | None, direction: str | None) -> str | None:
    side_text = _normalize_upper(side)
    direction_text = _normalize_upper(direction)

    if side_text == "BUY":
        return "SELL"
    if side_text == "SELL":
        return "BUY"

    if direction_text == "LONG":
        return "SELL"
    if direction_text == "SHORT":
        return "BUY"
    return None


def _fetch_open_lifecycles() -> list[LifecycleRow]:
    rows = fetch_all(
        """
        SELECT
            id,
            trade_key,
            symbol,
            mode,
            side,
            direction,
            status,
            entry_time,
            entry_price,
            exit_time,
            exit_price,
            stop_price,
            target_price,
            exit_reason,
            shares,
            realized_pnl,
            realized_pnl_percent,
            duration_minutes,
            signal_timestamp,
            signal_entry,
            signal_stop,
            signal_target,
            signal_confidence,
            order_id,
            parent_order_id,
            exit_order_id
        FROM trade_lifecycles
        WHERE status = 'OPEN'
        ORDER BY entry_time ASC, id ASC
        """
    )
    return [
        LifecycleRow(
            id=row["id"],
            trade_key=_normalize_text(row.get("trade_key")),
            symbol=_normalize_upper(row.get("symbol")),
            mode=_normalize_text(row.get("mode")),
            side=_normalize_upper(row.get("side")) or None,
            direction=_normalize_upper(row.get("direction")) or None,
            status=_normalize_upper(row.get("status")),
            entry_time=row.get("entry_time"),
            entry_price=_to_float(row.get("entry_price")),
            exit_time=row.get("exit_time"),
            exit_price=_to_float(row.get("exit_price")),
            stop_price=_to_float(row.get("stop_price")),
            target_price=_to_float(row.get("target_price")),
            exit_reason=_normalize_text(row.get("exit_reason")) or None,
            shares=_to_float(row.get("shares")),
            realized_pnl=_to_float(row.get("realized_pnl")),
            realized_pnl_percent=_to_float(row.get("realized_pnl_percent")),
            duration_minutes=_to_float(row.get("duration_minutes")),
            signal_timestamp=row.get("signal_timestamp"),
            signal_entry=_to_float(row.get("signal_entry")),
            signal_stop=_to_float(row.get("signal_stop")),
            signal_target=_to_float(row.get("signal_target")),
            signal_confidence=_to_float(row.get("signal_confidence")),
            order_id=_normalize_text(row.get("order_id")) or None,
            parent_order_id=_normalize_text(row.get("parent_order_id")) or None,
            exit_order_id=_normalize_text(row.get("exit_order_id")) or None,
        )
        for row in rows
    ]


def _fetch_broker_orders_for_symbols(symbols: list[str]) -> list[BrokerOrderRow]:
    if not symbols:
        return []

    rows = fetch_all(
        """
        SELECT
            order_id,
            symbol,
            side,
            order_type,
            status,
            qty,
            filled_qty,
            avg_fill_price,
            submitted_at,
            filled_at,
            created_at
        FROM broker_orders
        WHERE symbol = ANY(%(symbols)s)
        ORDER BY symbol ASC, COALESCE(filled_at, submitted_at, created_at) ASC, id ASC
        """,
        {"symbols": symbols},
    )
    return [
        BrokerOrderRow(
            order_id=_normalize_text(row.get("order_id")),
            symbol=_normalize_upper(row.get("symbol")),
            side=_normalize_upper(row.get("side")) or None,
            order_type=_normalize_text(row.get("order_type")) or None,
            status=_normalize_upper(row.get("status")) or None,
            qty=_to_float(row.get("qty")),
            filled_qty=_to_float(row.get("filled_qty")),
            avg_fill_price=_to_float(row.get("avg_fill_price")),
            submitted_at=row.get("submitted_at"),
            filled_at=row.get("filled_at"),
            created_at=row.get("created_at"),
        )
        for row in rows
    ]


def _order_time(order: BrokerOrderRow) -> datetime | None:
    return order.filled_at or order.submitted_at or order.created_at


def _find_exit_order(lifecycle: LifecycleRow, broker_orders: list[BrokerOrderRow]) -> BrokerOrderRow | None:
    expected_exit_side = _expected_exit_side(lifecycle.side, lifecycle.direction)
    entry_time = lifecycle.entry_time

    candidates: list[BrokerOrderRow] = []
    for order in broker_orders:
        if order.symbol != lifecycle.symbol:
            continue
        if _normalize_upper(order.status) != "FILLED":
            continue
        if expected_exit_side and _normalize_upper(order.side) != expected_exit_side:
            continue

        order_time = _order_time(order)
        if entry_time and order_time and order_time <= entry_time:
            continue

        if lifecycle.order_id and order.order_id == lifecycle.order_id:
            continue
        if lifecycle.parent_order_id and order.order_id == lifecycle.parent_order_id:
            continue
        if lifecycle.trade_key and order.order_id == lifecycle.trade_key:
            continue

        candidates.append(order)

    candidates.sort(key=lambda row: (_order_time(row) or datetime.max))
    return candidates[0] if candidates else None


def cleanup(dry_run: bool = False) -> dict[str, int]:
    open_lifecycles = _fetch_open_lifecycles()
    broker_orders = _fetch_broker_orders_for_symbols(sorted({row.symbol for row in open_lifecycles}))

    broker_orders_by_symbol: dict[str, list[BrokerOrderRow]] = {}
    for order in broker_orders:
        broker_orders_by_symbol.setdefault(order.symbol, []).append(order)

    preserved = 0
    closed = 0
    no_match = 0

    for lifecycle in open_lifecycles:
        if lifecycle.symbol in PRESERVE_SYMBOLS:
            preserved += 1
            continue

        exit_order = _find_exit_order(lifecycle, broker_orders_by_symbol.get(lifecycle.symbol, []))
        if not exit_order:
            no_match += 1
            continue

        exit_time = _order_time(exit_order)
        exit_price = exit_order.avg_fill_price
        if exit_time is None or exit_price is None:
            no_match += 1
            continue

        realized_pnl = _compute_realized_pnl(lifecycle.entry_price, exit_price, lifecycle.shares, lifecycle.direction)
        realized_pnl_percent = _compute_realized_pnl_percent(lifecycle.entry_price, exit_price, lifecycle.direction)
        duration_minutes = _compute_duration_minutes(lifecycle.entry_time, exit_time)

        if not dry_run:
            upsert_trade_lifecycle(
                trade_key=lifecycle.trade_key,
                symbol=lifecycle.symbol,
                mode=lifecycle.mode,
                side=lifecycle.side,
                direction=lifecycle.direction,
                status="CLOSED",
                entry_time=lifecycle.entry_time,
                entry_price=lifecycle.entry_price,
                exit_time=exit_time,
                exit_price=exit_price,
                stop_price=lifecycle.stop_price,
                target_price=lifecycle.target_price,
                exit_reason="BROKER_FILLED_BACKFILL",
                shares=lifecycle.shares,
                realized_pnl=realized_pnl,
                realized_pnl_percent=realized_pnl_percent,
                duration_minutes=duration_minutes,
                signal_timestamp=lifecycle.signal_timestamp,
                signal_entry=lifecycle.signal_entry,
                signal_stop=lifecycle.signal_stop,
                signal_target=lifecycle.signal_target,
                signal_confidence=lifecycle.signal_confidence,
                order_id=lifecycle.order_id,
                parent_order_id=lifecycle.parent_order_id,
                exit_order_id=exit_order.order_id,
            )
        closed += 1

    return {
        "open_lifecycles_seen": len(open_lifecycles),
        "preserved_open_positions": preserved,
        "closed_from_broker_orders": closed,
        "open_without_match": no_match,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Close stale OPEN trade_lifecycles from later filled broker_orders while preserving the 4 currently open symbols"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="show cleanup counts without writing updates",
    )
    args = parser.parse_args()

    result = cleanup(dry_run=args.dry_run)
    print(result)


if __name__ == "__main__":
    main()