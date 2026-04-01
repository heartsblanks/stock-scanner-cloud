

from __future__ import annotations

import argparse
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db import execute, fetch_all
from storage import upsert_trade_lifecycle


CLOSE_EVENT_TYPES = {"STOP_HIT", "TARGET_HIT", "MANUAL_CLOSE", "EOD_CLOSE"}


@dataclass
class LifecycleSeed:
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


def _event_trade_key(row: dict[str, Any]) -> str:
    return (
        _normalize_text(row.get("parent_order_id"))
        or _normalize_text(row.get("order_id"))
        or ""
    )


def _queue_key(row: dict[str, Any]) -> tuple[str, str, str]:
    shares = _to_float(row.get("shares"))
    shares_key = "" if shares is None else f"{shares:.8f}"
    return (
        _normalize_upper(row.get("symbol")),
        _normalize_text(row.get("mode")),
        shares_key,
    )


def _infer_direction(side: Any, entry_price: Any, exit_price: Any) -> str | None:
    side_text = _normalize_upper(side)
    if side_text == "BUY":
        return "LONG"
    if side_text == "SELL":
        return "SHORT"

    entry_val = _to_float(entry_price)
    exit_val = _to_float(exit_price)
    if entry_val is None or exit_val is None:
        return None
    if exit_val > entry_val:
        return "LONG"
    if exit_val < entry_val:
        return "SHORT"
    return None


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


def _fetch_trade_events() -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT
            id,
            event_time,
            event_type,
            symbol,
            side,
            shares,
            price,
            mode,
            order_id,
            parent_order_id,
            status
        FROM trade_events
        ORDER BY event_time ASC, id ASC
        """
    )


def _build_lifecycles(rows: list[dict[str, Any]]) -> list[LifecycleSeed]:
    opens_by_trade_key: dict[str, dict[str, Any]] = {}
    opens_by_queue: dict[tuple[str, str, str], deque[dict[str, Any]]] = defaultdict(deque)
    lifecycles: list[LifecycleSeed] = []

    for row in rows:
        event_type = _normalize_upper(row.get("event_type"))
        if event_type == "OPEN":
            trade_key = _event_trade_key(row)
            if trade_key:
                opens_by_trade_key[trade_key] = row
            opens_by_queue[_queue_key(row)].append(row)
            continue

        if event_type not in CLOSE_EVENT_TYPES:
            continue

        matched_open = None
        trade_key = _event_trade_key(row)
        if trade_key and trade_key in opens_by_trade_key:
            matched_open = opens_by_trade_key.pop(trade_key)
            queue = opens_by_queue.get(_queue_key(matched_open))
            if queue:
                for idx, queued in enumerate(queue):
                    if queued.get("id") == matched_open.get("id"):
                        del queue[idx]
                        break
                if not queue:
                    opens_by_queue.pop(_queue_key(matched_open), None)
        else:
            queue = opens_by_queue.get(_queue_key(row))
            if queue:
                matched_open = queue.popleft()
                matched_trade_key = _event_trade_key(matched_open)
                if matched_trade_key:
                    opens_by_trade_key.pop(matched_trade_key, None)
                if not queue:
                    opens_by_queue.pop(_queue_key(row), None)

        if not matched_open:
            continue

        open_trade_key = _event_trade_key(matched_open)
        close_trade_key = _event_trade_key(row)
        resolved_trade_key = open_trade_key or close_trade_key or f"{_normalize_upper(row.get('symbol'))}:{matched_open.get('id')}"

        entry_time = matched_open.get("event_time")
        exit_time = row.get("event_time")
        entry_price = _to_float(matched_open.get("price"))
        exit_price = _to_float(row.get("price"))
        shares = _to_float(matched_open.get("shares"))
        side = _normalize_upper(matched_open.get("side")) or None
        direction = _infer_direction(side, entry_price, exit_price)

        lifecycles.append(
            LifecycleSeed(
                trade_key=resolved_trade_key,
                symbol=_normalize_upper(matched_open.get("symbol")),
                mode=_normalize_text(matched_open.get("mode")),
                side=side,
                direction=direction,
                status="CLOSED",
                entry_time=entry_time,
                entry_price=entry_price,
                exit_time=exit_time,
                exit_price=exit_price,
                stop_price=None,
                target_price=None,
                exit_reason=event_type,
                shares=shares,
                realized_pnl=_compute_realized_pnl(entry_price, exit_price, shares, direction),
                realized_pnl_percent=_compute_realized_pnl_percent(entry_price, exit_price, direction),
                duration_minutes=_compute_duration_minutes(entry_time, exit_time),
                signal_timestamp=entry_time,
                signal_entry=entry_price,
                signal_stop=None,
                signal_target=None,
                signal_confidence=None,
                order_id=_normalize_text(matched_open.get("order_id")) or None,
                parent_order_id=_normalize_text(matched_open.get("parent_order_id")) or None,
                exit_order_id=_normalize_text(row.get("order_id")) or None,
            )
        )

    for trade_key, open_row in list(opens_by_trade_key.items()):
        entry_time = open_row.get("event_time")
        entry_price = _to_float(open_row.get("price"))
        side = _normalize_upper(open_row.get("side")) or None
        lifecycles.append(
            LifecycleSeed(
                trade_key=trade_key,
                symbol=_normalize_upper(open_row.get("symbol")),
                mode=_normalize_text(open_row.get("mode")),
                side=side,
                direction=_infer_direction(side, entry_price, None),
                status="OPEN",
                entry_time=entry_time,
                entry_price=entry_price,
                exit_time=None,
                exit_price=None,
                stop_price=None,
                target_price=None,
                exit_reason=None,
                shares=_to_float(open_row.get("shares")),
                realized_pnl=None,
                realized_pnl_percent=None,
                duration_minutes=None,
                signal_timestamp=entry_time,
                signal_entry=entry_price,
                signal_stop=None,
                signal_target=None,
                signal_confidence=None,
                order_id=_normalize_text(open_row.get("order_id")) or None,
                parent_order_id=_normalize_text(open_row.get("parent_order_id")) or None,
                exit_order_id=None,
            )
        )

    return lifecycles


def _reset_trade_lifecycles() -> None:
    execute("TRUNCATE TABLE trade_lifecycles RESTART IDENTITY")


def backfill(reset: bool = False) -> dict[str, int]:
    if reset:
        _reset_trade_lifecycles()

    rows = _fetch_trade_events()
    seeds = _build_lifecycles(rows)

    open_count = 0
    closed_count = 0
    for seed in seeds:
        upsert_trade_lifecycle(
            trade_key=seed.trade_key,
            symbol=seed.symbol,
            mode=seed.mode,
            side=seed.side,
            direction=seed.direction,
            status=seed.status,
            entry_time=seed.entry_time,
            entry_price=seed.entry_price,
            exit_time=seed.exit_time,
            exit_price=seed.exit_price,
            stop_price=seed.stop_price,
            target_price=seed.target_price,
            exit_reason=seed.exit_reason,
            shares=seed.shares,
            realized_pnl=seed.realized_pnl,
            realized_pnl_percent=seed.realized_pnl_percent,
            duration_minutes=seed.duration_minutes,
            signal_timestamp=seed.signal_timestamp,
            signal_entry=seed.signal_entry,
            signal_stop=seed.signal_stop,
            signal_target=seed.signal_target,
            signal_confidence=seed.signal_confidence,
            order_id=seed.order_id,
            parent_order_id=seed.parent_order_id,
            exit_order_id=seed.exit_order_id,
        )
        if seed.status == "OPEN":
            open_count += 1
        elif seed.status == "CLOSED":
            closed_count += 1

    return {
        "trade_events": len(rows),
        "lifecycles_written": len(seeds),
        "open_lifecycles": open_count,
        "closed_lifecycles": closed_count,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill trade_lifecycles from historical trade_events")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="truncate trade_lifecycles before backfill",
    )
    args = parser.parse_args()

    result = backfill(reset=args.reset)
    print(result)


if __name__ == "__main__":
    main()
