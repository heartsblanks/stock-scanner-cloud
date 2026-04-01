from __future__ import annotations

import argparse
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional

from db import fetch_all
from storage import upsert_trade_lifecycle


@dataclass
class LifecycleRow:
    id: int
    trade_key: str
    symbol: str
    mode: str | None
    side: str | None
    direction: str | None
    status: str | None
    entry_time: Any
    entry_price: float | None
    exit_time: Any
    exit_price: float | None
    stop_price: float | None
    target_price: float | None
    exit_reason: str | None
    shares: float | None
    realized_pnl: float | None
    realized_pnl_percent: float | None
    duration_minutes: float | None
    signal_timestamp: Any
    signal_entry: float | None
    signal_stop: float | None
    signal_target: float | None
    signal_confidence: float | None
    order_id: str | None
    parent_order_id: str | None
    exit_order_id: str | None


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_upper(value: Any) -> str:
    return _normalize_text(value).upper()


def _to_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except Exception:
        return None


def _direction_from_side(side: Any) -> str | None:
    side_text = _normalize_upper(side)
    if side_text == "BUY":
        return "LONG"
    if side_text == "SELL":
        return "SHORT"
    return None


def _compute_realized_pnl(entry_price: Any, exit_price: Any, shares: Any, direction: Any) -> Optional[float]:
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


def _compute_realized_pnl_percent(entry_price: Any, exit_price: Any, direction: Any) -> Optional[float]:
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


def _fetch_candidate_rows(target_date: Optional[str]) -> list[LifecycleRow]:
    where_clauses = [
        "UPPER(COALESCE(side, '')) IN ('BUY', 'SELL')",
        "("
        "(UPPER(COALESCE(side, '')) = 'BUY' AND UPPER(COALESCE(direction, '')) <> 'LONG') "
        "OR "
        "(UPPER(COALESCE(side, '')) = 'SELL' AND UPPER(COALESCE(direction, '')) <> 'SHORT')"
        ")",
    ]
    params: dict[str, Any] = {}

    if target_date:
        where_clauses.append(
            "DATE(COALESCE(exit_time, entry_time, created_at)) = %(target_date)s::date"
        )
        params["target_date"] = target_date

    rows = fetch_all(
        f"""
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
        WHERE {' AND '.join(where_clauses)}
        ORDER BY COALESCE(exit_time, entry_time, created_at) ASC, id ASC
        """,
        params,
    )

    return [
        LifecycleRow(
            id=int(row["id"]),
            trade_key=_normalize_text(row.get("trade_key")),
            symbol=_normalize_upper(row.get("symbol")),
            mode=_normalize_text(row.get("mode")) or None,
            side=_normalize_upper(row.get("side")) or None,
            direction=_normalize_upper(row.get("direction")) or None,
            status=_normalize_upper(row.get("status")) or None,
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


def repair_rows(rows: list[LifecycleRow], *, apply_changes: bool) -> dict[str, Any]:
    repaired = 0
    previews: list[dict[str, Any]] = []

    for row in rows:
        corrected_direction = _direction_from_side(row.side)
        corrected_pnl = _compute_realized_pnl(row.entry_price, row.exit_price, row.shares, corrected_direction)
        corrected_pnl_pct = _compute_realized_pnl_percent(row.entry_price, row.exit_price, corrected_direction)

        previews.append({
            "id": row.id,
            "symbol": row.symbol,
            "side": row.side,
            "old_direction": row.direction,
            "new_direction": corrected_direction,
            "old_realized_pnl": row.realized_pnl,
            "new_realized_pnl": corrected_pnl,
            "old_realized_pnl_percent": row.realized_pnl_percent,
            "new_realized_pnl_percent": corrected_pnl_pct,
            "entry_price": row.entry_price,
            "exit_price": row.exit_price,
            "shares": row.shares,
            "exit_reason": row.exit_reason,
        })

        if not apply_changes:
            continue

        upsert_trade_lifecycle(
            trade_key=row.trade_key,
            symbol=row.symbol,
            mode=row.mode,
            side=row.side,
            direction=corrected_direction,
            status=row.status,
            entry_time=row.entry_time,
            entry_price=row.entry_price,
            exit_time=row.exit_time,
            exit_price=row.exit_price,
            stop_price=row.stop_price,
            target_price=row.target_price,
            exit_reason=row.exit_reason,
            shares=row.shares,
            realized_pnl=corrected_pnl,
            realized_pnl_percent=corrected_pnl_pct,
            duration_minutes=row.duration_minutes,
            signal_timestamp=row.signal_timestamp,
            signal_entry=row.signal_entry,
            signal_stop=row.signal_stop,
            signal_target=row.signal_target,
            signal_confidence=row.signal_confidence,
            order_id=row.order_id,
            parent_order_id=row.parent_order_id,
            exit_order_id=row.exit_order_id,
        )
        repaired += 1

    return {
        "candidate_count": len(rows),
        "repaired_count": repaired,
        "previews": previews,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair inconsistent trade_lifecycles direction/PnL using stored side.")
    parser.add_argument("--date", help="Restrict repairs to YYYY-MM-DD based on lifecycle entry/exit/created date.")
    parser.add_argument("--apply", action="store_true", help="Apply updates. Without this flag the script runs in dry-run mode.")
    args = parser.parse_args()

    rows = _fetch_candidate_rows(args.date)
    result = repair_rows(rows, apply_changes=args.apply)

    mode = "APPLY" if args.apply else "DRY_RUN"
    print(
        {
            "mode": mode,
            "date": args.date,
            "candidate_count": result["candidate_count"],
            "repaired_count": result["repaired_count"],
            "previews": result["previews"],
        }
    )


if __name__ == "__main__":
    main()
