from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional


from db import execute, fetch_all, fetch_one


def _normalize_text(value: Optional[str]) -> str:
    return str(value or "").strip()



def insert_scan_run(
    scan_time: datetime,
    mode: str,
    scan_source: Optional[str] = None,
    market_phase: Optional[str] = None,
    candidate_count: Optional[int] = None,
    placed_count: Optional[int] = None,
    skipped_count: Optional[int] = None,
) -> None:
    existing = fetch_one(
        """
        SELECT id
        FROM scan_runs
        WHERE scan_time = %(scan_time)s
          AND COALESCE(mode, '') = %(mode)s
          AND COALESCE(scan_source, '') = %(scan_source)s
          AND COALESCE(market_phase, '') = %(market_phase)s
          AND COALESCE(candidate_count, -1) = %(candidate_count_match)s
          AND COALESCE(placed_count, -1) = %(placed_count_match)s
          AND COALESCE(skipped_count, -1) = %(skipped_count_match)s
        LIMIT 1
        """,
        {
            "scan_time": scan_time,
            "mode": _normalize_text(mode),
            "scan_source": _normalize_text(scan_source),
            "market_phase": _normalize_text(market_phase),
            "candidate_count_match": candidate_count if candidate_count is not None else -1,
            "placed_count_match": placed_count if placed_count is not None else -1,
            "skipped_count_match": skipped_count if skipped_count is not None else -1,
        },
    )
    if existing:
        return
    execute(
        """
        INSERT INTO scan_runs (
            scan_time,
            mode,
            scan_source,
            market_phase,
            candidate_count,
            placed_count,
            skipped_count
        )
        VALUES (
            %(scan_time)s,
            %(mode)s,
            %(scan_source)s,
            %(market_phase)s,
            %(candidate_count)s,
            %(placed_count)s,
            %(skipped_count)s
        )
        """,
        {
            "scan_time": scan_time,
            "mode": mode,
            "scan_source": scan_source,
            "market_phase": market_phase,
            "candidate_count": candidate_count,
            "placed_count": placed_count,
            "skipped_count": skipped_count,
        },
    )



def insert_trade_event(
    event_time: datetime,
    event_type: str,
    symbol: str,
    side: Optional[str] = None,
    shares: Optional[float] = None,
    price: Optional[float] = None,
    mode: Optional[str] = None,
    order_id: Optional[str] = None,
    parent_order_id: Optional[str] = None,
    status: Optional[str] = None,
) -> None:
    existing = fetch_one(
        """
        SELECT id
        FROM trade_events
        WHERE event_time = %(event_time)s
          AND event_type = %(event_type)s
          AND symbol = %(symbol)s
          AND COALESCE(side, '') = %(side)s
          AND COALESCE(shares, -1) = %(shares_match)s
          AND COALESCE(price, -1) = %(price_match)s
          AND COALESCE(mode, '') = %(mode)s
          AND COALESCE(order_id, '') = %(order_id)s
          AND COALESCE(parent_order_id, '') = %(parent_order_id)s
          AND COALESCE(status, '') = %(status)s
        LIMIT 1
        """,
        {
            "event_time": event_time,
            "event_type": event_type,
            "symbol": symbol,
            "side": _normalize_text(side),
            "shares_match": shares if shares is not None else -1,
            "price_match": price if price is not None else -1,
            "mode": _normalize_text(mode),
            "order_id": _normalize_text(order_id),
            "parent_order_id": _normalize_text(parent_order_id),
            "status": _normalize_text(status),
        },
    )
    if existing:
        return
    execute(
        """
        INSERT INTO trade_events (
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
        )
        VALUES (
            %(event_time)s,
            %(event_type)s,
            %(symbol)s,
            %(side)s,
            %(shares)s,
            %(price)s,
            %(mode)s,
            %(order_id)s,
            %(parent_order_id)s,
            %(status)s
        )
        """,
        {
            "event_time": event_time,
            "event_type": event_type,
            "symbol": symbol,
            "side": side,
            "shares": shares,
            "price": price,
            "mode": mode,
            "order_id": order_id,
            "parent_order_id": parent_order_id,
            "status": status,
        },
    )



def insert_broker_order(
    order_id: str,
    symbol: Optional[str] = None,
    side: Optional[str] = None,
    order_type: Optional[str] = None,
    status: Optional[str] = None,
    qty: Optional[float] = None,
    filled_qty: Optional[float] = None,
    avg_fill_price: Optional[float] = None,
    submitted_at: Optional[datetime] = None,
    filled_at: Optional[datetime] = None,
) -> None:
    existing = fetch_one(
        """
        SELECT id
        FROM broker_orders
        WHERE order_id = %(order_id)s
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        {"order_id": order_id},
    )

    if existing:
        execute(
            """
            UPDATE broker_orders
            SET symbol = %(symbol)s,
                side = %(side)s,
                order_type = %(order_type)s,
                status = %(status)s,
                qty = %(qty)s,
                filled_qty = %(filled_qty)s,
                avg_fill_price = %(avg_fill_price)s,
                submitted_at = %(submitted_at)s,
                filled_at = %(filled_at)s
            WHERE id = %(id)s
            """,
            {
                "id": existing["id"],
                "order_id": order_id,
                "symbol": symbol,
                "side": side,
                "order_type": order_type,
                "status": status,
                "qty": qty,
                "filled_qty": filled_qty,
                "avg_fill_price": avg_fill_price,
                "submitted_at": submitted_at,
                "filled_at": filled_at,
            },
        )
        return

    execute(
        """
        INSERT INTO broker_orders (
            order_id,
            symbol,
            side,
            order_type,
            status,
            qty,
            filled_qty,
            avg_fill_price,
            submitted_at,
            filled_at
        )
        VALUES (
            %(order_id)s,
            %(symbol)s,
            %(side)s,
            %(order_type)s,
            %(status)s,
            %(qty)s,
            %(filled_qty)s,
            %(avg_fill_price)s,
            %(submitted_at)s,
            %(filled_at)s
        )
        """,
        {
            "order_id": order_id,
            "symbol": symbol,
            "side": side,
            "order_type": order_type,
            "status": status,
            "qty": qty,
            "filled_qty": filled_qty,
            "avg_fill_price": avg_fill_price,
            "submitted_at": submitted_at,
            "filled_at": filled_at,
        },
    )



def insert_reconciliation_run(
    run_time: datetime,
    matched_count: int,
    unmatched_count: int,
    notes: Optional[str] = None,
) -> None:
    existing = fetch_one(
        """
        SELECT id
        FROM reconciliation_runs
        WHERE run_time = %(run_time)s
          AND matched_count = %(matched_count)s
          AND unmatched_count = %(unmatched_count)s
          AND COALESCE(notes, '') = %(notes)s
        LIMIT 1
        """,
        {
            "run_time": run_time,
            "matched_count": matched_count,
            "unmatched_count": unmatched_count,
            "notes": _normalize_text(notes),
        },
    )
    if existing:
        return
    execute(
        """
        INSERT INTO reconciliation_runs (
            run_time,
            matched_count,
            unmatched_count,
            notes
        )
        VALUES (
            %(run_time)s,
            %(matched_count)s,
            %(unmatched_count)s,
            %(notes)s
        )
        """,
        {
            "run_time": run_time,
            "matched_count": matched_count,
            "unmatched_count": unmatched_count,
            "notes": notes,
        },
    )


# Insert reconciliation detail
def insert_reconciliation_detail(
    run_id: Optional[int],
    broker_parent_order_id: Optional[str] = None,
    symbol: Optional[str] = None,
    mode: Optional[str] = None,
    client_order_id: Optional[str] = None,
    local_entry_timestamp_utc: Optional[datetime] = None,
    local_exit_timestamp_utc: Optional[datetime] = None,
    local_entry_price: Optional[float] = None,
    alpaca_entry_price: Optional[float] = None,
    local_exit_price: Optional[float] = None,
    alpaca_exit_price: Optional[float] = None,
    local_shares: Optional[float] = None,
    alpaca_entry_qty: Optional[float] = None,
    alpaca_exit_qty: Optional[float] = None,
    local_exit_reason: Optional[str] = None,
    alpaca_exit_reason: Optional[str] = None,
    alpaca_exit_order_id: Optional[str] = None,
    entry_price_diff: Optional[float] = None,
    exit_price_diff: Optional[float] = None,
    match_status: Optional[str] = None,
) -> None:
    existing = fetch_one(
        """
        SELECT id
        FROM reconciliation_details
        WHERE COALESCE(run_id, -1) = %(run_id_match)s
          AND COALESCE(broker_parent_order_id, '') = %(broker_parent_order_id)s
          AND COALESCE(symbol, '') = %(symbol)s
          AND COALESCE(mode, '') = %(mode)s
          AND COALESCE(client_order_id, '') = %(client_order_id)s
          AND COALESCE(local_entry_timestamp_utc, TIMESTAMPTZ '1970-01-01 00:00:00+00') = %(local_entry_timestamp_utc_match)s
          AND COALESCE(local_exit_timestamp_utc, TIMESTAMPTZ '1970-01-01 00:00:00+00') = %(local_exit_timestamp_utc_match)s
          AND COALESCE(local_entry_price, -1) = %(local_entry_price_match)s
          AND COALESCE(alpaca_entry_price, -1) = %(alpaca_entry_price_match)s
          AND COALESCE(local_exit_price, -1) = %(local_exit_price_match)s
          AND COALESCE(alpaca_exit_price, -1) = %(alpaca_exit_price_match)s
          AND COALESCE(local_shares, -1) = %(local_shares_match)s
          AND COALESCE(alpaca_entry_qty, -1) = %(alpaca_entry_qty_match)s
          AND COALESCE(alpaca_exit_qty, -1) = %(alpaca_exit_qty_match)s
          AND COALESCE(local_exit_reason, '') = %(local_exit_reason)s
          AND COALESCE(alpaca_exit_reason, '') = %(alpaca_exit_reason)s
          AND COALESCE(alpaca_exit_order_id, '') = %(alpaca_exit_order_id)s
          AND COALESCE(entry_price_diff, -1) = %(entry_price_diff_match)s
          AND COALESCE(exit_price_diff, -1) = %(exit_price_diff_match)s
          AND COALESCE(match_status, '') = %(match_status)s
        LIMIT 1
        """,
        {
            "run_id_match": run_id if run_id is not None else -1,
            "broker_parent_order_id": _normalize_text(broker_parent_order_id),
            "symbol": _normalize_text(symbol),
            "mode": _normalize_text(mode),
            "client_order_id": _normalize_text(client_order_id),
            "local_entry_timestamp_utc_match": local_entry_timestamp_utc or datetime(1970, 1, 1),
            "local_exit_timestamp_utc_match": local_exit_timestamp_utc or datetime(1970, 1, 1),
            "local_entry_price_match": local_entry_price if local_entry_price is not None else -1,
            "alpaca_entry_price_match": alpaca_entry_price if alpaca_entry_price is not None else -1,
            "local_exit_price_match": local_exit_price if local_exit_price is not None else -1,
            "alpaca_exit_price_match": alpaca_exit_price if alpaca_exit_price is not None else -1,
            "local_shares_match": local_shares if local_shares is not None else -1,
            "alpaca_entry_qty_match": alpaca_entry_qty if alpaca_entry_qty is not None else -1,
            "alpaca_exit_qty_match": alpaca_exit_qty if alpaca_exit_qty is not None else -1,
            "local_exit_reason": _normalize_text(local_exit_reason),
            "alpaca_exit_reason": _normalize_text(alpaca_exit_reason),
            "alpaca_exit_order_id": _normalize_text(alpaca_exit_order_id),
            "entry_price_diff_match": entry_price_diff if entry_price_diff is not None else -1,
            "exit_price_diff_match": exit_price_diff if exit_price_diff is not None else -1,
            "match_status": _normalize_text(match_status),
        },
    )
    if existing:
        return

    execute(
        """
        INSERT INTO reconciliation_details (
            run_id,
            broker_parent_order_id,
            symbol,
            mode,
            client_order_id,
            local_entry_timestamp_utc,
            local_exit_timestamp_utc,
            local_entry_price,
            alpaca_entry_price,
            local_exit_price,
            alpaca_exit_price,
            local_shares,
            alpaca_entry_qty,
            alpaca_exit_qty,
            local_exit_reason,
            alpaca_exit_reason,
            alpaca_exit_order_id,
            entry_price_diff,
            exit_price_diff,
            match_status
        )
        VALUES (
            %(run_id)s,
            %(broker_parent_order_id)s,
            %(symbol)s,
            %(mode)s,
            %(client_order_id)s,
            %(local_entry_timestamp_utc)s,
            %(local_exit_timestamp_utc)s,
            %(local_entry_price)s,
            %(alpaca_entry_price)s,
            %(local_exit_price)s,
            %(alpaca_exit_price)s,
            %(local_shares)s,
            %(alpaca_entry_qty)s,
            %(alpaca_exit_qty)s,
            %(local_exit_reason)s,
            %(alpaca_exit_reason)s,
            %(alpaca_exit_order_id)s,
            %(entry_price_diff)s,
            %(exit_price_diff)s,
            %(match_status)s
        )
        """,
        {
            "run_id": run_id,
            "broker_parent_order_id": broker_parent_order_id,
            "symbol": symbol,
            "mode": mode,
            "client_order_id": client_order_id,
            "local_entry_timestamp_utc": local_entry_timestamp_utc,
            "local_exit_timestamp_utc": local_exit_timestamp_utc,
            "local_entry_price": local_entry_price,
            "alpaca_entry_price": alpaca_entry_price,
            "local_exit_price": local_exit_price,
            "alpaca_exit_price": alpaca_exit_price,
            "local_shares": local_shares,
            "alpaca_entry_qty": alpaca_entry_qty,
            "alpaca_exit_qty": alpaca_exit_qty,
            "local_exit_reason": local_exit_reason,
            "alpaca_exit_reason": alpaca_exit_reason,
            "alpaca_exit_order_id": alpaca_exit_order_id,
            "entry_price_diff": entry_price_diff,
            "exit_price_diff": exit_price_diff,
            "match_status": match_status,
        },
    )



def get_recent_trade_events(limit: int = 100) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT *
        FROM trade_events
        ORDER BY event_time DESC, id DESC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )



def get_recent_scan_runs(limit: int = 100) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT *
        FROM scan_runs
        ORDER BY scan_time DESC, id DESC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )



def get_trade_event_by_order_id(order_id: str) -> Optional[dict[str, Any]]:
    return fetch_one(
        """
        SELECT *
        FROM trade_events
        WHERE order_id = %(order_id)s
        ORDER BY event_time DESC, id DESC
        LIMIT 1
        """,
        {"order_id": order_id},
    )



def get_broker_order(order_id: str) -> Optional[dict[str, Any]]:
    return fetch_one(
        """
        SELECT *
        FROM broker_orders
        WHERE order_id = %(order_id)s
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        {"order_id": order_id},
    )


# Read reconciliation details for a run
def get_reconciliation_details_for_run(run_id: int) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT *
        FROM reconciliation_details
        WHERE run_id = %(run_id)s
        ORDER BY id ASC
        """,
        {"run_id": run_id},
    )


# === Phase 5 dashboard API helpers ===

def get_latest_reconciliation_run() -> Optional[dict[str, Any]]:
    return fetch_one(
        """
        SELECT *
        FROM reconciliation_runs
        ORDER BY run_time DESC, id DESC
        LIMIT 1
        """,
        {},
    )


def get_reconciliation_status_counts_for_run(run_id: int) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT match_status, COUNT(*)::INT AS count
        FROM reconciliation_details
        WHERE run_id = %(run_id)s
        GROUP BY match_status
        ORDER BY count DESC, match_status ASC
        """,
        {"run_id": run_id},
    )


def get_latest_reconciliation_summary() -> dict[str, Any]:
    latest_run = get_latest_reconciliation_run()
    if not latest_run:
        return {
            "latest_run": None,
            "match_status_counts": [],
            "mismatch_count": 0,
        }

    run_id = int(latest_run["id"])
    status_counts = get_reconciliation_status_counts_for_run(run_id)
    mismatch_count = sum(
        int(row.get("count", 0) or 0)
        for row in status_counts
        if str(row.get("match_status", "")).strip() != "matched"
    )

    return {
        "latest_run": latest_run,
        "match_status_counts": status_counts,
        "mismatch_count": mismatch_count,
    }


def get_recent_reconciliation_details(limit: int = 100, run_id: Optional[int] = None) -> list[dict[str, Any]]:
    if run_id is not None:
        return fetch_all(
            """
            SELECT *
            FROM reconciliation_details
            WHERE run_id = %(run_id)s
            ORDER BY id DESC
            LIMIT %(limit)s
            """,
            {"run_id": run_id, "limit": limit},
        )

    return fetch_all(
        """
        SELECT *
        FROM reconciliation_details
        ORDER BY id DESC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )


def get_recent_reconciliation_mismatches(limit: int = 100, run_id: Optional[int] = None) -> list[dict[str, Any]]:
    if run_id is not None:
        return fetch_all(
            """
            SELECT *
            FROM reconciliation_details
            WHERE run_id = %(run_id)s
              AND COALESCE(match_status, '') <> 'matched'
            ORDER BY id DESC
            LIMIT %(limit)s
            """,
            {"run_id": run_id, "limit": limit},
        )

    return fetch_all(
        """
        SELECT *
        FROM reconciliation_details
        WHERE COALESCE(match_status, '') <> 'matched'
        ORDER BY id DESC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )


def get_recent_alpaca_api_logs(limit: int = 100) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT *
        FROM alpaca_api_logs
        ORDER BY logged_at DESC, id DESC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )


def get_recent_alpaca_api_errors(limit: int = 100) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT *
        FROM alpaca_api_logs
        WHERE COALESCE(success, FALSE) = FALSE
           OR COALESCE(status_code, 0) >= 400
           OR COALESCE(error_message, '') <> ''
        ORDER BY logged_at DESC, id DESC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )



def get_latest_scan_run() -> Optional[dict[str, Any]]:
    return fetch_one(
        """
        SELECT *
        FROM scan_runs
        ORDER BY scan_time DESC, id DESC
        LIMIT 1
        """,
        {},
    )



def get_recent_broker_orders(limit: int = 100) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT *
        FROM broker_orders
        ORDER BY COALESCE(submitted_at, created_at) DESC, id DESC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )



def get_broker_order_status_counts() -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT COALESCE(status, '') AS status, COUNT(*)::INT AS count
        FROM broker_orders
        GROUP BY COALESCE(status, '')
        ORDER BY count DESC, status ASC
        """,
        {},
    )



def get_recent_trade_event_rows(limit: int = 100) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT *
        FROM trade_events
        ORDER BY event_time DESC, id DESC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )



def get_trade_event_counts_by_type() -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT event_type, COUNT(*)::INT AS count
        FROM trade_events
        GROUP BY event_type
        ORDER BY count DESC, event_type ASC
        """,
        {},
    )



def get_open_trade_events(limit: int = 100) -> list[dict[str, Any]]:
    return fetch_all(
        """
        WITH ranked AS (
            SELECT
                te.*,
                ROW_NUMBER() OVER (
                    PARTITION BY COALESCE(NULLIF(parent_order_id, ''), NULLIF(order_id, ''), symbol)
                    ORDER BY event_time DESC, id DESC
                ) AS rn
            FROM trade_events te
        )
        SELECT *
        FROM ranked
        WHERE rn = 1
          AND UPPER(COALESCE(status, '')) = 'OPEN'
        ORDER BY event_time DESC, id DESC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )



def get_closed_trade_events(limit: int = 100) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT *
        FROM trade_events
        WHERE UPPER(COALESCE(status, '')) = 'CLOSED'
        ORDER BY event_time DESC, id DESC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )



def _to_optional_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None



def _trade_group_key(row: dict[str, Any]) -> str:
    parent_order_id = str(row.get("parent_order_id") or "").strip()
    order_id = str(row.get("order_id") or "").strip()
    symbol = str(row.get("symbol") or "").strip().upper()
    return parent_order_id or order_id or symbol



def _infer_trade_direction(side: Optional[str]) -> str:
    normalized = str(side or "").strip().lower()
    if normalized == "buy":
        return "LONG"
    if normalized == "sell":
        return "SHORT"
    return "UNKNOWN"



def _compute_realized_pnl(
    *,
    entry_price: Optional[float],
    exit_price: Optional[float],
    shares: Optional[float],
    side: Optional[str],
) -> Optional[float]:
    if entry_price is None or exit_price is None or shares is None:
        return None

    normalized = str(side or "").strip().lower()
    if normalized == "buy":
        return round((exit_price - entry_price) * shares, 6)
    if normalized == "sell":
        return round((entry_price - exit_price) * shares, 6)
    return None



def _compute_pnl_percent(
    *,
    entry_price: Optional[float],
    exit_price: Optional[float],
    side: Optional[str],
) -> Optional[float]:
    if entry_price in (None, 0) or exit_price is None:
        return None

    normalized = str(side or "").strip().lower()
    if normalized == "buy":
        return round(((exit_price - entry_price) / entry_price) * 100.0, 6)
    if normalized == "sell":
        return round(((entry_price - exit_price) / entry_price) * 100.0, 6)
    return None



def _build_trade_lifecycle_rows_from_events(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = _trade_group_key(row)
        if not key:
            continue
        grouped.setdefault(key, []).append(row)

    lifecycle_rows: list[dict[str, Any]] = []

    for key, events in grouped.items():
        sorted_events = sorted(
            events,
            key=lambda item: (
                item.get("event_time") or "",
                item.get("id") or 0,
            ),
        )

        open_event = next(
            (event for event in sorted_events if str(event.get("event_type", "")).strip().upper() == "OPEN"),
            None,
        )
        latest_event = sorted_events[-1] if sorted_events else None
        close_event = None
        for event in reversed(sorted_events):
            if str(event.get("status", "")).strip().upper() == "CLOSED":
                close_event = event
                break

        entry_event = open_event or sorted_events[0]
        exit_event = close_event

        entry_price = _to_optional_float(entry_event.get("price")) if entry_event else None
        exit_price = _to_optional_float(exit_event.get("price")) if exit_event else None
        shares = _to_optional_float((entry_event or latest_event or {}).get("shares"))
        side = (entry_event or latest_event or {}).get("side")
        direction = _infer_trade_direction(side)
        realized_pnl = _compute_realized_pnl(
            entry_price=entry_price,
            exit_price=exit_price,
            shares=shares,
            side=side,
        )
        realized_pnl_percent = _compute_pnl_percent(
            entry_price=entry_price,
            exit_price=exit_price,
            side=side,
        )

        entry_time = entry_event.get("event_time") if entry_event else None
        exit_time = exit_event.get("event_time") if exit_event else None
        duration_minutes = None
        if isinstance(entry_time, datetime) and isinstance(exit_time, datetime):
            duration_minutes = round((exit_time - entry_time).total_seconds() / 60.0, 2)

        lifecycle_rows.append({
            "trade_key": key,
            "symbol": (entry_event or latest_event or {}).get("symbol"),
            "mode": (entry_event or latest_event or {}).get("mode"),
            "side": side,
            "direction": direction,
            "status": "CLOSED" if exit_event else "OPEN",
            "entry_event_type": entry_event.get("event_type") if entry_event else None,
            "exit_event_type": exit_event.get("event_type") if exit_event else None,
            "entry_time": entry_time,
            "exit_time": exit_time,
            "duration_minutes": duration_minutes,
            "shares": shares,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "realized_pnl": realized_pnl,
            "realized_pnl_percent": realized_pnl_percent,
            "exit_reason": exit_event.get("event_type") if exit_event else None,
            "order_id": (entry_event or latest_event or {}).get("order_id"),
            "parent_order_id": (entry_event or latest_event or {}).get("parent_order_id"),
            "signal_info": None,
            "event_count": len(sorted_events),
        })

    lifecycle_rows.sort(
        key=lambda item: (
            item.get("entry_time") or datetime.min,
            item.get("trade_key") or "",
        ),
        reverse=True,
    )
    return lifecycle_rows



def get_trade_lifecycle_rows(limit: int = 100, status: Optional[str] = None) -> list[dict[str, Any]]:
    rows = fetch_all(
        """
        SELECT *
        FROM trade_events
        ORDER BY event_time DESC, id DESC
        LIMIT %(limit)s
        """,
        {"limit": max(limit * 5, 500)},
    )
    lifecycle_rows = _build_trade_lifecycle_rows_from_events(rows)

    if status:
        normalized_status = str(status).strip().upper()
        lifecycle_rows = [
            row for row in lifecycle_rows
            if str(row.get("status", "")).strip().upper() == normalized_status
        ]

    return lifecycle_rows[:limit]



def get_trade_lifecycle_summary(limit: int = 1000) -> dict[str, Any]:
    rows = get_trade_lifecycle_rows(limit=limit)
    closed_rows = [row for row in rows if str(row.get("status", "")).strip().upper() == "CLOSED"]
    pnl_values = [row["realized_pnl"] for row in closed_rows if row.get("realized_pnl") is not None]

    wins = [value for value in pnl_values if value > 0]
    losses = [value for value in pnl_values if value < 0]

    return {
        "trade_count": len(rows),
        "closed_trade_count": len(closed_rows),
        "open_trade_count": len([row for row in rows if str(row.get("status", "")).strip().upper() == "OPEN"]),
        "realized_pnl_total": round(sum(pnl_values), 6) if pnl_values else 0.0,
        "winning_trade_count": len(wins),
        "losing_trade_count": len(losses),
        "flat_trade_count": len([value for value in pnl_values if value == 0]),
        "average_realized_pnl": round(sum(pnl_values) / len(pnl_values), 6) if pnl_values else None,
        "best_trade_pnl": max(pnl_values) if pnl_values else None,
        "worst_trade_pnl": min(pnl_values) if pnl_values else None,
    }



def get_latest_scan_summary() -> dict[str, Any]:
    latest_scan = get_latest_scan_run()
    return {
        "latest_scan": latest_scan,
        "scan_runs_count": get_table_row_count("scan_runs"),
    }


def get_table_row_count(table_name: str) -> int:
    allowed_tables = {
        "scan_runs",
        "trade_events",
        "broker_orders",
        "reconciliation_runs",
        "reconciliation_details",
        "alpaca_api_logs",
        "trade_lifecycles",
    }
    normalized_table_name = str(table_name).strip()
    if normalized_table_name not in allowed_tables:
        raise ValueError(f"Unsupported table for count: {normalized_table_name}")

    row = fetch_one(f"SELECT COUNT(*)::INT AS count FROM {normalized_table_name}", {})
    return int(row["count"]) if row else 0


def get_ops_summary() -> dict[str, Any]:
    latest_run = get_latest_reconciliation_run()
    return {
        "scan_runs_count": get_table_row_count("scan_runs"),
        "trade_events_count": get_table_row_count("trade_events"),
        "broker_orders_count": get_table_row_count("broker_orders"),
        "reconciliation_runs_count": get_table_row_count("reconciliation_runs"),
        "reconciliation_details_count": get_table_row_count("reconciliation_details"),
        "alpaca_api_logs_count": get_table_row_count("alpaca_api_logs"),
        "open_trade_events_count": len(get_open_trade_events(limit=1000)),
        "closed_trade_events_count": len(get_closed_trade_events(limit=1000)),
        "latest_scan_run": get_latest_scan_run(),
        "latest_reconciliation_run_id": latest_run.get("id") if latest_run else None,
        "latest_reconciliation_run_time": latest_run.get("run_time") if latest_run else None,
        "broker_order_status_counts": get_broker_order_status_counts(),
        "trade_event_type_counts": get_trade_event_counts_by_type(),
    }


def insert_alpaca_api_log(
    logged_at: datetime,
    method: str,
    url: str,
    params: Optional[dict[str, Any]] = None,
    request_body: Optional[dict[str, Any]] = None,
    status_code: Optional[int] = None,
    response_body: Optional[str] = None,
    success: Optional[bool] = None,
    error_message: Optional[str] = None,
    duration_ms: Optional[int] = None,
) -> None:
    execute(
        """
        INSERT INTO alpaca_api_logs (
            logged_at,
            method,
            url,
            params_json,
            request_body_json,
            status_code,
            response_body,
            success,
            error_message,
            duration_ms
        )
        VALUES (
            %(logged_at)s,
            %(method)s,
            %(url)s,
            %(params_json)s,
            %(request_body_json)s,
            %(status_code)s,
            %(response_body)s,
            %(success)s,
            %(error_message)s,
            %(duration_ms)s
        )
        """,
        {
            "logged_at": logged_at,
            "method": method,
            "url": url,
            "params_json": json.dumps(params, sort_keys=True) if params is not None else None,
            "request_body_json": json.dumps(request_body, sort_keys=True) if request_body is not None else None,
            "status_code": status_code,
            "response_body": response_body,
            "success": success,
            "error_message": error_message,
            "duration_ms": duration_ms,
        },
    )



def upsert_trade_lifecycle(
    trade_key: str,
    symbol: Optional[str] = None,
    mode: Optional[str] = None,
    side: Optional[str] = None,
    direction: Optional[str] = None,
    status: Optional[str] = None,
    entry_time: Optional[datetime] = None,
    entry_price: Optional[float] = None,
    exit_time: Optional[datetime] = None,
    exit_price: Optional[float] = None,
    stop_price: Optional[float] = None,
    target_price: Optional[float] = None,
    exit_reason: Optional[str] = None,
    shares: Optional[float] = None,
    realized_pnl: Optional[float] = None,
    realized_pnl_percent: Optional[float] = None,
    duration_minutes: Optional[float] = None,
    signal_timestamp: Optional[datetime] = None,
    signal_entry: Optional[float] = None,
    signal_stop: Optional[float] = None,
    signal_target: Optional[float] = None,
    signal_confidence: Optional[float] = None,
    order_id: Optional[str] = None,
    parent_order_id: Optional[str] = None,
    exit_order_id: Optional[str] = None,
) -> None:
    normalized_trade_key = _normalize_text(trade_key)
    if not normalized_trade_key:
        raise ValueError("trade_key is required")

    existing = fetch_one(
        """
        SELECT id
        FROM trade_lifecycles
        WHERE trade_key = %(trade_key)s
        LIMIT 1
        """,
        {"trade_key": normalized_trade_key},
    )

    params = {
        "trade_key": normalized_trade_key,
        "symbol": symbol,
        "mode": mode,
        "side": side,
        "direction": direction,
        "status": status,
        "entry_time": entry_time,
        "entry_price": entry_price,
        "exit_time": exit_time,
        "exit_price": exit_price,
        "stop_price": stop_price,
        "target_price": target_price,
        "exit_reason": exit_reason,
        "shares": shares,
        "realized_pnl": realized_pnl,
        "realized_pnl_percent": realized_pnl_percent,
        "duration_minutes": duration_minutes,
        "signal_timestamp": signal_timestamp,
        "signal_entry": signal_entry,
        "signal_stop": signal_stop,
        "signal_target": signal_target,
        "signal_confidence": signal_confidence,
        "order_id": order_id,
        "parent_order_id": parent_order_id,
        "exit_order_id": exit_order_id,
    }

    if existing:
        params["id"] = existing["id"]
        execute(
            """
            UPDATE trade_lifecycles
            SET symbol = %(symbol)s,
                mode = %(mode)s,
                side = %(side)s,
                direction = %(direction)s,
                status = %(status)s,
                entry_time = %(entry_time)s,
                entry_price = %(entry_price)s,
                exit_time = %(exit_time)s,
                exit_price = %(exit_price)s,
                stop_price = %(stop_price)s,
                target_price = %(target_price)s,
                exit_reason = %(exit_reason)s,
                shares = %(shares)s,
                realized_pnl = %(realized_pnl)s,
                realized_pnl_percent = %(realized_pnl_percent)s,
                duration_minutes = %(duration_minutes)s,
                signal_timestamp = %(signal_timestamp)s,
                signal_entry = %(signal_entry)s,
                signal_stop = %(signal_stop)s,
                signal_target = %(signal_target)s,
                signal_confidence = %(signal_confidence)s,
                order_id = %(order_id)s,
                parent_order_id = %(parent_order_id)s,
                exit_order_id = %(exit_order_id)s,
                updated_at = NOW()
            WHERE id = %(id)s
            """,
            params,
        )
        return

    execute(
        """
        INSERT INTO trade_lifecycles (
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
        )
        VALUES (
            %(trade_key)s,
            %(symbol)s,
            %(mode)s,
            %(side)s,
            %(direction)s,
            %(status)s,
            %(entry_time)s,
            %(entry_price)s,
            %(exit_time)s,
            %(exit_price)s,
            %(stop_price)s,
            %(target_price)s,
            %(exit_reason)s,
            %(shares)s,
            %(realized_pnl)s,
            %(realized_pnl_percent)s,
            %(duration_minutes)s,
            %(signal_timestamp)s,
            %(signal_entry)s,
            %(signal_stop)s,
            %(signal_target)s,
            %(signal_confidence)s,
            %(order_id)s,
            %(parent_order_id)s,
            %(exit_order_id)s
        )
        """,
        params,
    )



def get_trade_lifecycles(limit: int = 100, status: Optional[str] = None) -> list[dict[str, Any]]:
    if status:
        return fetch_all(
            """
            SELECT *
            FROM trade_lifecycles
            WHERE UPPER(COALESCE(status, '')) = %(status)s
            ORDER BY COALESCE(entry_time, created_at) DESC, id DESC
            LIMIT %(limit)s
            """,
            {"status": str(status).strip().upper(), "limit": limit},
        )

    return fetch_all(
        """
        SELECT *
        FROM trade_lifecycles
        ORDER BY COALESCE(entry_time, created_at) DESC, id DESC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )



def get_trade_lifecycle_summary_from_table(limit: int = 1000) -> dict[str, Any]:
    rows = get_trade_lifecycles(limit=limit)
    closed_rows = [row for row in rows if str(row.get("status", "")).strip().upper() == "CLOSED"]
    pnl_values = [
        _to_optional_float(row.get("realized_pnl"))
        for row in closed_rows
        if _to_optional_float(row.get("realized_pnl")) is not None
    ]

    wins = [value for value in pnl_values if value > 0]
    losses = [value for value in pnl_values if value < 0]
    flats = [value for value in pnl_values if value == 0]

    return {
        "trade_count": len(rows),
        "closed_trade_count": len(closed_rows),
        "open_trade_count": len([row for row in rows if str(row.get("status", "")).strip().upper() == "OPEN"]),
        "realized_pnl_total": round(sum(pnl_values), 6) if pnl_values else 0.0,
        "winning_trade_count": len(wins),
        "losing_trade_count": len(losses),
        "flat_trade_count": len(flats),
        "average_realized_pnl": round(sum(pnl_values) / len(pnl_values), 6) if pnl_values else None,
        "best_trade_pnl": max(pnl_values) if pnl_values else None,
        "worst_trade_pnl": min(pnl_values) if pnl_values else None,
    }


# === DB-first analytics helpers for dashboard ===

def get_trade_lifecycles_for_date(target_date: str, limit: int = 1000) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT *
        FROM trade_lifecycles
        WHERE (
            entry_time::date = %(target_date)s::date
            OR exit_time::date = %(target_date)s::date
        )
        ORDER BY COALESCE(entry_time, created_at) DESC, id DESC
        LIMIT %(limit)s
        """,
        {"target_date": target_date, "limit": limit},
    )


def get_trade_lifecycle_summary_for_date(target_date: str, limit: int = 5000) -> dict[str, Any]:
    rows = get_trade_lifecycles_for_date(target_date=target_date, limit=limit)
    closed_rows = [row for row in rows if str(row.get("status", "")).strip().upper() == "CLOSED"]
    open_rows = [row for row in rows if str(row.get("status", "")).strip().upper() == "OPEN"]

    pnl_values = [
        _to_optional_float(row.get("realized_pnl"))
        for row in closed_rows
        if _to_optional_float(row.get("realized_pnl")) is not None
    ]
    duration_values = [
        _to_optional_float(row.get("duration_minutes"))
        for row in closed_rows
        if _to_optional_float(row.get("duration_minutes")) is not None
    ]

    wins = [value for value in pnl_values if value > 0]
    losses = [value for value in pnl_values if value < 0]
    flats = [value for value in pnl_values if value == 0]

    return {
        "date": target_date,
        "trade_count": len(rows),
        "closed_trade_count": len(closed_rows),
        "open_trade_count": len(open_rows),
        "realized_pnl_total": round(sum(pnl_values), 6) if pnl_values else 0.0,
        "winning_trade_count": len(wins),
        "losing_trade_count": len(losses),
        "flat_trade_count": len(flats),
        "win_rate_percent": round((len(wins) / len(closed_rows)) * 100.0, 6) if closed_rows else None,
        "average_realized_pnl": round(sum(pnl_values) / len(pnl_values), 6) if pnl_values else None,
        "best_trade_pnl": max(pnl_values) if pnl_values else None,
        "worst_trade_pnl": min(pnl_values) if pnl_values else None,
        "average_duration_minutes": round(sum(duration_values) / len(duration_values), 6) if duration_values else None,
    }


def get_symbol_performance(limit: int = 100) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT
            symbol,
            COUNT(*)::INT AS trade_count,
            COUNT(*) FILTER (WHERE UPPER(COALESCE(status, '')) = 'CLOSED')::INT AS closed_trade_count,
            COUNT(*) FILTER (WHERE COALESCE(realized_pnl, 0) > 0)::INT AS winning_trade_count,
            COUNT(*) FILTER (WHERE COALESCE(realized_pnl, 0) < 0)::INT AS losing_trade_count,
            ROUND(COALESCE(SUM(realized_pnl), 0)::numeric, 6) AS realized_pnl_total,
            ROUND(AVG(realized_pnl)::numeric, 6) AS average_realized_pnl,
            ROUND(AVG(duration_minutes)::numeric, 6) AS average_duration_minutes
        FROM trade_lifecycles
        GROUP BY symbol
        ORDER BY realized_pnl_total DESC, symbol ASC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )


def get_mode_performance(limit: int = 100) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT
            COALESCE(mode, '') AS mode,
            COUNT(*)::INT AS trade_count,
            COUNT(*) FILTER (WHERE UPPER(COALESCE(status, '')) = 'CLOSED')::INT AS closed_trade_count,
            COUNT(*) FILTER (WHERE COALESCE(realized_pnl, 0) > 0)::INT AS winning_trade_count,
            COUNT(*) FILTER (WHERE COALESCE(realized_pnl, 0) < 0)::INT AS losing_trade_count,
            ROUND(COALESCE(SUM(realized_pnl), 0)::numeric, 6) AS realized_pnl_total,
            ROUND(AVG(realized_pnl)::numeric, 6) AS average_realized_pnl,
            ROUND(AVG(duration_minutes)::numeric, 6) AS average_duration_minutes
        FROM trade_lifecycles
        GROUP BY COALESCE(mode, '')
        ORDER BY realized_pnl_total DESC, mode ASC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )


def get_exit_reason_breakdown(limit: int = 100) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT
            COALESCE(exit_reason, '') AS exit_reason,
            COUNT(*)::INT AS trade_count,
            ROUND(COALESCE(SUM(realized_pnl), 0)::numeric, 6) AS realized_pnl_total,
            ROUND(AVG(realized_pnl)::numeric, 6) AS average_realized_pnl
        FROM trade_lifecycles
        WHERE UPPER(COALESCE(status, '')) = 'CLOSED'
        GROUP BY COALESCE(exit_reason, '')
        ORDER BY trade_count DESC, exit_reason ASC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )


def get_hourly_performance(limit: int = 100) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT
            EXTRACT(HOUR FROM entry_time)::INT AS entry_hour_utc,
            COUNT(*)::INT AS trade_count,
            COUNT(*) FILTER (WHERE UPPER(COALESCE(status, '')) = 'CLOSED')::INT AS closed_trade_count,
            ROUND(COALESCE(SUM(realized_pnl), 0)::numeric, 6) AS realized_pnl_total,
            ROUND(AVG(realized_pnl)::numeric, 6) AS average_realized_pnl,
            ROUND(AVG(duration_minutes)::numeric, 6) AS average_duration_minutes
        FROM trade_lifecycles
        WHERE entry_time IS NOT NULL
        GROUP BY EXTRACT(HOUR FROM entry_time)
        ORDER BY entry_hour_utc ASC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )

def get_equity_curve(limit: int = 5000) -> list[dict[str, Any]]:
    rows = fetch_all(
        """
        SELECT
            COALESCE(exit_time, entry_time, created_at) AS timestamp,
            COALESCE(realized_pnl, 0) AS realized_pnl
        FROM trade_lifecycles
        ORDER BY COALESCE(exit_time, entry_time, created_at) ASC, id ASC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )

    cumulative = 0.0
    curve = []
    for row in rows:
        realized_pnl = _to_optional_float(row.get("realized_pnl")) or 0.0
        cumulative += realized_pnl
        timestamp = row.get("timestamp")
        curve.append({
            "timestamp": timestamp.isoformat() if hasattr(timestamp, "isoformat") else str(timestamp),
            "realized_pnl": round(realized_pnl, 6),
            "cumulative_pnl": round(cumulative, 6),
        })

    return curve

def get_dashboard_summary(target_date: Optional[str] = None) -> dict[str, Any]:
    base_summary = (
        get_trade_lifecycle_summary_for_date(target_date=target_date, limit=5000)
        if target_date
        else get_trade_lifecycle_summary_from_table(limit=5000)
    )

    symbol_performance = get_symbol_performance(limit=10)
    mode_performance = get_mode_performance(limit=10)
    exit_reason_breakdown = get_exit_reason_breakdown(limit=20)
    hourly_performance = get_hourly_performance(limit=24)
    equity_curve = get_equity_curve(limit=5000)

    return {
        "summary": base_summary,
        "top_symbols": symbol_performance,
        "mode_performance": mode_performance,
        "exit_reason_breakdown": exit_reason_breakdown,
        "hourly_performance": hourly_performance,
        "equity_curve": equity_curve,
        "insights": {
            "best_symbol": (symbol_performance[0] if symbol_performance else None),
            "best_mode": (mode_performance[0] if mode_performance else None),
            "most_common_exit": (exit_reason_breakdown[0] if exit_reason_breakdown else None),
            "best_hour": (
                max(hourly_performance, key=lambda x: x.get("realized_pnl_total", 0))
                if hourly_performance else None
            ),
        }
    }
