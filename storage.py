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


def insert_alpaca_api_log(
    logged_at: datetime,
    method: str,
    url: str,
    params: Optional[dict[str, Any]] = None,
    request_body: Optional[dict[str, Any]] = None,
    status_code: Optional[int] = None,
    response_body: Optional[str] = None,
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
            response_body
        )
        VALUES (
            %(logged_at)s,
            %(method)s,
            %(url)s,
            %(params_json)s,
            %(request_body_json)s,
            %(status_code)s,
            %(response_body)s
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
        },
    )
