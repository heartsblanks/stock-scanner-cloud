from __future__ import annotations

from datetime import datetime
from typing import Optional

from db import execute, fetch_all, fetch_one
from repositories.common import normalize_text


def insert_reconciliation_run(
    run_time: datetime,
    matched_count: int,
    unmatched_count: int,
    notes: Optional[str] = None,
    severity: Optional[str] = None,
    mismatch_count: Optional[int] = None,
    run_started_at: Optional[datetime] = None,
    run_completed_at: Optional[datetime] = None,
) -> None:
    resolved_mismatch_count = unmatched_count if mismatch_count is None else mismatch_count
    resolved_run_started_at = run_time if run_started_at is None else run_started_at
    resolved_run_completed_at = run_time if run_completed_at is None else run_completed_at
    existing = fetch_one(
        """
        SELECT id
        FROM reconciliation_runs
        WHERE run_time = %(run_time)s
          AND matched_count = %(matched_count)s
          AND unmatched_count = %(unmatched_count)s
          AND COALESCE(mismatch_count, -1) = %(mismatch_count)s
          AND COALESCE(severity, '') = %(severity)s
          AND COALESCE(run_started_at, TIMESTAMPTZ '1970-01-01 00:00:00+00') = %(run_started_at)s
          AND COALESCE(run_completed_at, TIMESTAMPTZ '1970-01-01 00:00:00+00') = %(run_completed_at)s
          AND COALESCE(notes, '') = %(notes)s
        LIMIT 1
        """,
        {
            "run_time": run_time,
            "matched_count": matched_count,
            "unmatched_count": unmatched_count,
            "mismatch_count": resolved_mismatch_count,
            "severity": normalize_text(severity),
            "run_started_at": resolved_run_started_at,
            "run_completed_at": resolved_run_completed_at,
            "notes": normalize_text(notes),
        },
    )
    if existing:
        return
    execute(
        """
        INSERT INTO reconciliation_runs (
            run_time, matched_count, unmatched_count, notes, severity, mismatch_count, run_started_at, run_completed_at
        ) VALUES (
            %(run_time)s, %(matched_count)s, %(unmatched_count)s, %(notes)s, %(severity)s, %(mismatch_count)s, %(run_started_at)s, %(run_completed_at)s
        )
        """,
        {
            "run_time": run_time,
            "matched_count": matched_count,
            "unmatched_count": unmatched_count,
            "notes": notes,
            "severity": severity,
            "mismatch_count": resolved_mismatch_count,
            "run_started_at": resolved_run_started_at,
            "run_completed_at": resolved_run_completed_at,
        },
    )


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
            "broker_parent_order_id": normalize_text(broker_parent_order_id),
            "symbol": normalize_text(symbol),
            "mode": normalize_text(mode),
            "client_order_id": normalize_text(client_order_id),
            "local_entry_timestamp_utc_match": local_entry_timestamp_utc or datetime(1970, 1, 1),
            "local_exit_timestamp_utc_match": local_exit_timestamp_utc or datetime(1970, 1, 1),
            "local_entry_price_match": local_entry_price if local_entry_price is not None else -1,
            "alpaca_entry_price_match": alpaca_entry_price if alpaca_entry_price is not None else -1,
            "local_exit_price_match": local_exit_price if local_exit_price is not None else -1,
            "alpaca_exit_price_match": alpaca_exit_price if alpaca_exit_price is not None else -1,
            "local_shares_match": local_shares if local_shares is not None else -1,
            "alpaca_entry_qty_match": alpaca_entry_qty if alpaca_entry_qty is not None else -1,
            "alpaca_exit_qty_match": alpaca_exit_qty if alpaca_exit_qty is not None else -1,
            "local_exit_reason": normalize_text(local_exit_reason),
            "alpaca_exit_reason": normalize_text(alpaca_exit_reason),
            "alpaca_exit_order_id": normalize_text(alpaca_exit_order_id),
            "entry_price_diff_match": entry_price_diff if entry_price_diff is not None else -1,
            "exit_price_diff_match": exit_price_diff if exit_price_diff is not None else -1,
            "match_status": normalize_text(match_status),
        },
    )
    if existing:
        return
    execute(
        """
        INSERT INTO reconciliation_details (
            run_id, broker_parent_order_id, symbol, mode, client_order_id, local_entry_timestamp_utc,
            local_exit_timestamp_utc, local_entry_price, alpaca_entry_price, local_exit_price, alpaca_exit_price,
            local_shares, alpaca_entry_qty, alpaca_exit_qty, local_exit_reason, alpaca_exit_reason,
            alpaca_exit_order_id, entry_price_diff, exit_price_diff, match_status
        ) VALUES (
            %(run_id)s, %(broker_parent_order_id)s, %(symbol)s, %(mode)s, %(client_order_id)s, %(local_entry_timestamp_utc)s,
            %(local_exit_timestamp_utc)s, %(local_entry_price)s, %(alpaca_entry_price)s, %(local_exit_price)s, %(alpaca_exit_price)s,
            %(local_shares)s, %(alpaca_entry_qty)s, %(alpaca_exit_qty)s, %(local_exit_reason)s, %(alpaca_exit_reason)s,
            %(alpaca_exit_order_id)s, %(entry_price_diff)s, %(exit_price_diff)s, %(match_status)s
        )
        """,
        locals(),
    )


def get_reconciliation_details_for_run(run_id: int) -> list[dict]:
    return fetch_all("SELECT * FROM reconciliation_details WHERE run_id = %(run_id)s ORDER BY id ASC", {"run_id": run_id})


def get_latest_reconciliation_run() -> Optional[dict]:
    return fetch_one("SELECT * FROM reconciliation_runs ORDER BY run_time DESC, id DESC LIMIT 1", {})


def get_reconciliation_status_counts_for_run(run_id: int) -> list[dict]:
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


def get_latest_reconciliation_summary() -> dict:
    latest_run = get_latest_reconciliation_run()
    if not latest_run:
        return {"latest_run": None, "match_status_counts": [], "mismatch_count": 0}
    run_id = int(latest_run["id"])
    status_counts = get_reconciliation_status_counts_for_run(run_id)
    mismatch_count = sum(int(row.get("count", 0) or 0) for row in status_counts if str(row.get("match_status", "")).strip() != "matched")
    return {"latest_run": latest_run, "match_status_counts": status_counts, "mismatch_count": mismatch_count}


def get_recent_reconciliation_details(limit: int = 100, run_id: Optional[int] = None) -> list[dict]:
    if run_id is not None:
        return fetch_all("SELECT * FROM reconciliation_details WHERE run_id = %(run_id)s ORDER BY id DESC LIMIT %(limit)s", {"run_id": run_id, "limit": limit})
    return fetch_all("SELECT * FROM reconciliation_details ORDER BY id DESC LIMIT %(limit)s", {"limit": limit})


def get_recent_reconciliation_mismatches(limit: int = 100, run_id: Optional[int] = None) -> list[dict]:
    if run_id is not None:
        return fetch_all(
            "SELECT * FROM reconciliation_details WHERE run_id = %(run_id)s AND COALESCE(match_status, '') <> 'matched' ORDER BY id DESC LIMIT %(limit)s",
            {"run_id": run_id, "limit": limit},
        )
    return fetch_all(
        "SELECT * FROM reconciliation_details WHERE COALESCE(match_status, '') <> 'matched' ORDER BY id DESC LIMIT %(limit)s",
        {"limit": limit},
    )


def get_reconciliation_runs(limit: int = 20) -> list[dict]:
    return fetch_all(
        """
        SELECT id, run_time, matched_count, unmatched_count, notes, created_at, severity, mismatch_count, run_started_at, run_completed_at
        FROM reconciliation_runs
        ORDER BY run_time DESC, id DESC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )
