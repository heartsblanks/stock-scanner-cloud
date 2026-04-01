from __future__ import annotations

import csv
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from google.cloud import storage

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from storage import insert_broker_order, get_recent_trade_event_rows

OUTPUT_PATH = Path("alpaca_reconciliation.csv")

from alpaca.alpaca_orders import fetch_orders
CLOSE_EVENT_TYPES = {"STOP_HIT", "TARGET_HIT", "MANUAL_CLOSE"}


@dataclass
class LocalTradePair:
    broker_parent_order_id: str
    symbol: str
    mode: str
    entry_timestamp_utc: str
    exit_timestamp_utc: str
    entry_price: float | None
    exit_price: float | None
    shares: float | None
    exit_reason: str
    client_order_id: str


@dataclass
class ReconciliationDetailRow:
    broker_parent_order_id: str
    symbol: str
    mode: str
    client_order_id: str
    local_entry_timestamp_utc: str
    local_exit_timestamp_utc: str
    local_entry_price: float | None
    alpaca_entry_price: float | None
    local_exit_price: float | None
    alpaca_exit_price: float | None
    local_shares: float | None
    alpaca_entry_qty: float | None
    alpaca_exit_qty: float | None
    local_exit_reason: str
    alpaca_exit_reason: str
    alpaca_exit_order_id: str
    entry_price_diff: float | str
    exit_price_diff: float | str
    match_status: str


def to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_alpaca_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def fetch_alpaca_orders(limit: int = 500) -> list[dict[str, Any]]:
    return fetch_orders(limit=limit, status="all", nested=True, direction="desc")


def get_db_trade_event_rows(limit: int = 5000) -> list[dict[str, Any]]:
    try:
        return get_recent_trade_event_rows(limit=limit)
    except Exception as e:
        print(f"Failed to read trade events from DB: {e}", flush=True)
        return []


def build_local_trade_pairs(rows: list[dict[str, Any]]) -> list[LocalTradePair]:
    open_by_parent: dict[str, dict[str, Any]] = {}
    pairs: list[LocalTradePair] = []

    ordered_rows = sorted(
        rows or [],
        key=lambda row: (
            str(row.get("event_time") or row.get("timestamp_utc") or ""),
            str(row.get("parent_order_id") or row.get("broker_parent_order_id") or ""),
        ),
    )

    for row in ordered_rows:
        parent_id = str(
            row.get("parent_order_id")
            or row.get("broker_parent_order_id")
            or row.get("order_id")
            or row.get("broker_order_id")
            or ""
        ).strip()
        if not parent_id:
            continue

        event_type = str(row.get("event_type", "")).strip().upper()
        status = str(row.get("status", "")).strip().upper()

        if event_type == "OPEN" and status == "OPEN":
            open_by_parent[parent_id] = row
            continue

        if status != "CLOSED" and event_type not in CLOSE_EVENT_TYPES and event_type != "EXTERNAL_EXIT":
            continue

        open_row = open_by_parent.get(parent_id)
        if not open_row:
            continue

        event_time_value = row.get("event_time") or row.get("timestamp_utc") or ""
        entry_time_value = open_row.get("event_time") or open_row.get("timestamp_utc") or ""
        exit_reason_value = str(row.get("event_type") or row.get("exit_reason") or "").strip().upper()
        if exit_reason_value == "MANUAL_CLOSE":
            exit_reason_value = str(row.get("exit_reason") or "MANUAL_CLOSE").strip().upper() or "MANUAL_CLOSE"

        client_order_id = str(row.get("client_order_id") or open_row.get("client_order_id") or "").strip()
        if not client_order_id:
            notes_text = str(open_row.get("notes", ""))
            if "client_order_id=" in notes_text:
                client_order_id = notes_text.split("client_order_id=")[-1].strip()

        pairs.append(
            LocalTradePair(
                broker_parent_order_id=parent_id,
                symbol=str(open_row.get("symbol", "")).strip().upper(),
                mode=str(open_row.get("mode", "")).strip().lower(),
                entry_timestamp_utc=str(entry_time_value).strip(),
                exit_timestamp_utc=str(event_time_value).strip(),
                entry_price=to_float(open_row.get("price") if open_row.get("price") not in (None, "") else open_row.get("entry_price")),
                exit_price=to_float(row.get("price") if row.get("price") not in (None, "") else row.get("exit_price")),
                shares=to_float(open_row.get("qty") if open_row.get("qty") not in (None, "") else open_row.get("shares")),
                exit_reason=exit_reason_value,
                client_order_id=client_order_id,
            )
        )
        del open_by_parent[parent_id]

    return pairs


def _find_external_exit_order(parent_order: dict[str, Any], all_orders: list[dict[str, Any]]) -> dict[str, Any] | None:
    try:
        symbol = str(parent_order.get("symbol", "")).strip().upper()
        parent_id = str(parent_order.get("id", "")).strip()
        parent_side = str(parent_order.get("side", "")).strip().lower()
        parent_submitted_at = parse_alpaca_datetime(parent_order.get("submitted_at"))
        parent_filled_qty = to_float(parent_order.get("filled_qty"))

        leg_ids = {
            str(leg.get("id", "")).strip()
            for leg in (parent_order.get("legs") or [])
            if str(leg.get("id", "")).strip()
        }

        opposite_side = "sell" if parent_side == "buy" else "buy"

        candidates: list[tuple[int, datetime, str, dict[str, Any]]] = []
        for o in all_orders or []:
            try:
                candidate_id = str(o.get("id", "")).strip()

                if str(o.get("symbol", "")).strip().upper() != symbol:
                    continue
                if not candidate_id or candidate_id == parent_id or candidate_id in leg_ids:
                    continue
                if str(o.get("side", "")).strip().lower() != opposite_side:
                    continue

                filled_qty = to_float(o.get("filled_qty"))
                if filled_qty is None or filled_qty <= 0:
                    continue

                status = str(o.get("status", "")).strip().lower()
                if status != "filled":
                    continue

                submitted_at = parse_alpaca_datetime(o.get("submitted_at"))
                filled_at = parse_alpaca_datetime(o.get("filled_at"))
                effective_time = filled_at or submitted_at
                if not effective_time:
                    continue

                if parent_submitted_at:
                    if submitted_at and submitted_at < parent_submitted_at:
                        continue
                    if filled_at and filled_at < parent_submitted_at:
                        continue

                qty_rank = 0 if parent_filled_qty and abs(filled_qty - parent_filled_qty) < 1e-9 else 1

                candidates.append((qty_rank, effective_time, candidate_id, o))
            except Exception:
                continue

        if not candidates:
            return None

        candidates.sort(key=lambda item: (item[0], item[1], item[2]))
        return candidates[0][3]

    except Exception:
        return None


def infer_alpaca_exit_from_order_set(order: dict[str, Any], all_orders: list[dict[str, Any]]) -> tuple[str, float | None, float | None, str]:
    exit_reason, exit_qty, exit_price, exit_order_id = infer_alpaca_exit(order)
    if exit_reason != "OPEN_ONLY":
        return exit_reason, exit_qty, exit_price, exit_order_id

    external = _find_external_exit_order(order, all_orders)
    if external:
        return (
            "EXTERNAL_EXIT",
            to_float(external.get("filled_qty")),
            to_float(external.get("filled_avg_price")),
            str(external.get("id", "")).strip(),
        )

    return exit_reason, exit_qty, exit_price, exit_order_id


def infer_alpaca_exit(order: dict[str, Any]) -> tuple[str, float | None, float | None, str]:
    legs = order.get("legs") or []
    for leg in legs:
        status = str(leg.get("status", "")).strip().lower()
        if status != "filled":
            continue

        leg_type = str(leg.get("type", "")).strip().lower()
        filled_qty = to_float(leg.get("filled_qty"))
        filled_avg_price = to_float(leg.get("filled_avg_price"))
        leg_id = str(leg.get("id", "")).strip()

        if leg_type == "limit":
            return "TARGET_HIT", filled_qty, filled_avg_price, leg_id
        if leg_type == "stop":
            return "STOP_HIT", filled_qty, filled_avg_price, leg_id

    if str(order.get("status", "")).strip().lower() == "filled":
        return "OPEN_ONLY", to_float(order.get("filled_qty")), to_float(order.get("filled_avg_price")), ""

    return "UNKNOWN", None, None, ""


# Helper to normalize exit reasons so MANUAL_CLOSE and EXTERNAL_EXIT are treated as equivalent
def normalize_exit_reason(reason: Any) -> str:
    normalized = str(reason or "").strip().upper()
    if normalized in {"MANUAL_CLOSE", "EXTERNAL_EXIT"}:
        return "EXTERNAL_EXIT"
    return normalized


def build_alpaca_index(orders: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for order in orders:
        order_id = str(order.get("id", "")).strip()
        if order_id:
            index[order_id] = order
    return index


def flatten_alpaca_orders(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    for order in orders:
        flattened.append(order)
        for leg in order.get("legs") or []:
            flattened.append(leg)
    return flattened



def persist_alpaca_orders_to_db(orders: list[dict[str, Any]]) -> None:
    for order in flatten_alpaca_orders(orders):
        order_id = str(order.get("id", "") or "").strip()
        if not order_id:
            continue
        try:
            insert_broker_order(
                order_id=order_id,
                symbol=str(order.get("symbol", "") or "").strip().upper() or None,
                side=str(order.get("side", "") or "").strip().lower() or None,
                order_type=str(order.get("type", "") or "").strip().lower() or None,
                status=str(order.get("status", "") or "").strip().lower() or None,
                qty=to_float(order.get("qty")),
                filled_qty=to_float(order.get("filled_qty")),
                avg_fill_price=to_float(order.get("filled_avg_price")),
                submitted_at=parse_alpaca_datetime(order.get("submitted_at")),
                filled_at=parse_alpaca_datetime(order.get("filled_at")),
            )
        except Exception as e:
            print(f"DB broker order persist failed for {order_id}: {e}", flush=True)


def build_reconciliation_detail_row(pair: LocalTradePair, order: dict[str, Any] | None, all_orders: list[dict[str, Any]]) -> ReconciliationDetailRow:
    if not order:
        return ReconciliationDetailRow(
            broker_parent_order_id=pair.broker_parent_order_id,
            symbol=pair.symbol,
            mode=pair.mode,
            client_order_id=pair.client_order_id,
            local_entry_timestamp_utc=pair.entry_timestamp_utc,
            local_exit_timestamp_utc=pair.exit_timestamp_utc,
            local_entry_price=pair.entry_price,
            alpaca_entry_price=None,
            local_exit_price=pair.exit_price,
            alpaca_exit_price=None,
            local_shares=pair.shares,
            alpaca_entry_qty=None,
            alpaca_exit_qty=None,
            local_exit_reason=pair.exit_reason,
            alpaca_exit_reason="",
            alpaca_exit_order_id="",
            entry_price_diff="",
            exit_price_diff="",
            match_status="missing_in_alpaca",
        )

    alpaca_entry_price = to_float(order.get("filled_avg_price"))
    alpaca_entry_qty = to_float(order.get("filled_qty"))
    alpaca_exit_reason, alpaca_exit_qty, alpaca_exit_price, alpaca_exit_order_id = infer_alpaca_exit_from_order_set(order, all_orders)

    entry_price_diff = (
        round((pair.entry_price or 0.0) - (alpaca_entry_price or 0.0), 6)
        if pair.entry_price is not None and alpaca_entry_price is not None
        else ""
    )
    exit_price_diff = (
        round((pair.exit_price or 0.0) - (alpaca_exit_price or 0.0), 6)
        if pair.exit_price is not None and alpaca_exit_price is not None
        else ""
    )

    normalized_local_exit_reason = normalize_exit_reason(pair.exit_reason)
    normalized_alpaca_exit_reason = normalize_exit_reason(alpaca_exit_reason)

    match_status = "matched"
    if normalized_alpaca_exit_reason == "UNKNOWN":
        match_status = "exit_not_resolved"
    elif normalized_local_exit_reason and normalized_alpaca_exit_reason and normalized_local_exit_reason != normalized_alpaca_exit_reason:
        match_status = "exit_reason_mismatch"
    elif pair.shares is not None and alpaca_entry_qty is not None and abs(pair.shares - alpaca_entry_qty) > 1e-9:
        match_status = "entry_qty_mismatch"
    elif pair.shares is not None and alpaca_exit_qty is not None and abs(pair.shares - alpaca_exit_qty) > 1e-9:
        match_status = "exit_qty_mismatch"

    return ReconciliationDetailRow(
        broker_parent_order_id=pair.broker_parent_order_id,
        symbol=pair.symbol,
        mode=pair.mode,
        client_order_id=pair.client_order_id,
        local_entry_timestamp_utc=pair.entry_timestamp_utc,
        local_exit_timestamp_utc=pair.exit_timestamp_utc,
        local_entry_price=pair.entry_price,
        alpaca_entry_price=alpaca_entry_price,
        local_exit_price=pair.exit_price,
        alpaca_exit_price=alpaca_exit_price,
        local_shares=pair.shares,
        alpaca_entry_qty=alpaca_entry_qty,
        alpaca_exit_qty=alpaca_exit_qty,
        local_exit_reason=pair.exit_reason,
        alpaca_exit_reason=alpaca_exit_reason,
        alpaca_exit_order_id=alpaca_exit_order_id,
        entry_price_diff=entry_price_diff,
        exit_price_diff=exit_price_diff,
        match_status=match_status,
    )


def reconcile(local_pairs: list[LocalTradePair], alpaca_index: dict[str, dict[str, Any]], alpaca_orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    for pair in local_pairs:
        order = alpaca_index.get(pair.broker_parent_order_id)
        detail_row = build_reconciliation_detail_row(pair, order, alpaca_orders)
        results.append({
            "broker_parent_order_id": detail_row.broker_parent_order_id,
            "symbol": detail_row.symbol,
            "mode": detail_row.mode,
            "client_order_id": detail_row.client_order_id,
            "local_entry_timestamp_utc": detail_row.local_entry_timestamp_utc,
            "local_exit_timestamp_utc": detail_row.local_exit_timestamp_utc,
            "local_entry_price": detail_row.local_entry_price if detail_row.local_entry_price is not None else "",
            "alpaca_entry_price": detail_row.alpaca_entry_price if detail_row.alpaca_entry_price is not None else "",
            "local_exit_price": detail_row.local_exit_price if detail_row.local_exit_price is not None else "",
            "alpaca_exit_price": detail_row.alpaca_exit_price if detail_row.alpaca_exit_price is not None else "",
            "local_shares": detail_row.local_shares if detail_row.local_shares is not None else "",
            "alpaca_entry_qty": detail_row.alpaca_entry_qty if detail_row.alpaca_entry_qty is not None else "",
            "alpaca_exit_qty": detail_row.alpaca_exit_qty if detail_row.alpaca_exit_qty is not None else "",
            "local_exit_reason": detail_row.local_exit_reason,
            "alpaca_exit_reason": detail_row.alpaca_exit_reason,
            "alpaca_exit_order_id": detail_row.alpaca_exit_order_id,
            "entry_price_diff": detail_row.entry_price_diff,
            "exit_price_diff": detail_row.exit_price_diff,
            "match_status": detail_row.match_status,
        })

    return results


def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    headers = [
        "broker_parent_order_id",
        "symbol",
        "mode",
        "client_order_id",
        "local_entry_timestamp_utc",
        "local_exit_timestamp_utc",
        "local_entry_price",
        "alpaca_entry_price",
        "local_exit_price",
        "alpaca_exit_price",
        "local_shares",
        "alpaca_entry_qty",
        "alpaca_exit_qty",
        "local_exit_reason",
        "alpaca_exit_reason",
        "alpaca_exit_order_id",
        "entry_price_diff",
        "exit_price_diff",
        "match_status",
    ]

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def summarize_reconciliation(rows: list[dict[str, Any]]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for row in rows:
        key = str(row.get("match_status", "unknown")).strip() or "unknown"
        summary[key] = summary.get(key, 0) + 1
    return summary



# Helper to determine severity level from summary
def calculate_severity(summary: dict[str, int]) -> tuple[str, int]:
    mismatch_count = 0

    # critical issues
    critical_keys = {"missing_in_alpaca", "missing_in_db"}
    for key in critical_keys:
        mismatch_count += summary.get(key, 0)

    if mismatch_count > 0:
        return "CRITICAL", mismatch_count

    # warning issues
    warning_keys = {
        "exit_reason_mismatch",
        "entry_qty_mismatch",
        "exit_qty_mismatch",
        "exit_not_resolved",
    }
    warning_count = sum(summary.get(k, 0) for k in warning_keys)

    if warning_count > 0:
        return "WARNING", warning_count

    return "OK", 0


def run_reconciliation() -> dict[str, Any]:
    local_rows = get_db_trade_event_rows(limit=5000)
    alpaca_orders = fetch_alpaca_orders()
    persist_alpaca_orders_to_db(alpaca_orders)

    local_pairs = build_local_trade_pairs(local_rows)
    alpaca_index = build_alpaca_index(alpaca_orders)
    reconciled_rows = reconcile(local_pairs, alpaca_index, alpaca_orders)
    summary = summarize_reconciliation(reconciled_rows)

    print(f"Reconciliation summary: {summary}", flush=True)

    write_csv(reconciled_rows, OUTPUT_PATH)

    severity, mismatch_count = calculate_severity(summary)

    return {
        "rows": reconciled_rows,
        "summary": summary,
        "severity": severity,
        "mismatch_count": mismatch_count,
        "file_path": str(OUTPUT_PATH),
        "total_rows": len(reconciled_rows),
    }


def upload_file_to_gcs(local_path: Path, bucket_name: str, object_name: str) -> str:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(str(local_path))
    return f"gs://{bucket_name}/{object_name}"


def main() -> None:
    result = run_reconciliation()
    print(
        f"Wrote {result['total_rows']} rows to {result['file_path']} using "
        f"DB trade events and Alpaca API orders"
    )


if __name__ == "__main__":
    main()
