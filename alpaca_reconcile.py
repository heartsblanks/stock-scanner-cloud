from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from io import StringIO
from datetime import datetime
from pathlib import Path
from typing import Any

 
from google.cloud import storage
from storage import insert_broker_order



GCS_BUCKET_NAME = os.getenv("TRADE_LOG_BUCKET", "stock-scanner-490821-logs")
GCS_TRADES_OBJECT = os.getenv("TRADE_LOG_OBJECT", "trades/trades.csv")
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


def get_gcs_csv_rows(bucket_name: str, object_name: str) -> list[dict[str, str]]:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    if not blob.exists():
        raise FileNotFoundError(f"Missing gs://{bucket_name}/{object_name}")

    csv_text = blob.download_as_text(encoding="utf-8")
    return list(csv.DictReader(StringIO(csv_text)))




def fetch_alpaca_orders(limit: int = 500) -> list[dict[str, Any]]:
    return fetch_orders(limit=limit, status="all", nested=True, direction="desc")




def build_local_trade_pairs(rows: list[dict[str, str]]) -> list[LocalTradePair]:
    open_by_parent: dict[str, dict[str, str]] = {}
    pairs: list[LocalTradePair] = []

    for row in rows:
        if str(row.get("trade_source", "")).strip().upper() != "ALPACA_PAPER":
            continue

        parent_id = str(row.get("broker_parent_order_id", "")).strip()
        if not parent_id:
            continue

        event_type = str(row.get("event_type", "")).strip().upper()
        if event_type == "OPEN":
            open_by_parent[parent_id] = row
            continue

        if event_type not in CLOSE_EVENT_TYPES:
            continue

        open_row = open_by_parent.get(parent_id)
        if not open_row:
            continue

        pairs.append(
            LocalTradePair(
                broker_parent_order_id=parent_id,
                symbol=str(open_row.get("symbol", "")).strip().upper(),
                mode=str(open_row.get("mode", "")).strip().lower(),
                entry_timestamp_utc=str(open_row.get("timestamp_utc", "")).strip(),
                exit_timestamp_utc=str(row.get("timestamp_utc", "")).strip(),
                entry_price=to_float(open_row.get("entry_price")),
                exit_price=to_float(row.get("exit_price")),
                shares=to_float(open_row.get("shares")),
                exit_reason=str(row.get("exit_reason", "")).strip().upper(),
                client_order_id=str(open_row.get("notes", "")).split("client_order_id=")[-1].strip() if "client_order_id=" in str(open_row.get("notes", "")) else "",
            )
        )
        del open_by_parent[parent_id]

    return pairs


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


def build_reconciliation_detail_row(pair: LocalTradePair, order: dict[str, Any] | None) -> ReconciliationDetailRow:
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
    alpaca_exit_reason, alpaca_exit_qty, alpaca_exit_price, alpaca_exit_order_id = infer_alpaca_exit(order)

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

    match_status = "matched"
    if alpaca_exit_reason == "UNKNOWN":
        match_status = "exit_not_resolved"
    elif pair.exit_reason and alpaca_exit_reason and pair.exit_reason != alpaca_exit_reason:
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


def reconcile(local_pairs: list[LocalTradePair], alpaca_index: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    for pair in local_pairs:
        order = alpaca_index.get(pair.broker_parent_order_id)
        detail_row = build_reconciliation_detail_row(pair, order)
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


def run_reconciliation() -> tuple[list[dict[str, Any]], Path]:
    local_rows = get_gcs_csv_rows(GCS_BUCKET_NAME, GCS_TRADES_OBJECT)
    alpaca_orders = fetch_alpaca_orders()
    persist_alpaca_orders_to_db(alpaca_orders)

    local_pairs = build_local_trade_pairs(local_rows)
    alpaca_index = build_alpaca_index(alpaca_orders)
    reconciled_rows = reconcile(local_pairs, alpaca_index)
    summary = summarize_reconciliation(reconciled_rows)
    print(f"Reconciliation summary: {summary}", flush=True)

    write_csv(reconciled_rows, OUTPUT_PATH)
    return reconciled_rows, OUTPUT_PATH


def upload_file_to_gcs(local_path: Path, bucket_name: str, object_name: str) -> str:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(str(local_path))
    return f"gs://{bucket_name}/{object_name}"


def main() -> None:
    reconciled_rows, output_path = run_reconciliation()
    print(
        f"Wrote {len(reconciled_rows)} rows to {output_path} using "
        f"gs://{GCS_BUCKET_NAME}/{GCS_TRADES_OBJECT} and Alpaca API orders"
    )


if __name__ == "__main__":
    main()