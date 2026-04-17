

from __future__ import annotations

import csv
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from google.cloud import storage

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.db import fetch_all


CLOSE_EVENT_TYPES = {"STOP_HIT", "TARGET_HIT", "MANUAL_CLOSE"}
CORE_MODES = {"core_one", "core_two", "core_three"}

ANALYSIS_SUMMARY_OUTPUT = Path("trade_analysis_summary.csv")
ANALYSIS_PAIRED_OUTPUT = Path("trade_analysis_paired_trades.csv")
ANALYSIS_GCS_BUCKET = os.getenv("TRADE_ANALYSIS_BUCKET", "stock-scanner-490821-logs")
ANALYSIS_SUMMARY_OBJECT = os.getenv("TRADE_ANALYSIS_SUMMARY_OBJECT", "reports/trade_analysis_summary.csv")
ANALYSIS_PAIRED_OBJECT = os.getenv("TRADE_ANALYSIS_PAIRED_OBJECT", "reports/trade_analysis_paired_trades.csv")

def stringify_db_row(row: dict[str, Any]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in row.items():
        if value is None:
            normalized[key] = ""
        else:
            normalized[key] = str(value)
    return normalized



def get_db_trade_rows() -> list[dict[str, str]]:
    rows = fetch_all(
        """
        SELECT
            event_time AS timestamp_utc,
            event_type,
            symbol,
            '' AS name,
            mode,
            'IBKR_PAPER' AS trade_source,
            COALESCE(NULLIF(broker, ''), 'IBKR') AS broker,
            order_id AS broker_order_id,
            parent_order_id AS broker_parent_order_id,
            status,
            shares,
            price,
            '' AS entry_price,
            '' AS stop_price,
            '' AS target_price,
            '' AS exit_price,
            '' AS exit_reason,
            '' AS notes,
            '' AS linked_signal_timestamp_utc,
            '' AS linked_signal_entry,
            '' AS linked_signal_stop,
            '' AS linked_signal_target,
            '' AS linked_signal_confidence
        FROM trade_events
        ORDER BY event_time ASC, id ASC
        """
    )
    return [stringify_db_row(row) for row in rows]



def get_db_signal_rows() -> list[dict[str, str]]:
    rows = fetch_all(
        """
        SELECT
            timestamp_utc,
            scan_id,
            mode,
            scan_source,
            market_phase,
            confidence,
            top_symbol
        FROM signal_logs
        ORDER BY timestamp_utc ASC, id ASC
        """
    )
    return [stringify_db_row(row) for row in rows]


def get_trade_rows() -> list[dict[str, str]]:
    return get_db_trade_rows()


def get_signal_rows() -> list[dict[str, str]]:
    return get_db_signal_rows()


@dataclass
class PairedTrade:
    symbol: str
    name: str
    mode: str
    trade_source: str
    side: str
    shares: float
    entry_timestamp_utc: str
    exit_timestamp_utc: str
    entry_price: float
    exit_price: float
    exit_reason: str
    confidence: float | None
    broker_parent_order_id: str
    scan_source: str
    market_phase: str

    @property
    def pnl(self) -> float:
        if self.side == "SELL":
            return (self.entry_price - self.exit_price) * self.shares
        return (self.exit_price - self.entry_price) * self.shares

    @property
    def outcome(self) -> str:
        pnl_value = self.pnl
        if pnl_value > 0:
            return "WIN"
        if pnl_value < 0:
            return "LOSS"
        return "FLAT"


def to_float(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def infer_side(open_row: dict[str, str]) -> str:
    notes = str(open_row.get("notes", "")).upper()
    if "PAPER SELL" in notes:
        return "SELL"
    if "PAPER BUY" in notes:
        return "BUY"

    entry_price = to_float(open_row.get("entry_price"))
    stop_price = to_float(open_row.get("stop_price"))
    target_price = to_float(open_row.get("target_price"))
    if entry_price is None or stop_price is None or target_price is None:
        return "UNKNOWN"

    if stop_price > entry_price and target_price < entry_price:
        return "SELL"
    if stop_price < entry_price and target_price > entry_price:
        return "BUY"
    return "UNKNOWN"



def upload_file_to_gcs(local_path: Path, bucket_name: str, object_name: str) -> str:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(str(local_path))
    return f"gs://{bucket_name}/{object_name}"


def normalize_signal_timestamp(timestamp_utc: str) -> str:
    value = str(timestamp_utc or "").strip()
    return value[:-6] if value.endswith("+00:00") else value


def build_signal_index(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    index: dict[str, dict[str, str]] = {}
    for row in rows:
        timestamp_utc = normalize_signal_timestamp(row.get("timestamp_utc", ""))
        mode = str(row.get("mode", "")).strip().lower()
        key = f"{timestamp_utc}|{mode}"
        if timestamp_utc and mode:
            index[key] = row
    return index


def core_group_for_mode(mode: str) -> str:
    normalized_mode = str(mode).strip().lower()
    return "core" if normalized_mode in CORE_MODES else "non_core"


def pair_trades(rows: Iterable[dict[str, str]], signal_index: dict[str, dict[str, str]] | None = None) -> tuple[list[PairedTrade], list[dict[str, str]]]:
    open_by_parent: dict[str, dict[str, str]] = {}
    paired: list[PairedTrade] = []
    unmatched_closes: list[dict[str, str]] = []
    signal_index = signal_index or {}

    for row in rows:
        event_type = str(row.get("event_type", "")).strip().upper()
        trade_source = str(row.get("trade_source", "")).strip().upper()
        parent_id = str(row.get("broker_parent_order_id", "")).strip()

        if trade_source != "IBKR_PAPER" or not parent_id:
            continue

        if event_type == "OPEN":
            open_by_parent[parent_id] = row
            continue

        if event_type not in CLOSE_EVENT_TYPES:
            continue

        open_row = open_by_parent.get(parent_id)
        if not open_row:
            unmatched_closes.append(row)
            continue

        shares = to_float(row.get("shares"))
        if shares is None or shares <= 0:
            shares = to_float(open_row.get("shares")) or 0.0

        entry_price = to_float(open_row.get("entry_price"))
        if entry_price is None:
            entry_price = to_float(open_row.get("price"))

        exit_price = to_float(row.get("exit_price"))
        if exit_price is None:
            exit_price = to_float(row.get("price"))

        if entry_price is None or exit_price is None:
            unmatched_closes.append(row)
            continue

        mode = str(open_row.get("mode", "")).strip().lower()
        linked_signal_timestamp_utc = normalize_signal_timestamp(open_row.get("linked_signal_timestamp_utc", "") or open_row.get("timestamp_utc", ""))
        signal_row = signal_index.get(f"{linked_signal_timestamp_utc}|{mode}", {})

        paired.append(
            PairedTrade(
                symbol=str(open_row.get("symbol", "")).strip().upper(),
                name=str(open_row.get("name", "")).strip() or str(open_row.get("symbol", "")).strip().upper(),
                mode=mode,
                trade_source=str(open_row.get("trade_source", "")).strip().upper(),
                side=infer_side(open_row),
                shares=shares,
                entry_timestamp_utc=str(open_row.get("timestamp_utc", "")).strip(),
                exit_timestamp_utc=str(row.get("timestamp_utc", "")).strip(),
                entry_price=entry_price,
                exit_price=exit_price,
                exit_reason=str(row.get("exit_reason", "") or row.get("event_type", "")).strip().upper(),
                confidence=to_float(open_row.get("linked_signal_confidence")) or to_float(signal_row.get("confidence")),
                broker_parent_order_id=parent_id,
                scan_source=str(signal_row.get("scan_source", "UNKNOWN")).strip().upper() or "UNKNOWN",
                market_phase=str(signal_row.get("market_phase", "UNKNOWN")).strip().upper() or "UNKNOWN",
            )
        )
        del open_by_parent[parent_id]

    return paired, unmatched_closes


def summarize_group(trades: list[PairedTrade]) -> dict[str, float | int]:
    total = len(trades)
    wins = sum(1 for trade in trades if trade.outcome == "WIN")
    losses = sum(1 for trade in trades if trade.outcome == "LOSS")
    flats = sum(1 for trade in trades if trade.outcome == "FLAT")
    gross_pnl = round(sum(trade.pnl for trade in trades), 4)
    avg_pnl = round(gross_pnl / total, 4) if total else 0.0
    win_rate = round((wins / total) * 100, 2) if total else 0.0
    avg_win = round(sum(trade.pnl for trade in trades if trade.pnl > 0) / wins, 4) if wins else 0.0
    avg_loss = round(sum(trade.pnl for trade in trades if trade.pnl < 0) / losses, 4) if losses else 0.0

    return {
        "trades": total,
        "wins": wins,
        "losses": losses,
        "flats": flats,
        "win_rate_pct": win_rate,
        "gross_pnl": gross_pnl,
        "avg_pnl": avg_pnl,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
    }


def print_summary(title: str, summary: dict[str, float | int]) -> None:
    print(title)
    print(f"  trades: {summary['trades']}")
    print(f"  wins: {summary['wins']}")
    print(f"  losses: {summary['losses']}")
    print(f"  flats: {summary['flats']}")
    print(f"  win_rate_pct: {summary['win_rate_pct']}")
    print(f"  gross_pnl: {summary['gross_pnl']}")
    print(f"  avg_pnl: {summary['avg_pnl']}")
    print(f"  avg_win: {summary['avg_win']}")
    print(f"  avg_loss: {summary['avg_loss']}")


def print_trade_table(trades: list[PairedTrade]) -> None:
    print("\nPaired trades")
    print(
        "symbol,mode,core_group,scan_source,market_phase,side,shares,entry_timestamp_utc,exit_timestamp_utc,entry_price,exit_price,exit_reason,confidence,pnl,outcome"
    )
    for trade in trades:
        print(
            f"{trade.symbol},{trade.mode},{core_group_for_mode(trade.mode)},{trade.scan_source},{trade.market_phase},"
            f"{trade.side},{trade.shares},{trade.entry_timestamp_utc},{trade.exit_timestamp_utc},"
            f"{trade.entry_price},{trade.exit_price},{trade.exit_reason},"
            f"{trade.confidence if trade.confidence is not None else ''},{round(trade.pnl, 4)},{trade.outcome}"
        )
def build_summary_rows(group_name: str, grouped_trades: dict[str, list[PairedTrade]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in sorted(grouped_trades):
        summary = summarize_group(grouped_trades[key])
        row = {"group_name": group_name, "group_value": key}
        row.update(summary)
        rows.append(row)
    return rows


def build_paired_trade_rows(trades: list[PairedTrade]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for trade in trades:
        rows.append({
            "symbol": trade.symbol,
            "name": trade.name,
            "mode": trade.mode,
            "core_group": core_group_for_mode(trade.mode),
            "scan_source": trade.scan_source,
            "market_phase": trade.market_phase,
            "side": trade.side,
            "shares": trade.shares,
            "entry_timestamp_utc": trade.entry_timestamp_utc,
            "exit_timestamp_utc": trade.exit_timestamp_utc,
            "entry_price": trade.entry_price,
            "exit_price": trade.exit_price,
            "exit_reason": trade.exit_reason,
            "confidence": trade.confidence if trade.confidence is not None else "",
            "broker_parent_order_id": trade.broker_parent_order_id,
            "pnl": round(trade.pnl, 4),
            "outcome": trade.outcome,
        })
    return rows


def write_csv(rows: list[dict[str, Any]], path: Path, headers: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def run_trade_analysis() -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, str]]]:
    trade_rows = get_trade_rows()
    signal_rows = get_signal_rows()
    signal_index = build_signal_index(signal_rows)

    paired_trades, unmatched_closes = pair_trades(trade_rows, signal_index=signal_index)

    by_mode: dict[str, list[PairedTrade]] = defaultdict(list)
    by_symbol: dict[str, list[PairedTrade]] = defaultdict(list)
    by_side: dict[str, list[PairedTrade]] = defaultdict(list)
    by_scan_source: dict[str, list[PairedTrade]] = defaultdict(list)
    by_market_phase: dict[str, list[PairedTrade]] = defaultdict(list)
    by_core_group: dict[str, list[PairedTrade]] = defaultdict(list)

    for trade in paired_trades:
        by_mode[trade.mode].append(trade)
        by_symbol[trade.symbol].append(trade)
        by_side[trade.side].append(trade)
        by_scan_source[trade.scan_source].append(trade)
        by_market_phase[trade.market_phase].append(trade)
        by_core_group[core_group_for_mode(trade.mode)].append(trade)

    summary_rows: list[dict[str, Any]] = []
    overall_summary = {"group_name": "overall", "group_value": "all"}
    overall_summary.update(summarize_group(paired_trades))
    summary_rows.append(overall_summary)
    summary_rows.extend(build_summary_rows("mode", by_mode))
    summary_rows.extend(build_summary_rows("symbol", by_symbol))
    summary_rows.extend(build_summary_rows("side", by_side))
    summary_rows.extend(build_summary_rows("scan_source", by_scan_source))
    summary_rows.extend(build_summary_rows("market_phase", by_market_phase))
    summary_rows.extend(build_summary_rows("core_group", by_core_group))

    paired_rows = build_paired_trade_rows(paired_trades)

    write_csv(
        summary_rows,
        ANALYSIS_SUMMARY_OUTPUT,
        [
            "group_name",
            "group_value",
            "trades",
            "wins",
            "losses",
            "flats",
            "win_rate_pct",
            "gross_pnl",
            "avg_pnl",
            "avg_win",
            "avg_loss",
        ],
    )
    write_csv(
        paired_rows,
        ANALYSIS_PAIRED_OUTPUT,
        [
            "symbol",
            "name",
            "mode",
            "core_group",
            "scan_source",
            "market_phase",
            "side",
            "shares",
            "entry_timestamp_utc",
            "exit_timestamp_utc",
            "entry_price",
            "exit_price",
            "exit_reason",
            "confidence",
            "broker_parent_order_id",
            "pnl",
            "outcome",
        ],
    )

    return summary_rows, paired_rows, unmatched_closes


def main() -> None:
    active_summary_rows, active_paired_rows, active_unmatched_closes = run_trade_analysis()

    overall_rows = [row for row in active_summary_rows if row.get("group_name") == "overall"]
    if overall_rows:
        print_summary("Overall summary", {k: overall_rows[0][k] for k in ["trades", "wins", "losses", "flats", "win_rate_pct", "gross_pnl", "avg_pnl", "avg_win", "avg_loss"]})

    print("\nSummary by mode")
    for row in active_summary_rows:
        if row.get("group_name") == "mode":
            print_summary(f"- {row['group_value']}", {k: row[k] for k in ["trades", "wins", "losses", "flats", "win_rate_pct", "gross_pnl", "avg_pnl", "avg_win", "avg_loss"]})

    print("\nSummary by side")
    for row in active_summary_rows:
        if row.get("group_name") == "side":
            print_summary(f"- {row['group_value']}", {k: row[k] for k in ["trades", "wins", "losses", "flats", "win_rate_pct", "gross_pnl", "avg_pnl", "avg_win", "avg_loss"]})

    print("\nSummary by scan_source")
    for row in active_summary_rows:
        if row.get("group_name") == "scan_source":
            print_summary(f"- {row['group_value']}", {k: row[k] for k in ["trades", "wins", "losses", "flats", "win_rate_pct", "gross_pnl", "avg_pnl", "avg_win", "avg_loss"]})

    print("\nSummary by market_phase")
    for row in active_summary_rows:
        if row.get("group_name") == "market_phase":
            print_summary(f"- {row['group_value']}", {k: row[k] for k in ["trades", "wins", "losses", "flats", "win_rate_pct", "gross_pnl", "avg_pnl", "avg_win", "avg_loss"]})

    print("\nSummary by core_group")
    for row in active_summary_rows:
        if row.get("group_name") == "core_group":
            print_summary(f"- {row['group_value']}", {k: row[k] for k in ["trades", "wins", "losses", "flats", "win_rate_pct", "gross_pnl", "avg_pnl", "avg_win", "avg_loss"]})

    print_trade_table([
        PairedTrade(
            symbol=str(row["symbol"]),
            name=str(row["name"]),
            mode=str(row["mode"]),
            trade_source="IBKR_PAPER",
            side=str(row["side"]),
            shares=float(row["shares"]),
            entry_timestamp_utc=str(row["entry_timestamp_utc"]),
            exit_timestamp_utc=str(row["exit_timestamp_utc"]),
            entry_price=float(row["entry_price"]),
            exit_price=float(row["exit_price"]),
            exit_reason=str(row["exit_reason"]),
            confidence=float(row["confidence"]) if row["confidence"] not in (None, "") else None,
            broker_parent_order_id=str(row["broker_parent_order_id"]),
            scan_source=str(row["scan_source"]),
            market_phase=str(row["market_phase"]),
        )
        for row in active_paired_rows
    ])

    if active_unmatched_closes:
        print("\nUnmatched closes")
        for row in active_unmatched_closes:
            print(
                f"{row.get('timestamp_utc', '')},{row.get('symbol', '')},{row.get('event_type', '')},"
                f"{row.get('broker_parent_order_id', '')}"
            )

    print("\nTrade analysis active source: DB")
    print(f"Wrote summary CSV to {ANALYSIS_SUMMARY_OUTPUT}")
    print(f"Wrote paired trades CSV to {ANALYSIS_PAIRED_OUTPUT}")


if __name__ == "__main__":
    main()
