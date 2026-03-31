from __future__ import annotations

import csv
import os
from collections import defaultdict
from pathlib import Path
from typing import Any

from google.cloud import storage
from db import fetch_all


ANALYSIS_SUMMARY_OUTPUT = Path("signal_analysis_summary.csv")
ANALYSIS_SIGNAL_ROWS_OUTPUT = Path("signal_analysis_rows.csv")
ANALYSIS_GCS_BUCKET = os.getenv("TRADE_ANALYSIS_BUCKET", "stock-scanner-490821-logs")
ANALYSIS_SUMMARY_OBJECT = os.getenv("SIGNAL_ANALYSIS_SUMMARY_OBJECT", "reports/signal_analysis_summary.csv")
ANALYSIS_ROWS_OBJECT = os.getenv("SIGNAL_ANALYSIS_ROWS_OBJECT", "reports/signal_analysis_rows.csv")
CORE_MODES = {"core_one", "core_two"}


def stringify_db_row(row: dict[str, Any]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in row.items():
        if value is None:
            normalized[key] = ""
        else:
            normalized[key] = str(value)
    return normalized



def get_db_signal_rows() -> list[dict[str, str]]:
    rows = fetch_all(
        """
        SELECT
            timestamp_utc,
            scan_id,
            scan_source,
            market_phase,
            mode,
            paper_trade_enabled,
            paper_trade_candidate_count,
            paper_trade_placed_count,
            paper_trade_long_candidate_count,
            paper_trade_short_candidate_count,
            paper_trade_placed_long_count,
            paper_trade_placed_short_count,
            paper_candidate_symbols,
            paper_placed_symbols,
            paper_skipped_symbols,
            paper_skip_reasons,
            paper_candidate_confidences,
            benchmark_sp500,
            benchmark_nasdaq,
            reason
        FROM signal_logs
        ORDER BY timestamp_utc ASC, id ASC
        """
    )
    return [stringify_db_row(row) for row in rows]


def get_signal_rows() -> list[dict[str, str]]:
    return get_db_signal_rows()


def upload_file_to_gcs(local_path: Path, bucket_name: str, object_name: str) -> str:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(str(local_path))
    return f"gs://{bucket_name}/{object_name}"


def to_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def to_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def core_group_for_mode(mode: str) -> str:
    normalized_mode = str(mode).strip().lower()
    return "core" if normalized_mode in CORE_MODES else "non_core"


def split_symbols(value: str) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    return [item.strip().upper() for item in text.split(",") if item.strip()]


def split_skip_reasons(value: str) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    return [item.strip() for item in text.split("|") if item.strip()]


def build_signal_rows(signal_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for row in signal_rows:
        mode = str(row.get("mode", "")).strip().lower()
        candidate_count = to_int(row.get("paper_trade_candidate_count"))
        placed_count = to_int(row.get("paper_trade_placed_count"))
        long_candidate_count = to_int(row.get("paper_trade_long_candidate_count"))
        short_candidate_count = to_int(row.get("paper_trade_short_candidate_count"))
        placed_long_count = to_int(row.get("paper_trade_placed_long_count"))
        placed_short_count = to_int(row.get("paper_trade_placed_short_count"))
        candidate_symbols = split_symbols(row.get("paper_candidate_symbols", ""))
        placed_symbols = split_symbols(row.get("paper_placed_symbols", ""))
        skipped_symbols = split_symbols(row.get("paper_skipped_symbols", ""))
        skip_reasons = split_skip_reasons(row.get("paper_skip_reasons", ""))

        explicit_skipped_count = to_int(row.get("paper_trade_skipped_count"))
        skipped_count = explicit_skipped_count if explicit_skipped_count > 0 else max(candidate_count - placed_count, 0)

        rows.append({
            "timestamp_utc": str(row.get("timestamp_utc", "")).strip(),
            "scan_id": str(row.get("scan_id", "")).strip(),
            "scan_source": str(row.get("scan_source", "UNKNOWN")).strip().upper() or "UNKNOWN",
            "market_phase": str(row.get("market_phase", "UNKNOWN")).strip().upper() or "UNKNOWN",
            "mode": mode,
            "core_group": core_group_for_mode(mode),
            "paper_trade_enabled": str(row.get("paper_trade_enabled", "")).strip(),
            "candidate_count": candidate_count,
            "placed_count": placed_count,
            "skipped_count": skipped_count,
            "long_candidate_count": long_candidate_count,
            "short_candidate_count": short_candidate_count,
            "placed_long_count": placed_long_count,
            "placed_short_count": placed_short_count,
            "placement_rate_pct": round((placed_count / candidate_count) * 100, 2) if candidate_count else 0.0,
            "candidate_symbols": ",".join(candidate_symbols),
            "placed_symbols": ",".join(placed_symbols),
            "skipped_symbols": ",".join(skipped_symbols),
            "skip_reasons": "|".join(skip_reasons),
            "confidence_values": str(row.get("paper_candidate_confidences", "")).strip(),
            "benchmark_sp500": str(row.get("benchmark_sp500", "")).strip().upper(),
            "benchmark_nasdaq": str(row.get("benchmark_nasdaq", "")).strip().upper(),
            "reason": str(row.get("reason", "")).strip(),
        })

    return rows


def summarize_signal_group(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total_scans = len(rows)
    scans_with_candidates = sum(1 for row in rows if to_int(row.get("candidate_count")) > 0)
    scans_with_placements = sum(1 for row in rows if to_int(row.get("placed_count")) > 0)
    total_candidates = sum(to_int(row.get("candidate_count")) for row in rows)
    total_placed = sum(to_int(row.get("placed_count")) for row in rows)
    total_skipped = sum(to_int(row.get("skipped_count")) for row in rows)
    placement_rate_pct = round((total_placed / total_candidates) * 100, 2) if total_candidates else 0.0
    candidate_scan_rate_pct = round((scans_with_candidates / total_scans) * 100, 2) if total_scans else 0.0
    placement_scan_rate_pct = round((scans_with_placements / total_scans) * 100, 2) if total_scans else 0.0

    return {
        "scans": total_scans,
        "scans_with_candidates": scans_with_candidates,
        "scans_with_placements": scans_with_placements,
        "candidate_scan_rate_pct": candidate_scan_rate_pct,
        "placement_scan_rate_pct": placement_scan_rate_pct,
        "total_candidates": total_candidates,
        "total_placed": total_placed,
        "total_skipped": total_skipped,
        "placement_rate_pct": placement_rate_pct,
    }


def build_summary_rows(group_name: str, grouped_rows: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for key in sorted(grouped_rows):
        summary = summarize_signal_group(grouped_rows[key])
        row = {"group_name": group_name, "group_value": key}
        row.update(summary)
        results.append(row)
    return results


def build_skip_reason_rows(signal_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, int] = defaultdict(int)

    for row in signal_rows:
        for reason in split_skip_reasons(str(row.get("skip_reasons", ""))):
            counts[reason] += 1

    return [
        {"group_name": "skip_reason", "group_value": reason, "count": counts[reason]}
        for reason in sorted(counts)
    ]


def write_csv(rows: list[dict[str, Any]], path: Path, headers: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def run_signal_analysis() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    raw_signal_rows = get_signal_rows()
    signal_rows = build_signal_rows(raw_signal_rows)

    by_mode: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_scan_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_market_phase: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_core_group: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for row in signal_rows:
        by_mode[str(row["mode"])].append(row)
        by_scan_source[str(row["scan_source"])].append(row)
        by_market_phase[str(row["market_phase"])].append(row)
        by_core_group[str(row["core_group"])].append(row)

    summary_rows: list[dict[str, Any]] = []
    overall_summary = {"group_name": "overall", "group_value": "all"}
    overall_summary.update(summarize_signal_group(signal_rows))
    summary_rows.append(overall_summary)
    summary_rows.extend(build_summary_rows("mode", by_mode))
    summary_rows.extend(build_summary_rows("scan_source", by_scan_source))
    summary_rows.extend(build_summary_rows("market_phase", by_market_phase))
    summary_rows.extend(build_summary_rows("core_group", by_core_group))
    summary_rows.extend(build_skip_reason_rows(signal_rows))

    write_csv(
        summary_rows,
        ANALYSIS_SUMMARY_OUTPUT,
        [
            "group_name",
            "group_value",
            "scans",
            "scans_with_candidates",
            "scans_with_placements",
            "candidate_scan_rate_pct",
            "placement_scan_rate_pct",
            "total_candidates",
            "total_placed",
            "total_skipped",
            "placement_rate_pct",
            "count",
        ],
    )
    write_csv(
        signal_rows,
        ANALYSIS_SIGNAL_ROWS_OUTPUT,
        [
            "timestamp_utc",
            "scan_id",
            "scan_source",
            "market_phase",
            "mode",
            "core_group",
            "paper_trade_enabled",
            "candidate_count",
            "placed_count",
            "skipped_count",
            "long_candidate_count",
            "short_candidate_count",
            "placed_long_count",
            "placed_short_count",
            "placement_rate_pct",
            "candidate_symbols",
            "placed_symbols",
            "skipped_symbols",
            "skip_reasons",
            "confidence_values",
            "benchmark_sp500",
            "benchmark_nasdaq",
            "reason",
        ],
    )
    return summary_rows, signal_rows


def print_summary_row(row: dict[str, Any]) -> None:
    print(f"- {row['group_name']}={row['group_value']}")
    if row.get("group_name") == "skip_reason":
        print(f"  count: {row.get('count', 0)}")
        return
    print(f"  scans: {row.get('scans', 0)}")
    print(f"  scans_with_candidates: {row.get('scans_with_candidates', 0)}")
    print(f"  scans_with_placements: {row.get('scans_with_placements', 0)}")
    print(f"  candidate_scan_rate_pct: {row.get('candidate_scan_rate_pct', 0)}")
    print(f"  placement_scan_rate_pct: {row.get('placement_scan_rate_pct', 0)}")
    print(f"  total_candidates: {row.get('total_candidates', 0)}")
    print(f"  total_placed: {row.get('total_placed', 0)}")
    print(f"  total_skipped: {row.get('total_skipped', 0)}")
    print(f"  placement_rate_pct: {row.get('placement_rate_pct', 0)}")


def main() -> None:
    active_summary_rows, active_signal_rows = run_signal_analysis()

    print("Overall signal summary")
    for row in active_summary_rows:
        if row.get("group_name") == "overall":
            print_summary_row(row)

    print("\nSignal summary by mode")
    for row in active_summary_rows:
        if row.get("group_name") == "mode":
            print_summary_row(row)

    print("\nSignal summary by scan_source")
    for row in active_summary_rows:
        if row.get("group_name") == "scan_source":
            print_summary_row(row)

    print("\nSignal summary by market_phase")
    for row in active_summary_rows:
        if row.get("group_name") == "market_phase":
            print_summary_row(row)

    print("\nSignal summary by core_group")
    for row in active_summary_rows:
        if row.get("group_name") == "core_group":
            print_summary_row(row)

    print("\nSkip reason counts")
    for row in active_summary_rows:
        if row.get("group_name") == "skip_reason":
            print_summary_row(row)

    print("\nSignal analysis active source: DB")
    print(f"Wrote summary CSV to {ANALYSIS_SUMMARY_OUTPUT}")
    print(f"Wrote signal rows CSV to {ANALYSIS_SIGNAL_ROWS_OUTPUT}")
    print(f"Signal rows analyzed (active): {len(active_signal_rows)}")


if __name__ == "__main__":
    main()
