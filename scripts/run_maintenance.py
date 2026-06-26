"""
Maintenance runner for GitHub Actions.
Prunes old DB rows directly (no Flask server needed).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def run() -> None:
    print("[maintenance] Starting")

    try:
        from repositories.maintenance_repo import prune_operational_data
        results = prune_operational_data({
            "signal_logs": 45,
            "scan_runs": 45,
            "paper_trade_attempts": 120,
            "broker_orders": 120,
            "reconciliation_details": 120,
            "reconciliation_runs": 120,
            "symbol_rankings": 120,
        })
        for table, result in results.items():
            print(f"[maintenance] {table}: deleted {result.get('deleted_count', 0)} rows")
        print("[maintenance] Completed ok")
        print(json.dumps(results, indent=2, default=str))
    except Exception as e:
        print(f"[maintenance] FAILED: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    run()
