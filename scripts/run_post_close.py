"""
Daily post-close runner for GitHub Actions.
Runs the DB-only parts of the post-close flow directly (no Flask server needed).
IBKR-dependent steps (sync, stale-close repair, symbol eligibility) are skipped
until the IBKR MCP connector integration is complete.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

NY_TZ = ZoneInfo("America/New_York")


def run() -> None:
    now_ny = datetime.now(NY_TZ)
    print(f"[post-close] Starting at {now_ny.strftime('%Y-%m-%d %H:%M ET')}")

    results: dict[str, dict] = {}

    # Trade analysis
    try:
        from analytics.trade_analysis import run_trade_analysis
        summary_rows, paired_rows, _ = run_trade_analysis()
        results["analyze_paper_trades"] = {
            "ok": True,
            "summary_row_count": len(summary_rows),
            "paired_row_count": len(paired_rows),
        }
        print(f"[post-close] trade_analysis: {len(summary_rows)} summary rows, {len(paired_rows)} paired rows")
    except Exception as e:
        results["analyze_paper_trades"] = {"ok": False, "error": str(e)}
        print(f"[post-close] trade_analysis FAILED: {e}", file=sys.stderr)

    # Signal analysis
    try:
        from analytics.signal_analysis import run_signal_analysis
        summary_rows, signal_rows = run_signal_analysis()
        results["analyze_signals"] = {
            "ok": True,
            "summary_row_count": len(summary_rows),
            "signal_row_count": len(signal_rows),
        }
        print(f"[post-close] signal_analysis: {len(summary_rows)} summary rows, {len(signal_rows)} signal rows")
    except Exception as e:
        results["analyze_signals"] = {"ok": False, "error": str(e)}
        print(f"[post-close] signal_analysis FAILED: {e}", file=sys.stderr)

    # Symbol rankings
    try:
        from orchestration.runtime_context import refresh_ibkr_symbol_rankings
        result = refresh_ibkr_symbol_rankings(ranking_date=now_ny.date().isoformat())
        results["refresh_symbol_rankings"] = {"ok": True, **result}
        print(f"[post-close] symbol_rankings: ok")
    except Exception as e:
        results["refresh_symbol_rankings"] = {"ok": False, "error": str(e)}
        print(f"[post-close] symbol_rankings FAILED: {e}", file=sys.stderr)

    # Mode rankings
    try:
        from orchestration.runtime_context import refresh_ibkr_mode_rankings
        result = refresh_ibkr_mode_rankings(ranking_date=now_ny.date().isoformat())
        results["refresh_mode_rankings"] = {"ok": True, **result}
        print(f"[post-close] mode_rankings: ok")
    except Exception as e:
        results["refresh_mode_rankings"] = {"ok": False, "error": str(e)}
        print(f"[post-close] mode_rankings FAILED: {e}", file=sys.stderr)

    # Summary
    all_ok = all(r.get("ok", False) for r in results.values())
    print(f"\n[post-close] Completed. All ok: {all_ok}")
    print(json.dumps(results, indent=2, default=str))

    if not all_ok:
        sys.exit(1)


if __name__ == "__main__":
    run()
