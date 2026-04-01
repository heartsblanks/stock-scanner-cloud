import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from scheduler_ops import (
    build_market_ops_plan,
    execute_post_close_ops,
    should_run_eod_close,
    should_run_market_scan,
    should_run_market_sync,
)


NY_TZ = ZoneInfo("America/New_York")


class SchedulerOpsTests(unittest.TestCase):
    def test_market_ops_plan_at_935_runs_sync_and_scan(self):
        now_ny = datetime(2026, 4, 1, 9, 35, tzinfo=NY_TZ)
        self.assertTrue(should_run_market_sync(now_ny))
        self.assertTrue(should_run_market_scan(now_ny))
        self.assertFalse(should_run_eod_close(now_ny))
        self.assertEqual(build_market_ops_plan(now_ny), ["sync", "scan"])

    def test_market_ops_plan_at_1555_runs_close_only(self):
        now_ny = datetime(2026, 4, 1, 15, 55, tzinfo=NY_TZ)
        self.assertFalse(should_run_market_sync(now_ny))
        self.assertFalse(should_run_market_scan(now_ny))
        self.assertTrue(should_run_eod_close(now_ny))
        self.assertEqual(build_market_ops_plan(now_ny), ["close"])

    def test_market_ops_plan_at_1005_runs_sync_and_scan(self):
        now_ny = datetime(2026, 4, 1, 10, 5, tzinfo=NY_TZ)
        self.assertEqual(build_market_ops_plan(now_ny), ["sync", "scan"])

    def test_post_close_ops_runs_all_tasks(self):
        now_ny = datetime(2026, 4, 1, 16, 30, tzinfo=NY_TZ)
        result = execute_post_close_ops(
            now_ny=now_ny,
            run_sync=lambda: {"ok": True, "task": "sync"},
            run_reconcile=lambda: {"ok": True, "task": "reconcile"},
            run_trade_analysis=lambda: {"ok": True, "task": "trade_analysis"},
            run_signal_analysis=lambda: {"ok": True, "task": "signal_analysis"},
            run_snapshot_export=lambda: {"ok": True, "task": "snapshot"},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["action_count"], 5)
        self.assertIn("sync", result["results"])
        self.assertIn("reconcile", result["results"])
        self.assertIn("analyze_paper_trades", result["results"])
        self.assertIn("analyze_signals", result["results"])
        self.assertIn("export_daily_snapshot", result["results"])


if __name__ == "__main__":
    unittest.main()
