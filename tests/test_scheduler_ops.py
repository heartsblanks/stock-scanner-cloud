import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from orchestration.scheduler_ops import (
    build_market_ops_plan,
    execute_ibkr_vm_control,
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

    def test_market_ops_plan_at_955_runs_sync_and_scan(self):
        now_ny = datetime(2026, 4, 1, 9, 55, tzinfo=NY_TZ)
        self.assertTrue(should_run_market_sync(now_ny))
        self.assertTrue(should_run_market_scan(now_ny))
        self.assertFalse(should_run_eod_close(now_ny))
        self.assertEqual(build_market_ops_plan(now_ny), ["sync", "scan"])

    def test_market_ops_plan_at_1055_runs_sync_and_scan(self):
        now_ny = datetime(2026, 4, 1, 10, 55, tzinfo=NY_TZ)
        self.assertTrue(should_run_market_sync(now_ny))
        self.assertTrue(should_run_market_scan(now_ny))
        self.assertFalse(should_run_eod_close(now_ny))
        self.assertEqual(build_market_ops_plan(now_ny), ["sync", "scan"])

    def test_post_close_ops_runs_all_tasks(self):
        now_ny = datetime(2026, 4, 1, 16, 30, tzinfo=NY_TZ)
        result = execute_post_close_ops(
            now_ny=now_ny,
            run_sync=lambda: {"ok": True, "task": "sync"},
            run_ibkr_stale_close_repair=lambda: {"ok": True, "task": "repair"},
            run_reconcile=lambda: {"ok": True, "task": "reconcile"},
            run_trade_analysis=lambda: {"ok": True, "task": "trade_analysis"},
            run_signal_analysis=lambda: {"ok": True, "task": "signal_analysis"},
            run_snapshot_export=lambda: {"ok": True, "task": "snapshot"},
            run_mode_ranking_refresh=lambda: {"ok": True, "task": "mode_ranking"},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["action_count"], 7)
        self.assertIn("sync", result["results"])
        self.assertIn("repair_ibkr_stale_closes", result["results"])
        self.assertIn("reconcile", result["results"])
        self.assertIn("analyze_paper_trades", result["results"])
        self.assertIn("analyze_signals", result["results"])
        self.assertIn("export_daily_snapshot", result["results"])
        self.assertIn("refresh_mode_rankings", result["results"])

    def test_post_close_ops_accepts_non_http_tuple_results(self):
        now_ny = datetime(2026, 4, 2, 16, 30, tzinfo=NY_TZ)
        result = execute_post_close_ops(
            now_ny=now_ny,
            run_sync=lambda: {"ok": True, "task": "sync"},
            run_ibkr_stale_close_repair=lambda: {"ok": True, "task": "repair"},
            run_reconcile=lambda: {"ok": True, "task": "reconcile"},
            run_trade_analysis=lambda: ([{"group_name": "mode"}], [{"symbol": "SNAP"}], []),
            run_signal_analysis=lambda: ([{"group_name": "skip_reason"}], [{"timestamp_utc": "2026-04-02T20:30:00Z"}]),
            run_snapshot_export=lambda: {"ok": True, "task": "snapshot"},
            run_mode_ranking_refresh=lambda: {"ok": True, "task": "mode_ranking"},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["results"]["analyze_paper_trades"]["status_code"], 200)
        self.assertEqual(result["results"]["analyze_signals"]["status_code"], 200)
        self.assertEqual(result["results"]["refresh_mode_rankings"]["status_code"], 200)

    def test_post_close_ops_can_skip_ibkr_repair_hook(self):
        now_ny = datetime(2026, 4, 2, 16, 30, tzinfo=NY_TZ)
        result = execute_post_close_ops(
            now_ny=now_ny,
            run_sync=lambda: {"ok": True, "task": "sync"},
            run_ibkr_stale_close_repair=None,
            run_reconcile=lambda: {"ok": True, "task": "reconcile"},
            run_trade_analysis=lambda: {"ok": True, "task": "trade_analysis"},
            run_signal_analysis=lambda: {"ok": True, "task": "signal_analysis"},
            run_snapshot_export=lambda: {"ok": True, "task": "snapshot"},
            run_mode_ranking_refresh=None,
        )

        self.assertTrue(result["ok"])
        self.assertNotIn("repair_ibkr_stale_closes", result["results"])

    def test_ibkr_vm_start_skips_on_holiday(self):
        now_ny = datetime(2026, 7, 4, 9, 15, tzinfo=NY_TZ)
        result = execute_ibkr_vm_control(
            now_ny=now_ny,
            action="start",
            is_trading_day=False,
            holiday_message="US market closed today (NYSE holiday or weekend).",
            get_instance_status=lambda: "TERMINATED",
            start_instance=lambda: {"unexpected": True},
            stop_instance=lambda: {"unexpected": True},
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["noop"])
        self.assertTrue(result["skipped"])
        self.assertEqual(result["instance_status_before"], "TERMINATED")
        self.assertIn("market closed", result["reason"].lower())

    def test_ibkr_vm_start_runs_on_trading_day(self):
        now_ny = datetime(2026, 4, 8, 9, 15, tzinfo=NY_TZ)
        result = execute_ibkr_vm_control(
            now_ny=now_ny,
            action="start",
            is_trading_day=True,
            holiday_message="US market is scheduled to trade today.",
            get_instance_status=lambda: "TERMINATED",
            start_instance=lambda: {"name": "operation-123", "status": "PENDING"},
            stop_instance=lambda: {"unexpected": True},
        )

        self.assertTrue(result["ok"])
        self.assertFalse(result["noop"])
        self.assertFalse(result["skipped"])
        self.assertEqual(result["results"]["start"]["status_code"], 200)
        self.assertEqual(result["instance_status_before"], "TERMINATED")
        self.assertEqual(result["instance_status_after"], "STARTING")

    def test_ibkr_vm_start_noops_when_already_running(self):
        now_ny = datetime(2026, 4, 8, 9, 15, tzinfo=NY_TZ)
        result = execute_ibkr_vm_control(
            now_ny=now_ny,
            action="start",
            is_trading_day=True,
            holiday_message=None,
            get_instance_status=lambda: "RUNNING",
            start_instance=lambda: {"unexpected": True},
            stop_instance=lambda: {"unexpected": True},
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["noop"])
        self.assertEqual(result["reason"], "IBKR VM is already running.")

    def test_ibkr_vm_stop_noops_when_already_stopped(self):
        now_ny = datetime(2026, 4, 8, 17, 0, tzinfo=NY_TZ)
        result = execute_ibkr_vm_control(
            now_ny=now_ny,
            action="stop",
            is_trading_day=True,
            holiday_message=None,
            get_instance_status=lambda: "TERMINATED",
            start_instance=lambda: {"unexpected": True},
            stop_instance=lambda: {"unexpected": True},
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["noop"])
        self.assertEqual(result["reason"], "IBKR VM is already stopped.")


if __name__ == "__main__":
    unittest.main()
