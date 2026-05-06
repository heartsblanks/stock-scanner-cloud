import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from orchestration.scheduler_ops import (
    build_market_ops_plan,
    execute_maintenance_ops,
    execute_ibkr_login_alert,
    execute_market_ops,
    execute_pre_close_prep,
    execute_ibkr_vm_control,
    execute_post_close_ops,
    should_run_eod_close,
    should_run_market_scan,
    should_run_pre_close_prep,
    should_run_market_sync,
)


NY_TZ = ZoneInfo("America/New_York")


class SchedulerOpsTests(unittest.TestCase):
    def test_market_ops_plan_at_935_is_idle(self):
        now_ny = datetime(2026, 4, 1, 9, 35, tzinfo=NY_TZ)
        self.assertFalse(should_run_market_sync(now_ny))
        self.assertFalse(should_run_market_scan(now_ny))
        self.assertFalse(should_run_eod_close(now_ny))
        self.assertEqual(build_market_ops_plan(now_ny), [])

    def test_market_ops_plan_at_1555_runs_close_only(self):
        now_ny = datetime(2026, 4, 1, 15, 55, tzinfo=NY_TZ)
        self.assertFalse(should_run_market_sync(now_ny))
        self.assertFalse(should_run_market_scan(now_ny))
        self.assertFalse(should_run_pre_close_prep(now_ny))
        self.assertTrue(should_run_eod_close(now_ny))
        self.assertEqual(build_market_ops_plan(now_ny), ["close"])

    def test_market_ops_plan_at_1550_runs_pre_close_prep(self):
        now_ny = datetime(2026, 4, 1, 15, 50, tzinfo=NY_TZ)
        self.assertTrue(should_run_market_sync(now_ny))
        self.assertFalse(should_run_market_scan(now_ny))
        self.assertTrue(should_run_pre_close_prep(now_ny))
        self.assertFalse(should_run_eod_close(now_ny))
        self.assertEqual(build_market_ops_plan(now_ny), ["sync", "health", "pre_close_prep"])

    def test_market_ops_plan_at_1005_runs_sync_then_scan(self):
        now_ny = datetime(2026, 4, 1, 10, 5, tzinfo=NY_TZ)
        self.assertEqual(build_market_ops_plan(now_ny), ["sync", "scan"])

    def test_market_ops_plan_at_955_runs_sync_then_scan(self):
        now_ny = datetime(2026, 4, 1, 9, 55, tzinfo=NY_TZ)
        self.assertFalse(should_run_market_sync(now_ny))
        self.assertTrue(should_run_market_scan(now_ny))
        self.assertFalse(should_run_eod_close(now_ny))
        self.assertEqual(build_market_ops_plan(now_ny), ["sync", "scan"])

    def test_market_ops_plan_at_1055_runs_sync_then_scan(self):
        now_ny = datetime(2026, 4, 1, 10, 55, tzinfo=NY_TZ)
        self.assertFalse(should_run_market_sync(now_ny))
        self.assertTrue(should_run_market_scan(now_ny))
        self.assertFalse(should_run_eod_close(now_ny))
        self.assertEqual(build_market_ops_plan(now_ny), ["sync", "scan"])

    def test_market_ops_plan_at_1000_runs_sync_and_periodic_health(self):
        now_ny = datetime(2026, 4, 1, 10, 0, tzinfo=NY_TZ)
        self.assertTrue(should_run_market_sync(now_ny))
        self.assertFalse(should_run_market_scan(now_ny))
        self.assertEqual(build_market_ops_plan(now_ny), ["sync", "health"])

    def test_execute_market_ops_runs_sync_before_scan(self):
        now_ny = datetime(2026, 4, 1, 10, 5, tzinfo=NY_TZ)
        execution_order = []
        result = execute_market_ops(
            now_ny=now_ny,
            run_sync=lambda: execution_order.append("sync") or {"ok": True},
            run_scan=lambda payload: execution_order.append("scan") or {"ok": True, "payload": payload},
            run_close=lambda: {"ok": True},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(execution_order, ["sync", "scan"])

    def test_post_close_ops_runs_all_tasks(self):
        now_ny = datetime(2026, 4, 1, 16, 30, tzinfo=NY_TZ)
        result = execute_post_close_ops(
            now_ny=now_ny,
            run_sync=lambda: {"ok": True, "task": "sync"},
            run_ibkr_stale_close_repair=lambda: {"ok": True, "task": "repair"},
            run_reconcile=lambda: {"ok": True, "task": "reconcile"},
            run_trade_analysis=lambda: {"ok": True, "task": "trade_analysis"},
            run_signal_analysis=lambda: {"ok": True, "task": "signal_analysis"},
            run_mode_ranking_refresh=lambda: {"ok": True, "task": "mode_ranking"},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["action_count"], 6)
        self.assertIn("sync", result["results"])
        self.assertIn("repair_ibkr_stale_closes", result["results"])
        self.assertIn("reconcile", result["results"])
        self.assertIn("analyze_paper_trades", result["results"])
        self.assertIn("analyze_signals", result["results"])
        self.assertIn("refresh_mode_rankings", result["results"])

    def test_pre_close_prep_reports_ibkr_readiness(self):
        now_ny = datetime(2026, 4, 1, 15, 50, tzinfo=NY_TZ)
        result = execute_pre_close_prep(
            now_ny=now_ny,
            get_ibkr_operational_status=lambda: {
                "ok": True,
                "enabled": True,
                "login_required": False,
                "state": "READY",
                "position_count": 3,
            },
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["ready_for_close"])
        self.assertEqual(result["ibkr_status"]["state"], "READY")

    def test_post_close_ops_accepts_non_http_tuple_results(self):
        now_ny = datetime(2026, 4, 2, 16, 30, tzinfo=NY_TZ)
        result = execute_post_close_ops(
            now_ny=now_ny,
            run_sync=lambda: {"ok": True, "task": "sync"},
            run_ibkr_stale_close_repair=lambda: {"ok": True, "task": "repair"},
            run_reconcile=lambda: {"ok": True, "task": "reconcile"},
            run_trade_analysis=lambda: ([{"group_name": "mode"}], [{"symbol": "SNAP"}], []),
            run_signal_analysis=lambda: ([{"group_name": "skip_reason"}], [{"timestamp_utc": "2026-04-02T20:30:00Z"}]),
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
            run_mode_ranking_refresh=None,
        )

        self.assertTrue(result["ok"])
        self.assertNotIn("repair_ibkr_stale_closes", result["results"])

    def test_post_close_ops_runs_symbol_eligibility_refresh_when_hook_is_provided(self):
        now_ny = datetime(2026, 4, 2, 16, 30, tzinfo=NY_TZ)
        result = execute_post_close_ops(
            now_ny=now_ny,
            run_sync=lambda: {"ok": True, "task": "sync"},
            run_symbol_eligibility_refresh=lambda: {"ok": True, "task": "refresh_symbol_eligibility"},
            run_ibkr_stale_close_repair=None,
            run_reconcile=lambda: {"ok": True, "task": "reconcile"},
            run_trade_analysis=lambda: {"ok": True, "task": "trade_analysis"},
            run_signal_analysis=lambda: {"ok": True, "task": "signal_analysis"},
            run_mode_ranking_refresh=None,
        )

        self.assertTrue(result["ok"])
        self.assertIn("refresh_symbol_eligibility", result["results"])

    def test_post_close_ops_runs_symbol_rankings_before_symbol_eligibility(self):
        now_ny = datetime(2026, 4, 2, 16, 30, tzinfo=NY_TZ)
        execution_order = []

        result = execute_post_close_ops(
            now_ny=now_ny,
            run_sync=lambda: execution_order.append("sync") or {"ok": True, "task": "sync"},
            run_symbol_ranking_refresh=lambda: execution_order.append("rankings") or {"ok": True, "task": "rankings"},
            run_symbol_eligibility_refresh=lambda: execution_order.append("eligibility") or {"ok": True, "task": "eligibility"},
            run_ibkr_stale_close_repair=lambda: execution_order.append("repair") or {"ok": True, "task": "repair"},
            run_reconcile=lambda: execution_order.append("reconcile") or {"ok": True, "task": "reconcile"},
            run_trade_analysis=lambda: {"ok": True, "task": "trade_analysis"},
            run_signal_analysis=lambda: {"ok": True, "task": "signal_analysis"},
            run_mode_ranking_refresh=None,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(execution_order, ["sync", "repair", "rankings", "eligibility", "reconcile"])
        self.assertIn("refresh_symbol_rankings", result["results"])

    def test_post_close_ops_captures_action_exception_without_crashing_whole_flow(self):
        now_ny = datetime(2026, 4, 2, 16, 30, tzinfo=NY_TZ)
        result = execute_post_close_ops(
            now_ny=now_ny,
            run_sync=lambda: {"ok": True, "task": "sync"},
            run_ibkr_stale_close_repair=None,
            run_reconcile=lambda: {"ok": True, "task": "reconcile"},
            run_trade_analysis=lambda: {"ok": True, "task": "trade_analysis"},
            run_signal_analysis=lambda: (_ for _ in ()).throw(RuntimeError("analysis failed")),
            run_mode_ranking_refresh=None,
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["results"]["analyze_signals"]["status_code"], 500)
        self.assertEqual(result["results"]["analyze_signals"]["body"]["action"], "analyze_signals")
        self.assertIn("analysis failed", result["results"]["analyze_signals"]["body"]["error"])

    def test_maintenance_ops_prunes_operational_tables(self):
        now_ny = datetime(2026, 4, 2, 18, 0, tzinfo=NY_TZ)
        result = execute_maintenance_ops(
            now_ny=now_ny,
            prune_logs=lambda retention_days: 12,
            prune_operational_data=lambda retention_days_by_table: {
                "signal_logs": {"retention_days": retention_days_by_table["signal_logs"], "deleted_count": 3},
                "scan_runs": {"retention_days": retention_days_by_table["scan_runs"], "deleted_count": 1},
            },
            retention_days=30,
        )

        self.assertTrue(result["ok"])
        self.assertIn("prune_operational_logs", result["results"])
        self.assertIn("prune_signal_logs", result["results"])
        self.assertIn("prune_scan_runs", result["results"])
        self.assertEqual(result["results"]["prune_signal_logs"]["body"]["retention_days"], 45)
        self.assertEqual(result["results"]["prune_scan_runs"]["body"]["deleted_count"], 1)

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

    def test_ibkr_login_alert_noops_when_telegram_alerts_disabled(self):
        now_ny = datetime(2026, 4, 8, 10, 0, tzinfo=NY_TZ)
        result = execute_ibkr_login_alert(
            now_ny=now_ny,
            get_ibkr_operational_status=lambda: {"enabled": True, "login_required": True},
            telegram_alerts_enabled=False,
            send_telegram_alert=lambda **kwargs: {"unexpected": True},
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["noop"])
        self.assertEqual(result["reason"], "telegram_alerts_disabled")

    def test_ibkr_login_alert_sends_when_login_is_required(self):
        now_ny = datetime(2026, 4, 8, 10, 0, tzinfo=NY_TZ)
        captured = {}

        result = execute_ibkr_login_alert(
            now_ny=now_ny,
            get_ibkr_operational_status=lambda: {
                "enabled": True,
                "login_required": True,
                "state": "LOGIN_REQUIRED",
                "account_ok": False,
                "bridge_health_ok": True,
                "market_data_ok": True,
                "position_count": 0,
                "errors": ["positions timeout"],
            },
            telegram_alerts_enabled=True,
            send_telegram_alert=lambda **kwargs: captured.update(kwargs) or {"ok": True, "sent": True, "reason": "delivered"},
        )

        self.assertTrue(result["ok"])
        self.assertFalse(result["noop"])
        self.assertEqual(result["reason"], "login_required_alert_attempted")
        self.assertEqual(captured["alert_key"], "ibkr-login-required")
        self.assertIn("IBKR login required", captured["message"])

    def test_ibkr_login_alert_noops_when_login_not_required(self):
        now_ny = datetime(2026, 4, 8, 10, 0, tzinfo=NY_TZ)
        result = execute_ibkr_login_alert(
            now_ny=now_ny,
            get_ibkr_operational_status=lambda: {"enabled": True, "login_required": False, "state": "READY"},
            telegram_alerts_enabled=True,
            send_telegram_alert=lambda **kwargs: {"unexpected": True},
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["noop"])
        self.assertEqual(result["reason"], "login_not_required")

    def test_ibkr_login_alert_noops_when_state_is_not_login_required(self):
        now_ny = datetime(2026, 4, 8, 10, 0, tzinfo=NY_TZ)
        result = execute_ibkr_login_alert(
            now_ny=now_ny,
            get_ibkr_operational_status=lambda: {"enabled": True, "login_required": True, "state": "DEGRADED"},
            telegram_alerts_enabled=True,
            send_telegram_alert=lambda **kwargs: {"unexpected": True},
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["noop"])
        self.assertEqual(result["reason"], "state_not_login_required")


if __name__ == "__main__":
    unittest.main()
