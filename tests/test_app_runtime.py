import time
import unittest
from unittest.mock import patch

from orchestration.app_runtime import handle_scan_request


class AppRuntimeTests(unittest.TestCase):
    def test_handle_scan_request_skips_ibkr_when_shadow_mode_disabled(self):
        ibkr_calls = []

        result = handle_scan_request(
            {"paper_trade": True, "mode": "core_one"},
            run_alpaca_scan=lambda payload: {"ok": True, "scan_id": "alpaca-1", "mode": payload["mode"]},
            shadow_mode_enabled=False,
            ibkr_bridge_enabled=True,
            run_ibkr_shadow_scans=lambda payload: ibkr_calls.append(payload) or {"ok": True},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["scan_id"], "alpaca-1")
        self.assertEqual(ibkr_calls, [])
        self.assertNotIn("parallel_runs", result)

    def test_handle_scan_request_runs_alpaca_and_ibkr_in_parallel(self):
        def alpaca_scan(_payload):
            time.sleep(0.05)
            return {"ok": True, "scan_id": "alpaca-2"}

        def ibkr_scan(_payload):
            time.sleep(0.05)
            return {"ok": True, "scan_id": "ibkr-2"}

        started = time.perf_counter()
        result = handle_scan_request(
            {"paper_trade": True, "mode": "core_one"},
            run_alpaca_scan=alpaca_scan,
            shadow_mode_enabled=True,
            ibkr_bridge_enabled=True,
            run_ibkr_shadow_scans=ibkr_scan,
        )
        elapsed = time.perf_counter() - started

        self.assertTrue(result["ok"])
        self.assertIn("parallel_runs", result)
        self.assertIn("shadow_ibkr", result)
        self.assertTrue(result["parallel_runs"]["alpaca"]["ok"])
        self.assertTrue(result["parallel_runs"]["ibkr"]["ok"])
        self.assertIsInstance(result["parallel_runs"]["cross_broker_start_delta_ms"], int)
        self.assertIsInstance(result["parallel_runs"]["cross_broker_finish_delta_ms"], int)
        self.assertGreaterEqual(result["parallel_runs"]["alpaca"]["duration_ms"], 0)
        self.assertGreaterEqual(result["parallel_runs"]["ibkr"]["duration_ms"], 0)
        self.assertLess(elapsed, 0.095)

    def test_handle_scan_request_runs_ibkr_only_when_alpaca_execution_disabled(self):
        alpaca_calls = []
        ibkr_calls = []

        with patch.dict("os.environ", {"ENABLE_ALPACA_EXECUTION": "false"}, clear=False):
            result = handle_scan_request(
                {"paper_trade": True, "mode": "core_one"},
                run_alpaca_scan=lambda payload: alpaca_calls.append(payload) or {"ok": True, "scan_id": "alpaca-disabled"},
                shadow_mode_enabled=True,
                ibkr_bridge_enabled=True,
                run_ibkr_shadow_scans=lambda payload: ibkr_calls.append(payload) or {"ok": True, "scan_id": "ibkr-only"},
            )

        self.assertEqual(alpaca_calls, [])
        self.assertEqual(len(ibkr_calls), 1)
        self.assertTrue(result["ok"])
        self.assertEqual(result["scan_id"], "ibkr-only")
        self.assertFalse(result["alpaca_execution_enabled"])
        self.assertEqual(result["alpaca_skipped"]["reason"], "alpaca_execution_disabled")
        self.assertTrue(result["parallel_runs"]["alpaca"]["skipped"])
        self.assertTrue(result["parallel_runs"]["ibkr"]["ok"])

    def test_handle_scan_request_returns_skip_when_alpaca_disabled_and_ibkr_unavailable(self):
        with patch.dict("os.environ", {"ENABLE_ALPACA_EXECUTION": "false"}, clear=False):
            result = handle_scan_request(
                {"paper_trade": True, "mode": "core_one"},
                run_alpaca_scan=lambda payload: {"ok": True, "scan_id": "alpaca-disabled"},
                shadow_mode_enabled=False,
                ibkr_bridge_enabled=False,
                run_ibkr_shadow_scans=lambda payload: {"ok": True, "scan_id": "ibkr-unavailable"},
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["reason"], "alpaca_execution_disabled")
        self.assertEqual(result["placed_count"], 0)
        self.assertEqual(result["results"], [])


if __name__ == "__main__":
    unittest.main()
