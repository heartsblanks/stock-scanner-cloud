import time
import unittest
from unittest.mock import patch

from orchestration.app_runtime import handle_scan_request


class AppRuntimeTests(unittest.TestCase):
    def test_handle_scan_request_runs_ibkr_when_bridge_available(self):
        ibkr_calls = []

        result = handle_scan_request(
            {"paper_trade": True, "mode": "core_one"},
            run_ibkr_scan=lambda payload: {"ok": True, "scan_id": "unused-primary", "mode": payload["mode"]},
            shadow_mode_enabled=False,
            ibkr_bridge_enabled=True,
            run_ibkr_shadow_scans=lambda payload: ibkr_calls.append(payload) or {"ok": True, "scan_id": "ibkr-1"},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["scan_id"], "ibkr-1")
        self.assertEqual(len(ibkr_calls), 1)
        self.assertEqual(result["execution_broker"], "IBKR")
        self.assertTrue(result["parallel_runs"]["ibkr"]["ok"])

    def test_handle_scan_request_runs_ibkr_only(self):
        def ibkr_scan(_payload):
            time.sleep(0.05)
            return {"ok": True, "scan_id": "ibkr-2"}

        started = time.perf_counter()
        result = handle_scan_request(
            {"paper_trade": True, "mode": "core_one"},
            run_ibkr_scan=lambda _payload: {"ok": True, "scan_id": "unused-primary"},
            shadow_mode_enabled=True,
            ibkr_bridge_enabled=True,
            run_ibkr_shadow_scans=ibkr_scan,
        )
        elapsed = time.perf_counter() - started

        self.assertTrue(result["ok"])
        self.assertIn("parallel_runs", result)
        self.assertTrue(result["parallel_runs"]["ibkr"]["ok"])
        self.assertGreaterEqual(result["parallel_runs"]["ibkr"]["duration_ms"], 0)
        self.assertGreaterEqual(elapsed, 0.045)

    def test_handle_scan_request_ignores_legacy_alpaca_flag_and_runs_ibkr(self):
        ibkr_calls = []

        with patch.dict("os.environ", {"ENABLE_IBKR_SHADOW_MODE": "false"}, clear=False):
            result = handle_scan_request(
                {"paper_trade": True, "mode": "core_one"},
                run_ibkr_scan=lambda payload: {"ok": True, "scan_id": "unused-primary"},
                shadow_mode_enabled=True,
                ibkr_bridge_enabled=True,
                run_ibkr_shadow_scans=lambda payload: ibkr_calls.append(payload) or {"ok": True, "scan_id": "ibkr-only"},
            )

        self.assertEqual(len(ibkr_calls), 1)
        self.assertTrue(result["ok"])
        self.assertEqual(result["scan_id"], "ibkr-only")
        self.assertTrue(result["parallel_runs"]["ibkr"]["ok"])

    def test_handle_scan_request_returns_error_when_ibkr_unavailable(self):
        result = handle_scan_request(
            {"paper_trade": True, "mode": "core_one"},
            run_ibkr_scan=lambda payload: {"ok": True, "scan_id": "unused-primary"},
            shadow_mode_enabled=False,
            ibkr_bridge_enabled=False,
            run_ibkr_shadow_scans=lambda payload: {"ok": True, "scan_id": "ibkr-unavailable"},
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "ibkr_bridge_unavailable")
        self.assertEqual(result["placed_count"], 0)
        self.assertEqual(result["results"], [])


if __name__ == "__main__":
    unittest.main()
