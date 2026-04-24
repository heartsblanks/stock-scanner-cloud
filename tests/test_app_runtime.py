import time
import unittest
from unittest.mock import patch

from orchestration.app_runtime import close_all_paper_positions_for_broker, handle_scan_request


class FakeBroker:
    def get_open_positions(self):
        return []

    def cancel_open_orders_for_symbol(self, symbol):
        return []

    def close_position(self, symbol):
        return {}

    def get_order_by_id(self, order_id, nested=False):
        return {}

    def sync_order_by_id(self, order_id):
        return {"id": order_id, "status": "Filled"}


class AppRuntimeTests(unittest.TestCase):
    def test_close_all_paper_positions_for_broker_passes_sync_order_by_id(self):
        captured = {}
        broker = FakeBroker()

        def run_close_all_paper_positions(**kwargs):
            captured.update(kwargs)
            return kwargs["sync_order_by_id"]("close-1")

        result = close_all_paper_positions_for_broker(
            broker,
            run_close_all_paper_positions=run_close_all_paper_positions,
            execute_close_all_paper_positions=lambda **kwargs: {},
            get_managed_open_paper_trades_for_eod_close_for_broker=lambda broker: [],
            safe_insert_broker_order=lambda **kwargs: None,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            upsert_trade_lifecycle=lambda **kwargs: None,
            to_float_or_none=lambda value: None,
            parse_iso_utc=lambda value: value,
        )

        self.assertEqual(result["status"], "Filled")
        self.assertIs(captured["sync_order_by_id"].__self__, broker)
        self.assertIs(captured["sync_order_by_id"].__func__, FakeBroker.sync_order_by_id)

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

    def test_handle_scan_request_ignores_legacy_ibkr_flag_and_runs_ibkr(self):
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
