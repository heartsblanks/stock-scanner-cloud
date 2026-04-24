import unittest
from unittest.mock import patch

import app


class IbkrOperationalStatusTests(unittest.TestCase):
    @patch("app.ibkr_bridge_enabled", return_value=True)
    @patch("app._account_equity_from_broker_account", return_value=125000.0)
    @patch("app.ibkr_bridge_get")
    def test_ready_when_bridge_account_and_market_data_pass(self, mock_bridge_get, _mock_equity, _mock_enabled):
        mock_bridge_get.side_effect = [
            {"ok": True, "ibkr": {"configured": True}},
            {"account_id": "DU12345"},
            [],
            [{"symbol": "SPY"}, {"symbol": "SPY"}],
        ]

        result = app.get_ibkr_operational_status()

        self.assertEqual(result["state"], "READY")
        self.assertTrue(result["bridge_health_ok"])
        self.assertTrue(result["account_ok"])
        self.assertTrue(result["market_data_ok"])
        self.assertFalse(result["login_required"])

    @patch("app.ibkr_bridge_enabled", return_value=True)
    @patch("app.ibkr_bridge_get")
    def test_login_required_when_bridge_health_works_but_account_fails(self, mock_bridge_get, _mock_enabled):
        mock_bridge_get.side_effect = [
            {"ok": True, "ibkr": {"configured": True}},
            RuntimeError("not logged in"),
            RuntimeError("no positions"),
            [],
        ]

        result = app.get_ibkr_operational_status()

        self.assertEqual(result["state"], "LOGIN_REQUIRED")
        self.assertTrue(result["bridge_health_ok"])
        self.assertFalse(result["account_ok"])
        self.assertTrue(result["login_required"])
        self.assertIn("account:", result["errors"][0])

    @patch("app.ibkr_bridge_enabled", return_value=True)
    @patch("app._account_equity_from_broker_account", return_value=125000.0)
    @patch("app.ibkr_bridge_get")
    def test_market_data_unavailable_when_account_works_but_candles_do_not(self, mock_bridge_get, _mock_equity, _mock_enabled):
        mock_bridge_get.side_effect = [
            {"ok": True, "ibkr": {"configured": True}},
            {"account_id": "DU12345"},
            [],
            [],
        ]

        result = app.get_ibkr_operational_status()

        self.assertEqual(result["state"], "MARKET_DATA_UNAVAILABLE")
        self.assertTrue(result["account_ok"])
        self.assertFalse(result["market_data_ok"])
        self.assertFalse(result["login_required"])

    @patch("app.ibkr_bridge_enabled", return_value=True)
    @patch("app.ibkr_bridge_get")
    def test_ready_when_positions_and_market_data_work_even_if_account_times_out(self, mock_bridge_get, _mock_enabled):
        mock_bridge_get.side_effect = [
            {"ok": True, "ibkr": {"configured": True}},
            RuntimeError("account timeout"),
            [{"symbol": "NVDA"}],
            [{"symbol": "SPY"}],
        ]

        result = app.get_ibkr_operational_status()

        self.assertEqual(result["state"], "READY")
        self.assertFalse(result["account_ok"])
        self.assertTrue(result["session_probe_ok"])
        self.assertTrue(result["market_data_ok"])
        self.assertFalse(result["login_required"])

    @patch("app.ibkr_bridge_enabled", return_value=True)
    @patch("app.ibkr_bridge_get")
    def test_degraded_not_login_required_when_low_call_positions_probe_times_out(self, mock_bridge_get, _mock_enabled):
        mock_bridge_get.side_effect = [
            {"ok": True, "ibkr": {"configured": True}},
            RuntimeError("positions timeout"),
        ]

        with patch.dict(
            "os.environ",
            {
                "IBKR_LOW_CALL_MODE": "true",
                "IBKR_STATUS_INCLUDE_ACCOUNT_PROBE": "false",
                "IBKR_STATUS_INCLUDE_MARKET_DATA_PROBE": "false",
            },
            clear=False,
        ):
            result = app.get_ibkr_operational_status()

        self.assertEqual(result["state"], "DEGRADED")
        self.assertFalse(result["login_required"])
        self.assertIn("positions:", result["errors"][0])

    @patch("app.ibkr_bridge_enabled", return_value=True)
    @patch("app.ibkr_bridge_get")
    def test_login_required_when_low_call_positions_probe_reports_auth_failure(self, mock_bridge_get, _mock_enabled):
        mock_bridge_get.side_effect = [
            {"ok": True, "ibkr": {"configured": True}},
            RuntimeError("not logged in"),
        ]

        with patch.dict(
            "os.environ",
            {
                "IBKR_LOW_CALL_MODE": "true",
                "IBKR_STATUS_INCLUDE_ACCOUNT_PROBE": "false",
                "IBKR_STATUS_INCLUDE_MARKET_DATA_PROBE": "false",
            },
            clear=False,
        ):
            result = app.get_ibkr_operational_status()

        self.assertEqual(result["state"], "LOGIN_REQUIRED")
        self.assertTrue(result["login_required"])

    @patch("app.ibkr_bridge_enabled", return_value=True)
    @patch("app.ibkr_bridge_get")
    def test_login_required_when_low_call_positions_probe_returns_service_unavailable(self, mock_bridge_get, _mock_enabled):
        mock_bridge_get.side_effect = [
            {"ok": True, "ibkr": {"configured": True}},
            RuntimeError(
                "IBKR bridge request failed during GET /positions: "
                "503 Server Error: SERVICE UNAVAILABLE for url: http://10.132.0.2:8090/positions"
            ),
        ]

        with patch.dict(
            "os.environ",
            {
                "IBKR_LOW_CALL_MODE": "true",
                "IBKR_STATUS_INCLUDE_ACCOUNT_PROBE": "false",
                "IBKR_STATUS_INCLUDE_MARKET_DATA_PROBE": "false",
            },
            clear=False,
        ):
            result = app.get_ibkr_operational_status()

        self.assertEqual(result["state"], "LOGIN_REQUIRED")
        self.assertTrue(result["login_required"])


if __name__ == "__main__":
    unittest.main()
