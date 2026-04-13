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
        ]

        result = app.get_ibkr_operational_status()

        self.assertEqual(result["state"], "MARKET_DATA_UNAVAILABLE")
        self.assertTrue(result["account_ok"])
        self.assertFalse(result["market_data_ok"])
        self.assertTrue(result["login_required"])


if __name__ == "__main__":
    unittest.main()
