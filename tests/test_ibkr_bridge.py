import unittest
from unittest.mock import patch

try:
    from ibkr_bridge.app import app
except ModuleNotFoundError:  # pragma: no cover - local bare Python may not have Flask installed
    app = None


class FakeIbkrClient:
    def health_snapshot(self):
        return {"configured": True, "dependency_ready": True}

    def get_account(self):
        return {"account_id": "DU123", "equity": 100000.0, "buying_power": 200000.0, "status": "ACTIVE"}

    def get_positions(self):
        return [{"symbol": "AAPL", "qty": 10, "current_price": 210.0}]

    def get_open_orders(self):
        return [{"id": "1001", "symbol": "AAPL", "status": "Submitted"}]

    def get_order(self, order_id):
        if str(order_id) == "1001":
            return {"id": "1001", "symbol": "AAPL", "status": "Submitted"}
        return None

    def cancel_orders_by_symbol(self, symbol):
        return ["1001"] if str(symbol).upper() == "AAPL" else []

    def close_position(self, symbol):
        if str(symbol).upper() == "AAPL":
            return {"attempted": True, "placed": True, "symbol": "AAPL", "order_id": "2001", "status": "Submitted"}
        return {"attempted": False, "placed": False, "symbol": str(symbol).upper(), "reason": "no_open_position"}

    def place_paper_bracket_order(self, trade, max_notional=None):
        return {
            "attempted": True,
            "placed": True,
            "broker": "IBKR",
            "symbol": str(((trade or {}).get("metrics") or {}).get("symbol", "")).upper(),
            "broker_order_id": "3001",
            "broker_parent_order_id": "3001",
            "broker_order_status": "Submitted",
            "shares": 10,
            "client_order_id": "scanner-AAPL-BUY-1-10",
            "estimated_notional": 1000.0,
        }


@unittest.skipIf(app is None, "Flask is not installed in the local Python environment")
class IbkrBridgeApiTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def test_health_includes_ibkr_snapshot(self):
        with patch("ibkr_bridge.app.get_ibkr_client", return_value=FakeIbkrClient()):
            response = self.client.get("/health")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["service"], "ibkr-bridge")
        self.assertEqual(payload["ibkr"]["configured"], True)

    def test_account_route_returns_bridge_account(self):
        with patch("ibkr_bridge.app.get_ibkr_client", return_value=FakeIbkrClient()):
            response = self.client.get("/account")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["account_id"], "DU123")
        self.assertEqual(payload["equity"], 100000.0)

    def test_positions_route_returns_bridge_positions(self):
        with patch("ibkr_bridge.app.get_ibkr_client", return_value=FakeIbkrClient()):
            response = self.client.get("/positions")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload[0]["symbol"], "AAPL")

    def test_open_order_lookup_returns_not_found_when_absent(self):
        with patch("ibkr_bridge.app.get_ibkr_client", return_value=FakeIbkrClient()):
            response = self.client.get("/orders/9999")

        self.assertEqual(response.status_code, 503)
        payload = response.get_json()
        self.assertEqual(payload["error"], "service_unavailable")
        self.assertEqual(payload["operation"], "get_order:9999")

    def test_cancel_orders_by_symbol_route_returns_canceled_ids(self):
        with patch("ibkr_bridge.app.get_ibkr_client", return_value=FakeIbkrClient()):
            response = self.client.post("/orders/cancel-by-symbol", json={"symbol": "AAPL"})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["symbol"], "AAPL")
        self.assertEqual(payload["canceled_order_ids"], ["1001"])

    def test_close_position_route_returns_close_order(self):
        with patch("ibkr_bridge.app.get_ibkr_client", return_value=FakeIbkrClient()):
            response = self.client.post("/positions/close", json={"symbol": "AAPL"})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["attempted"])
        self.assertTrue(payload["placed"])
        self.assertEqual(payload["order_id"], "2001")

    def test_place_paper_bracket_route_returns_order_payload(self):
        with patch("ibkr_bridge.app.get_ibkr_client", return_value=FakeIbkrClient()):
            response = self.client.post(
                "/orders/paper-bracket",
                json={"trade": {"metrics": {"symbol": "AAPL"}}},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["placed"])
        self.assertEqual(payload["broker"], "IBKR")
        self.assertEqual(payload["broker_order_id"], "3001")

    def test_positions_route_logs_success_summary(self):
        with patch("ibkr_bridge.app.get_ibkr_client", return_value=FakeIbkrClient()):
            with patch("ibkr_bridge.app.log_info") as mock_log_info:
                response = self.client.get("/positions")

        self.assertEqual(response.status_code, 200)
        mock_log_info.assert_called_once()
        self.assertEqual(mock_log_info.call_args.args[0], "IBKR bridge positions fetched")
        self.assertEqual(mock_log_info.call_args.kwargs["operation"], "get_positions")
        self.assertEqual(mock_log_info.call_args.kwargs["count"], 1)

    def test_runtime_error_logs_exception(self):
        with patch("ibkr_bridge.app.get_ibkr_client", side_effect=RuntimeError("gateway unavailable")):
            with patch("ibkr_bridge.app.log_exception") as mock_log_exception:
                response = self.client.get("/positions")

        self.assertEqual(response.status_code, 503)
        mock_log_exception.assert_called_once()
        self.assertEqual(mock_log_exception.call_args.args[0], "IBKR bridge runtime error")


if __name__ == "__main__":
    unittest.main()
