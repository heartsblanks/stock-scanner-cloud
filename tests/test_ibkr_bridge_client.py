import unittest
import sys
from unittest.mock import Mock, patch

from brokers.ibkr_bridge_client import ibkr_bridge_get


class IbkrBridgeClientTests(unittest.TestCase):
    def test_ibkr_bridge_timeout_raises_clear_runtime_error(self):
        fake_requests = Mock()
        fake_requests.Timeout = type("Timeout", (Exception,), {})
        fake_requests.RequestException = type("RequestException", (Exception,), {})
        fake_requests.request.side_effect = fake_requests.Timeout("timed out")

        with patch.dict(
            "os.environ",
            {
                "IBKR_BRIDGE_BASE_URL": "https://ibkr-bridge.example.com",
                "IBKR_BRIDGE_TIMEOUT_SECONDS": "30",
            },
            clear=False,
        ):
            with patch.dict(sys.modules, {"requests": fake_requests}):
                with self.assertRaises(RuntimeError) as ctx:
                    ibkr_bridge_get("/orders/123/sync", timeout=8)

        self.assertIn("IBKR bridge timeout", str(ctx.exception))
        self.assertIn("GET /orders/123/sync", str(ctx.exception))
        self.assertIn("8s", str(ctx.exception))

    def test_ibkr_bridge_request_failure_raises_clear_runtime_error(self):
        fake_requests = Mock()
        fake_requests.Timeout = type("Timeout", (Exception,), {})
        fake_requests.RequestException = type("RequestException", (Exception,), {})
        fake_requests.request.side_effect = fake_requests.RequestException("connection aborted")

        with patch.dict(
            "os.environ",
            {
                "IBKR_BRIDGE_BASE_URL": "https://ibkr-bridge.example.com",
            },
            clear=False,
        ):
            with patch.dict(sys.modules, {"requests": fake_requests}):
                with self.assertRaises(RuntimeError) as ctx:
                    ibkr_bridge_get("/positions", timeout=5)

        self.assertIn("IBKR bridge request failed", str(ctx.exception))
        self.assertIn("GET /positions", str(ctx.exception))

    def test_ibkr_bridge_success_returns_json_payload(self):
        fake_requests = Mock()
        fake_requests.Timeout = type("Timeout", (Exception,), {})
        fake_requests.RequestException = type("RequestException", (Exception,), {})
        response = Mock()
        response.text = '{"ok": true}'
        response.json.return_value = {"ok": True}
        response.raise_for_status.return_value = None
        fake_requests.request.return_value = response

        with patch.dict(
            "os.environ",
            {
                "IBKR_BRIDGE_BASE_URL": "https://ibkr-bridge.example.com",
                "IBKR_BRIDGE_TOKEN": "secret-token",
            },
            clear=False,
        ):
            with patch.dict(sys.modules, {"requests": fake_requests}):
                result = ibkr_bridge_get("/health", timeout=4)

        self.assertEqual(result, {"ok": True})
        fake_requests.request.assert_called_once_with(
            method="GET",
            url="https://ibkr-bridge.example.com/health",
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer secret-token",
            },
            params=None,
            json=None,
            timeout=4,
        )


if __name__ == "__main__":
    unittest.main()
