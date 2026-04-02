import unittest
from unittest import SkipTest
from unittest.mock import patch

try:
    from alpaca.alpaca_http import _log_alpaca_call
except ModuleNotFoundError as exc:
    if exc.name in {"psycopg", "requests"}:
        raise SkipTest(f"{exc.name} dependency is not available in this local unittest environment")
    raise


class AlpacaHttpLoggingTests(unittest.TestCase):
    def test_successful_call_is_not_persisted_when_audit_disabled(self):
        with patch.dict("os.environ", {"ENABLE_ALPACA_HTTP_AUDIT": "false"}, clear=False):
            with patch("alpaca.alpaca_http.insert_alpaca_api_log") as insert_log:
                _log_alpaca_call(
                    "GET",
                    "https://paper-api.alpaca.markets/v2/account",
                    None,
                    None,
                    200,
                    '{"ok":true}',
                    success=True,
                    duration_ms=12,
                )

        insert_log.assert_not_called()

    def test_failed_call_is_persisted_even_when_audit_disabled(self):
        with patch.dict("os.environ", {"ENABLE_ALPACA_HTTP_AUDIT": "false"}, clear=False):
            with patch("alpaca.alpaca_http.insert_alpaca_api_log") as insert_log:
                _log_alpaca_call(
                    "GET",
                    "https://paper-api.alpaca.markets/v2/account",
                    None,
                    None,
                    500,
                    '{"message":"boom"}',
                    success=False,
                    error_message="boom",
                    duration_ms=25,
                )

        insert_log.assert_called_once()

    def test_successful_call_is_persisted_when_audit_enabled(self):
        with patch.dict("os.environ", {"ENABLE_ALPACA_HTTP_AUDIT": "true"}, clear=False):
            with patch("alpaca.alpaca_http.insert_alpaca_api_log") as insert_log:
                _log_alpaca_call(
                    "POST",
                    "https://paper-api.alpaca.markets/v2/orders",
                    None,
                    {"symbol": "PLUG"},
                    200,
                    '{"id":"abc"}',
                    success=True,
                    duration_ms=42,
                )

        insert_log.assert_called_once()


if __name__ == "__main__":
    unittest.main()
