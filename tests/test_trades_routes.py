import unittest
from datetime import datetime, timezone

HAS_FLASK = True
try:
    from routes.trades import _format_trade_log_time, _safe_live_float
except ModuleNotFoundError as exc:
    if exc.name != "flask":
        raise
    HAS_FLASK = False
    _format_trade_log_time = None
    _safe_live_float = None


@unittest.skipUnless(HAS_FLASK, "flask dependency is not available in this local unittest environment")
class TradeRouteFormattingTests(unittest.TestCase):
    def test_format_trade_log_time_accepts_datetime(self):
        value = datetime(2026, 4, 9, 19, 45, 12, tzinfo=timezone.utc)
        self.assertEqual(_format_trade_log_time(value), "19:45")

    def test_format_trade_log_time_accepts_iso_string(self):
        self.assertEqual(_format_trade_log_time("2026-04-09T19:45:12+00:00"), "19:45")

    def test_format_trade_log_time_accepts_rfc_string(self):
        self.assertEqual(_format_trade_log_time("Thu, 09 Apr 2026 19:45:12 GMT"), "19:45")

    def test_safe_live_float_filters_ibkr_sentinel_values(self):
        self.assertIsNone(_safe_live_float("1.7976931348623157e+308"))
        self.assertIsNone(_safe_live_float(float("inf")))
        self.assertIsNone(_safe_live_float("not-a-number"))
        self.assertEqual(_safe_live_float("68.04"), 68.04)


if __name__ == "__main__":
    unittest.main()
