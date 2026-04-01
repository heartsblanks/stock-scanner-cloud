import unittest
from datetime import datetime, timezone

from trade_math import (
    compute_duration_minutes,
    compute_realized_pnl,
    compute_realized_pnl_percent,
    infer_direction,
    normalize_trade_key,
    resolve_lifecycle_side,
)


class TradeMathTests(unittest.TestCase):
    def test_infer_direction_prefers_side_for_losing_buy_trade(self):
        self.assertEqual(
            infer_direction(14.01, 13.95, 13.95, 14.25, side="BUY"),
            "LONG",
        )

    def test_compute_realized_pnl_for_long_and_short(self):
        self.assertAlmostEqual(compute_realized_pnl(100, 105, 10, "LONG"), 50.0, places=6)
        self.assertAlmostEqual(compute_realized_pnl(100, 95, 10, "SHORT"), 50.0, places=6)

    def test_compute_realized_pnl_percent(self):
        self.assertAlmostEqual(compute_realized_pnl_percent(100, 95, "LONG"), -5.0, places=6)
        self.assertAlmostEqual(compute_realized_pnl_percent(100, 95, "SHORT"), 5.0, places=6)

    def test_compute_duration_minutes(self):
        entry = datetime(2026, 3, 31, 14, 20, tzinfo=timezone.utc)
        exit_ts = datetime(2026, 3, 31, 14, 25, tzinfo=timezone.utc)
        self.assertEqual(compute_duration_minutes(entry, exit_ts), 5.0)

    def test_resolve_lifecycle_side_and_trade_key(self):
        self.assertEqual(resolve_lifecycle_side({"side": "buy"}, None), "BUY")
        self.assertEqual(resolve_lifecycle_side({}, "SHORT"), "SELL")
        self.assertEqual(normalize_trade_key("AAPL", "parent-1", "order-1"), "parent-1")
        self.assertEqual(normalize_trade_key("AAPL", "", "order-1"), "order-1")


if __name__ == "__main__":
    unittest.main()
