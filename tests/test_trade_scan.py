import os
import unittest
from datetime import datetime
from unittest import SkipTest
from unittest.mock import patch
from zoneinfo import ZoneInfo

try:
    from analytics.trade_scan import evaluate_symbol
except ModuleNotFoundError as exc:
    if exc.name == "requests":
        raise SkipTest("requests dependency is not available in this local unittest environment")
    raise


NY_TZ = ZoneInfo("America/New_York")


def build_valid_breakout_candles() -> list[dict]:
    candles = []
    base_prices = [
        99.40,
        99.50,
        99.55,
        99.60,
        99.65,
        99.70,
        99.75,
        99.80,
        99.85,
        99.90,
        99.95,
        100.00,
        100.05,
        100.10,
        100.55,
    ]
    for minute_offset, close_price in enumerate(base_prices):
        hour = 9
        minute = 30 + minute_offset
        candles.append(
            {
                "datetime": f"2026-04-01 {hour:02d}:{minute:02d}:00",
                "open": close_price - 0.10,
                "high": close_price - 0.05 if minute_offset < 14 else 100.40,
                "low": close_price - 0.25,
                "close": close_price,
            }
        )
    return candles


class TradeScanLateSessionTests(unittest.TestCase):
    def test_late_session_priority_nine_stock_can_still_be_valid_when_hard_block_disabled(self):
        info = {"symbol": "GOOGL", "type": "stock", "priority": 9, "market": "NASDAQ"}
        candles = build_valid_breakout_candles()

        with patch.dict(os.environ, {"ENABLE_LATE_SESSION_HARD_BLOCK": "false"}, clear=False):
            with patch("trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 13, 0, tzinfo=NY_TZ)):
                result = evaluate_symbol(
                    name="Alphabet",
                    info=info,
                    candles=candles,
                    account_size=100000.0,
                    benchmark_directions={"NASDAQ": "BUY"},
                    current_open_positions=0,
                    current_open_exposure=0.0,
                )

        self.assertEqual(result["decision"], "VALID")
        self.assertTrue(result["checks"]["late_session_strict_rule"])
        self.assertGreaterEqual(result["metrics"]["final_confidence"], 75)

    def test_late_session_priority_nine_stock_is_rejected_when_hard_block_enabled(self):
        info = {"symbol": "GOOGL", "type": "stock", "priority": 9, "market": "NASDAQ"}
        candles = build_valid_breakout_candles()

        with patch.dict(os.environ, {"ENABLE_LATE_SESSION_HARD_BLOCK": "true"}, clear=False):
            with patch("trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 13, 0, tzinfo=NY_TZ)):
                result = evaluate_symbol(
                    name="Alphabet",
                    info=info,
                    candles=candles,
                    account_size=100000.0,
                    benchmark_directions={"NASDAQ": "BUY"},
                    current_open_positions=0,
                    current_open_exposure=0.0,
                )

        self.assertEqual(result["decision"], "REJECTED")
        self.assertEqual(result["final_reason"], "Later in session: only stronger names allowed.")
        self.assertFalse(result["checks"]["late_session_strict_rule"])


if __name__ == "__main__":
    unittest.main()
