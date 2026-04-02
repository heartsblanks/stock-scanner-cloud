import os
import unittest
from datetime import datetime
from unittest import SkipTest
from unittest.mock import patch
from zoneinfo import ZoneInfo

try:
    from analytics.trade_scan import calculate_position_sizing, evaluate_symbol
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
            with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 13, 0, tzinfo=NY_TZ)):
                result = evaluate_symbol(
                    name="Alphabet",
                    info=info,
                    candles=candles,
                    account_size=100000.0,
                    benchmark_directions={"NASDAQ": "BUY"},
                    current_open_positions=0,
                    current_open_exposure=0.0,
                )

        self.assertTrue(result["checks"]["late_session_strict_rule"])
        self.assertNotEqual(result["final_reason"], "Later in session: only stronger names allowed.")

    def test_late_session_priority_nine_stock_is_rejected_when_hard_block_enabled(self):
        info = {"symbol": "GOOGL", "type": "stock", "priority": 9, "market": "NASDAQ"}
        candles = build_valid_breakout_candles()

        with patch.dict(os.environ, {"ENABLE_LATE_SESSION_HARD_BLOCK": "true"}, clear=False):
            with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 13, 0, tzinfo=NY_TZ)):
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


class TradeScanSizingConfigTests(unittest.TestCase):
    def test_position_sizing_defaults_to_full_equity_without_position_cap(self):
        with patch.dict(
            os.environ,
            {
                "PAPER_TRADE_MAX_CAPITAL_ALLOCATION_PCT": "1.0",
                "PAPER_TRADE_ENFORCE_MAX_POSITIONS": "false",
                "PAPER_TRADE_MAX_POSITIONS": "10",
            },
            clear=False,
        ):
            sizing = calculate_position_sizing(
                account_size=100000.0,
                entry=250.0,
                stop=245.0,
                current_open_positions=4,
                current_open_exposure=25000.0,
            )

        self.assertFalse(sizing["position_limit_enforced"])
        self.assertEqual(sizing["remaining_slots"], 1)
        self.assertAlmostEqual(sizing["max_total_allocated_capital"], 100000.0, places=2)
        self.assertAlmostEqual(sizing["remaining_allocatable_capital"], 75000.0, places=2)
        self.assertEqual(sizing["shares"], 300)


class TradeScanTimePenaltyTests(unittest.TestCase):
    def test_post_noon_time_penalty_is_stronger(self):
        info = {"symbol": "GOOGL", "type": "stock", "priority": 9, "market": "NASDAQ"}
        candles = build_valid_breakout_candles()
        candles[-1]["low"] = 98.50

        with patch.dict(os.environ, {"ENABLE_LATE_SESSION_HARD_BLOCK": "false"}, clear=False):
            with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 12, 0, tzinfo=NY_TZ)):
                result = evaluate_symbol(
                    name="Alphabet",
                    info=info,
                    candles=candles,
                    account_size=100000.0,
                    benchmark_directions={"NASDAQ": "BUY"},
                    current_open_positions=0,
                    current_open_exposure=0.0,
                )

        self.assertEqual(result["metrics"]["minutes_after_open"], 150)
        self.assertEqual(result["metrics"]["time_penalty"], 10)


if __name__ == "__main__":
    unittest.main()
