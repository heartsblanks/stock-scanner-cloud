import os
import unittest
import sys
from datetime import datetime
from types import ModuleType
from unittest.mock import patch
from zoneinfo import ZoneInfo

if "pandas_market_calendars" not in sys.modules:
    sys.modules["pandas_market_calendars"] = ModuleType("pandas_market_calendars")
if "psycopg" not in sys.modules:
    fake_psycopg = ModuleType("psycopg")
    fake_psycopg_rows = ModuleType("psycopg.rows")
    fake_psycopg_rows.dict_row = object()
    fake_psycopg.rows = fake_psycopg_rows
    sys.modules["psycopg"] = fake_psycopg
    sys.modules["psycopg.rows"] = fake_psycopg_rows

from analytics.trade_scan import calculate_position_sizing, evaluate_symbol
from core.paper_trade_config import get_paper_trade_limits


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
                "volume": 2500 if minute_offset == len(base_prices) - 1 else 1000,
            }
        )
    return candles


def benchmark(direction: str, market: str) -> dict:
    return {
        market: direction,
        f"{market}_TREND_QUALITY": True,
        f"{market}_TREND_AVAILABLE": True,
        f"{market}_PRICE_VWAP_ALIGNED": True,
        f"{market}_SLOPE_ALIGNED": True,
    }


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
                    benchmark_directions=benchmark("BUY", "NASDAQ"),
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
                    benchmark_directions=benchmark("BUY", "NASDAQ"),
                    current_open_positions=0,
                    current_open_exposure=0.0,
                )

        self.assertEqual(result["decision"], "REJECTED")
        self.assertEqual(result["final_reason"], "Later in session: only stronger names allowed.")
        self.assertFalse(result["checks"]["late_session_strict_rule"])

    def test_power_hour_long_priority_nine_stock_is_rejected(self):
        info = {"symbol": "GOOGL", "type": "stock", "priority": 9, "market": "NASDAQ"}
        candles = build_valid_breakout_candles()

        with patch.dict(os.environ, {"ENABLE_LATE_SESSION_HARD_BLOCK": "false"}, clear=False):
            with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 14, 45, tzinfo=NY_TZ)):
                result = evaluate_symbol(
                    name="Alphabet",
                    info=info,
                    candles=candles,
                    account_size=100000.0,
                    benchmark_directions=benchmark("BUY", "NASDAQ"),
                    current_open_positions=0,
                    current_open_exposure=0.0,
                )

        self.assertEqual(result["decision"], "REJECTED")
        self.assertEqual(result["final_reason"], "Power-hour long setups require top-tier priority.")
        self.assertFalse(result["checks"]["power_hour_long_rule"])

    def test_power_hour_long_priority_ten_stock_can_still_pass(self):
        info = {"symbol": "GOOGL", "type": "stock", "priority": 10, "market": "NASDAQ"}
        candles = build_valid_breakout_candles()

        with patch.dict(os.environ, {"ENABLE_LATE_SESSION_HARD_BLOCK": "false"}, clear=False):
            with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 14, 45, tzinfo=NY_TZ)):
                result = evaluate_symbol(
                    name="Alphabet",
                    info=info,
                    candles=candles,
                    account_size=100000.0,
                    benchmark_directions=benchmark("BUY", "NASDAQ"),
                    current_open_positions=0,
                    current_open_exposure=0.0,
                )

        self.assertTrue(result["checks"]["power_hour_long_rule"])
        self.assertEqual(result["metrics"]["late_long_confidence_boost"], 4)


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

    def test_position_sizing_enforces_four_slots_for_2000_account(self):
        with patch.dict(
            os.environ,
            {
                "PAPER_TRADE_MAX_CAPITAL_ALLOCATION_PCT": "1.0",
                "PAPER_TRADE_ENFORCE_MAX_POSITIONS": "true",
                "PAPER_TRADE_MAX_POSITIONS": "4",
            },
            clear=False,
        ):
            sizing = calculate_position_sizing(
                account_size=2000.0,
                entry=100.0,
                stop=98.0,
                current_open_positions=1,
                current_open_exposure=500.0,
            )
            limits = get_paper_trade_limits()

        self.assertTrue(sizing["position_limit_enforced"])
        self.assertEqual(sizing["max_positions"], 4)
        self.assertEqual(sizing["remaining_slots"], 3)
        self.assertAlmostEqual(sizing["remaining_allocatable_capital"], 1500.0, places=2)
        self.assertAlmostEqual(sizing["per_trade_notional"], 500.0, places=2)
        self.assertEqual(sizing["shares"], 5)
        self.assertTrue(limits["position_limit_enforced"])
        self.assertEqual(limits["max_positions"], 4)

    def test_position_sizing_allows_fractional_shares_when_enabled(self):
        with patch.dict(
            os.environ,
            {
                "ENABLE_FRACTIONAL_SHARES": "true",
                "FRACTIONAL_SHARE_DECIMALS": "4",
                "PAPER_TRADE_MAX_CAPITAL_ALLOCATION_PCT": "1.0",
                "PAPER_TRADE_ENFORCE_MAX_POSITIONS": "false",
                "PAPER_TRADE_MAX_POSITIONS": "10",
            },
            clear=False,
        ):
            sizing = calculate_position_sizing(
                account_size=500.0,
                entry=600.0,
                stop=590.0,
                current_open_positions=0,
                current_open_exposure=0.0,
            )

        self.assertGreater(sizing["shares"], 0.0)
        self.assertLess(sizing["shares"], 1.0)


class TradeScanTimePenaltyTests(unittest.TestCase):
    def test_valid_breakout_passes_atr_noise_filter(self):
        info = {"symbol": "GOOGL", "type": "stock", "priority": 9, "market": "NASDAQ"}
        candles = build_valid_breakout_candles()
        candles.append(
            {
                "datetime": "2026-04-01 09:45:00",
                "open": 100.20,
                "high": 100.65,
                "low": 98.50,
                "close": 100.55,
                "volume": 2500,
            }
        )

        with patch.dict(os.environ, {"ENABLE_LATE_SESSION_HARD_BLOCK": "false"}, clear=False):
            with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 10, 15, tzinfo=NY_TZ)):
                result = evaluate_symbol(
                    name="Alphabet",
                    info=info,
                    candles=candles,
                    account_size=100000.0,
                    benchmark_directions=benchmark("BUY", "NASDAQ"),
                    current_open_positions=0,
                    current_open_exposure=0.0,
                )

        self.assertTrue(result["checks"]["atr_noise_filter"])
        self.assertGreater(result["metrics"]["stop_to_atr_ratio"], result["metrics"]["min_stop_to_atr_ratio"])

    def test_primary_mode_uses_stricter_confidence_floor_and_priority_cap(self):
        info = {"symbol": "RIVN", "type": "stock", "priority": 7, "market": "NASDAQ", "mode": "primary"}
        candles = build_valid_breakout_candles()
        candles.append(
            {
                "datetime": "2026-04-01 09:45:00",
                "open": 100.20,
                "high": 100.65,
                "low": 98.50,
                "close": 100.55,
                "volume": 2500,
            }
        )

        with patch.dict(os.environ, {"ENABLE_LATE_SESSION_HARD_BLOCK": "false"}, clear=False):
            with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 10, 15, tzinfo=NY_TZ)):
                result = evaluate_symbol(
                    name="Rivian",
                    info=info,
                    candles=candles,
                    account_size=100000.0,
                    benchmark_directions=benchmark("BUY", "NASDAQ"),
                    current_open_positions=0,
                    current_open_exposure=0.0,
                )

        self.assertEqual(result["decision"], "REJECTED")
        self.assertEqual(result["final_reason"], "Final confidence below threshold.")
        self.assertEqual(result["metrics"]["mode_confidence_floor"], 95)
        self.assertEqual(result["metrics"]["required_confidence"], 95)
        self.assertEqual(result["metrics"]["confidence_quality_cap"], 94)

    def test_rejects_trade_when_stop_is_too_tight_for_recent_atr(self):
        info = {"symbol": "PLUG", "type": "stock", "priority": 9, "market": "SP500"}
        candles = build_valid_breakout_candles()
        candles.extend(
            [
                {
                    "datetime": "2026-04-01 09:45:00",
                    "open": 100.20,
                    "high": 101.50,
                    "low": 99.35,
                    "close": 100.12,
                    "volume": 1000,
                },
                {
                    "datetime": "2026-04-01 09:46:00",
                    "open": 100.10,
                    "high": 101.35,
                    "low": 99.20,
                    "close": 100.08,
                    "volume": 1000,
                },
                {
                    "datetime": "2026-04-01 09:47:00",
                    "open": 100.08,
                    "high": 101.40,
                    "low": 99.15,
                    "close": 100.16,
                    "volume": 1000,
                },
                {
                    "datetime": "2026-04-01 09:48:00",
                    "open": 100.16,
                    "high": 101.45,
                    "low": 99.10,
                    "close": 100.18,
                    "volume": 1000,
                },
                {
                    "datetime": "2026-04-01 09:49:00",
                    "open": 100.18,
                    "high": 101.48,
                    "low": 99.05,
                    "close": 100.55,
                    "volume": 2500,
                },
            ]
        )

        with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 10, 0, tzinfo=NY_TZ)):
            result = evaluate_symbol(
                name="Plug Power",
                info=info,
                candles=candles,
                account_size=100000.0,
                benchmark_directions=benchmark("BUY", "SP500"),
                current_open_positions=0,
                current_open_exposure=0.0,
            )

        self.assertEqual(result["decision"], "REJECTED")
        self.assertEqual(result["final_reason"], "Stop too tight for current intraday volatility.")
        self.assertFalse(result["checks"]["atr_noise_filter"])
        self.assertLess(result["metrics"]["stop_to_atr_ratio"], result["metrics"]["min_stop_to_atr_ratio"])

    def test_post_noon_time_penalty_is_stronger(self):
        info = {"symbol": "GOOGL", "type": "stock", "priority": 9, "market": "NASDAQ"}
        candles = build_valid_breakout_candles()
        candles.append(
            {
                "datetime": "2026-04-01 09:45:00",
                "open": 100.20,
                "high": 100.65,
                "low": 98.50,
                "close": 100.55,
                "volume": 2500,
            }
        )

        with patch.dict(os.environ, {"ENABLE_LATE_SESSION_HARD_BLOCK": "false"}, clear=False):
            with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 12, 0, tzinfo=NY_TZ)):
                result = evaluate_symbol(
                    name="Alphabet",
                    info=info,
                    candles=candles,
                    account_size=100000.0,
                    benchmark_directions=benchmark("BUY", "NASDAQ"),
                    current_open_positions=0,
                    current_open_exposure=0.0,
                )

        self.assertEqual(result["metrics"]["minutes_after_open"], 150)
        self.assertEqual(result["metrics"]["time_penalty"], 10)

    def test_late_long_setups_require_extra_confidence(self):
        info = {"symbol": "GOOGL", "type": "stock", "priority": 9, "market": "NASDAQ"}
        candles = build_valid_breakout_candles()

        with patch.dict(os.environ, {"ENABLE_LATE_SESSION_HARD_BLOCK": "false"}, clear=False):
            with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 13, 15, tzinfo=NY_TZ)):
                result = evaluate_symbol(
                    name="Alphabet",
                    info=info,
                    candles=candles,
                    account_size=100000.0,
                    benchmark_directions=benchmark("BUY", "NASDAQ"),
                    current_open_positions=0,
                    current_open_exposure=0.0,
                )

        self.assertEqual(result["metrics"]["late_long_confidence_boost"], 2)
        self.assertEqual(result["metrics"]["required_confidence"], 77)

    def test_short_setups_get_extra_midday_time_penalty(self):
        info = {"symbol": "PLUG", "type": "stock", "priority": 9, "market": "SP500"}
        candles = [
            {
                "datetime": f"2026-04-01 09:{30 + minute_offset:02d}:00",
                "open": price + 0.10,
                "high": price + 0.20,
                "low": price + 0.05,
                "close": price,
                "volume": 2500 if minute_offset == 14 else 1000,
            }
            for minute_offset, price in enumerate([
                100.60,
                100.50,
                100.45,
                100.40,
                100.35,
                100.30,
                100.25,
                100.20,
                100.15,
                100.10,
                100.05,
                100.00,
                99.95,
                99.90,
                99.55,
            ])
        ]
        candles.append(
            {
                "datetime": "2026-04-01 09:45:00",
                "open": 99.60,
                "high": 99.65,
                "low": 98.50,
                "close": 99.55,
                "volume": 2500,
            }
        )

        with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 12, 0, tzinfo=NY_TZ)):
            result = evaluate_symbol(
                name="Plug Power",
                info=info,
                candles=candles,
                account_size=100000.0,
                benchmark_directions=benchmark("SELL", "SP500"),
                current_open_positions=0,
                current_open_exposure=0.0,
            )

        self.assertEqual(result["metrics"]["direction"], "SELL")
        self.assertEqual(result["metrics"]["time_penalty"], 20)
        self.assertEqual(result["metrics"]["required_confidence"], 82)

    def test_short_setups_use_higher_confidence_threshold(self):
        info = {"symbol": "PLUG", "type": "stock", "priority": 9, "market": "SP500"}
        candles = [
            {
                "datetime": f"2026-04-01 09:{30 + minute_offset:02d}:00",
                "open": price + 0.10,
                "high": price + 0.20,
                "low": price + 0.05,
                "close": price,
                "volume": 2500 if minute_offset == 14 else 1000,
            }
            for minute_offset, price in enumerate([
                100.60,
                100.50,
                100.45,
                100.40,
                100.35,
                100.30,
                100.25,
                100.20,
                100.15,
                100.10,
                100.05,
                100.00,
                99.95,
                99.90,
                99.55,
            ])
        ]
        candles.append(
            {
                "datetime": "2026-04-01 09:45:00",
                "open": 99.60,
                "high": 99.65,
                "low": 99.20,
                "close": 99.55,
                "volume": 2500,
            }
        )

        with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 11, 0, tzinfo=NY_TZ)):
            result = evaluate_symbol(
                name="Plug Power",
                info=info,
                candles=candles,
                account_size=100000.0,
                benchmark_directions=benchmark("SELL", "SP500"),
                current_open_positions=0,
                current_open_exposure=0.0,
            )

        self.assertEqual(result["metrics"]["direction"], "SELL")
        self.assertEqual(result["metrics"]["required_confidence"], 82)
        self.assertTrue(result["checks"]["confidence_threshold"])
        self.assertEqual(result["decision"], "VALID")
        self.assertEqual(result["final_reason"], "Price is below OR low and below VWAP.")

    def test_breakout_requires_volume_confirmation(self):
        info = {"symbol": "GOOGL", "type": "stock", "priority": 9, "market": "NASDAQ"}
        candles = build_valid_breakout_candles()
        candles[-1]["volume"] = 500

        with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 10, 0, tzinfo=NY_TZ)):
            result = evaluate_symbol(
                name="Alphabet",
                info=info,
                candles=candles,
                account_size=100000.0,
                benchmark_directions=benchmark("BUY", "NASDAQ"),
                current_open_positions=0,
                current_open_exposure=0.0,
            )

        self.assertEqual(result["decision"], "REJECTED")
        self.assertEqual(result["final_reason"], "Breakout volume confirmation failed.")
        self.assertFalse(result["checks"]["volume_confirmation"])

    def test_anti_chase_rejects_extended_breakout_from_opening_range(self):
        info = {"symbol": "GOOGL", "type": "stock", "priority": 9, "market": "NASDAQ"}
        candles = build_valid_breakout_candles()
        candles.append(
            {
                "datetime": "2026-04-01 09:45:00",
                "open": 101.80,
                "high": 102.10,
                "low": 101.70,
                "close": 102.00,
                "volume": 3000,
            }
        )

        with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 10, 0, tzinfo=NY_TZ)):
            result = evaluate_symbol(
                name="Alphabet",
                info=info,
                candles=candles,
                account_size=100000.0,
                benchmark_directions=benchmark("BUY", "NASDAQ"),
                current_open_positions=0,
                current_open_exposure=0.0,
            )

        self.assertEqual(result["decision"], "REJECTED")
        self.assertEqual(result["final_reason"], "Move already too extended from opening range.")
        self.assertFalse(result["checks"]["anti_chase_filter"])

    def test_benchmark_trend_quality_must_be_confirmed(self):
        info = {"symbol": "GOOGL", "type": "stock", "priority": 9, "market": "NASDAQ"}
        weak_benchmark = benchmark("BUY", "NASDAQ")
        weak_benchmark["NASDAQ_TREND_QUALITY"] = False

        with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 10, 0, tzinfo=NY_TZ)):
            result = evaluate_symbol(
                name="Alphabet",
                info=info,
                candles=build_valid_breakout_candles(),
                account_size=100000.0,
                benchmark_directions=weak_benchmark,
                current_open_positions=0,
                current_open_exposure=0.0,
            )

        self.assertEqual(result["decision"], "REJECTED")
        self.assertEqual(result["final_reason"], "Benchmark trend quality failed.")
        self.assertFalse(result["checks"]["benchmark_trend_quality"])


if __name__ == "__main__":
    unittest.main()
