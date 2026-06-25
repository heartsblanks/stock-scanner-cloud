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

from analytics.trade_scan import (
    _volume_confirmation_threshold,
    calculate_position_sizing,
    evaluate_symbol,
    fetch_instruments,
    run_scan,
)
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


def build_post_or_breakout_candles(*, final_close: float = 10.72, final_volume: float = 2500) -> list[dict]:
    candles = []
    for minute_offset in range(15):
        close_price = 10.00 + (minute_offset * 0.01)
        candles.append(
            {
                "datetime": f"2026-04-01 09:{30 + minute_offset:02d}:00",
                "open": close_price - 0.02,
                "high": 10.30 if minute_offset == 14 else close_price + 0.03,
                "low": 9.90 if minute_offset == 0 else close_price - 0.03,
                "close": close_price,
                "volume": 1000,
            }
        )
    candles.append(
        {
            "datetime": "2026-04-01 09:45:00",
            "open": final_close - 0.04,
            "high": final_close + 0.02,
            "low": final_close - 0.06,
            "close": final_close,
            "volume": final_volume,
        }
    )
    return candles


def benchmark(direction: str, market: str) -> dict:
    return {
        market: direction,
        f"{market}_RETURN": 0.001,
        f"{market}_TREND_QUALITY": True,
        f"{market}_TREND_AVAILABLE": True,
        f"{market}_PRICE_VWAP_ALIGNED": True,
        f"{market}_SLOPE_ALIGNED": True,
    }


class TradeScanQualityV2Tests(unittest.TestCase):
    def _info(self):
        return {"symbol": "SOFI", "type": "stock", "priority": 10, "market": "NASDAQ", "mode": "low_price"}

    def _base_env(self):
        return {
            "PAPER_LOW_PRICE_MODE_MIN_RELATIVE_VOLUME": "1.2",
            "PAPER_LOW_PRICE_MIN_3_CANDLE_REL_VOLUME": "1.1",
            "PAPER_BREAKOUT_CLOSE_BUFFER_PCT": "0.001",
            "PAPER_LOW_PRICE_BLOCK_AFTER_NY": "15:30",
            "PAPER_LOW_PRICE_LATE_STRICT_AFTER_NY": "14:30",
            "PAPER_MAX_BREAKOUT_OR_MULTIPLE_NON_CORE": "2.0",
            "PAPER_MAX_VWAP_EXTENSION_PCT_NON_CORE": "0.20",
        }

    def test_breakout_close_confirmation_rejects_weak_close(self):
        with patch.dict(os.environ, self._base_env(), clear=False):
            with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 10, 0, tzinfo=NY_TZ)):
                result = evaluate_symbol(
                    "SoFi",
                    self._info(),
                    build_post_or_breakout_candles(final_close=10.305, final_volume=2500),
                    2000,
                    benchmark("BUY", "NASDAQ"),
                )

        self.assertEqual(result["final_reason"], "breakout_close_confirmation_failed")
        self.assertFalse(result["checks"]["breakout_close_confirmation"])

    def test_inside_range_near_breakout_records_watch_reason(self):
        with patch.dict(os.environ, self._base_env(), clear=False):
            result = evaluate_symbol(
                "SoFi",
                self._info(),
                build_post_or_breakout_candles(final_close=10.28, final_volume=2500),
                2000,
                benchmark("BUY", "NASDAQ"),
            )

        self.assertEqual(result["final_reason"], "near_breakout_watch")
        self.assertTrue(result["metrics"]["near_breakout_watch"])

    def test_strong_near_breakout_can_be_promoted_to_candidate(self):
        env = {
            **self._base_env(),
            "PAPER_NEAR_BREAKOUT_PROMOTION_ENABLED": "true",
            "PAPER_NEAR_BREAKOUT_PROMOTION_PCT": "0.004",
            "PAPER_NEAR_BREAKOUT_PROMOTION_MIN_RELATIVE_VOLUME": "1.8",
            "PAPER_NEAR_BREAKOUT_PROMOTION_MIN_3_CANDLE_REL_VOLUME": "1.3",
        }
        candles = build_post_or_breakout_candles(final_close=10.285, final_volume=2500)
        candles[-1]["high"] = 10.45
        with patch.dict(os.environ, env, clear=False):
            with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 10, 0, tzinfo=NY_TZ)):
                result = evaluate_symbol(
                    "SoFi",
                    self._info(),
                    candles,
                    2000,
                    benchmark("BUY", "NASDAQ"),
                )

        self.assertEqual(result["decision"], "VALID")
        self.assertEqual(result["final_reason"], "near_breakout_promotion")
        self.assertTrue(result["metrics"]["near_breakout_promotion"])

    def test_default_near_breakout_promotion_allows_four_tenths_pct_distance(self):
        env = {
            **self._base_env(),
            "PAPER_NEAR_BREAKOUT_PROMOTION_ENABLED": "true",
            "PAPER_NEAR_BREAKOUT_PROMOTION_MIN_RELATIVE_VOLUME": "1.8",
            "PAPER_NEAR_BREAKOUT_PROMOTION_MIN_3_CANDLE_REL_VOLUME": "1.3",
        }
        candles = build_post_or_breakout_candles(final_close=10.265, final_volume=2500)
        candles[-1]["high"] = 10.45
        with patch.dict(os.environ, env, clear=False):
            with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 10, 0, tzinfo=NY_TZ)):
                result = evaluate_symbol(
                    "SoFi",
                    self._info(),
                    candles,
                    2000,
                    benchmark("BUY", "NASDAQ"),
                )

        self.assertEqual(result["decision"], "VALID")
        self.assertEqual(result["final_reason"], "near_breakout_promotion")
        self.assertTrue(result["metrics"]["near_breakout_promotion"])

    def test_three_candle_volume_pace_rejects_one_candle_fakeout(self):
        candles = build_post_or_breakout_candles(final_close=10.72, final_volume=1200)
        candles[-2]["volume"] = 100
        candles[-3]["volume"] = 100
        with patch.dict(os.environ, self._base_env(), clear=False):
            with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 10, 0, tzinfo=NY_TZ)):
                result = evaluate_symbol("SoFi", self._info(), candles, 2000, benchmark("BUY", "NASDAQ"))

        self.assertEqual(result["final_reason"], "three_candle_volume_pace_failed")
        self.assertFalse(result["checks"]["three_candle_volume_pace"])

    def test_relative_strength_opposed_rejects_buy(self):
        opposed_benchmark = benchmark("BUY", "NASDAQ")
        opposed_benchmark["NASDAQ_RETURN"] = 0.10
        with patch.dict(os.environ, self._base_env(), clear=False):
            with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 10, 0, tzinfo=NY_TZ)):
                result = evaluate_symbol(
                    "SoFi",
                    self._info(),
                    build_post_or_breakout_candles(final_close=10.72, final_volume=2500),
                    2000,
                    opposed_benchmark,
                )

        self.assertEqual(result["final_reason"], "relative_strength_opposed")
        self.assertFalse(result["checks"]["relative_strength_not_opposed"])

    def test_spread_too_wide_rejects_when_quote_available(self):
        with patch.dict(os.environ, self._base_env(), clear=False):
            with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 10, 0, tzinfo=NY_TZ)):
                result = evaluate_symbol(
                    "SoFi",
                    self._info(),
                    build_post_or_breakout_candles(final_close=10.72, final_volume=2500),
                    2000,
                    benchmark("BUY", "NASDAQ"),
                    quote={"bid": 10.60, "ask": 10.80},
                )

        self.assertEqual(result["final_reason"], "spread_too_wide")
        self.assertFalse(result["checks"]["spread_filter"])

    def test_failed_breakout_cooldown_rejects_symbol(self):
        with patch.dict(os.environ, self._base_env(), clear=False):
            with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 10, 0, tzinfo=NY_TZ)):
                result = evaluate_symbol(
                    "SoFi",
                    self._info(),
                    build_post_or_breakout_candles(final_close=10.72, final_volume=2500),
                    2000,
                    benchmark("BUY", "NASDAQ"),
                    failed_breakout_cooldown_symbols={"SOFI"},
                )

        self.assertEqual(result["final_reason"], "failed_breakout_cooldown_active")
        self.assertFalse(result["checks"]["failed_breakout_cooldown"])

    def test_failed_breakout_cooldown_is_direction_aware(self):
        with patch.dict(os.environ, self._base_env(), clear=False):
            with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 10, 0, tzinfo=NY_TZ)):
                result = evaluate_symbol(
                    "SoFi",
                    self._info(),
                    build_post_or_breakout_candles(final_close=10.72, final_volume=2500),
                    2000,
                    benchmark("BUY", "NASDAQ"),
                    failed_breakout_cooldown_symbols={"SOFI:SELL"},
                )

        self.assertNotEqual(result["final_reason"], "failed_breakout_cooldown_active")
        self.assertTrue(result["checks"]["failed_breakout_cooldown"])

    def test_run_scan_fetches_quotes_only_for_symbols_that_pass_candle_gates(self):
        instruments = {
            "Inside Range": {**self._info(), "symbol": "INSIDE", "market": "OTHER"},
            "Valid Breakout": {**self._info(), "symbol": "VALID", "market": "OTHER"},
        }

        def fake_intraday(symbol, **_kwargs):
            if symbol == "INSIDE":
                return build_post_or_breakout_candles(final_close=10.20, final_volume=2500)
            return build_post_or_breakout_candles(final_close=10.72, final_volume=2500)

        quote_calls = []

        def fake_quote(symbol, **_kwargs):
            quote_calls.append(symbol)
            return {"bid": 10.71, "ask": 10.73}

        with patch.dict(os.environ, self._base_env(), clear=False):
            with patch("analytics.trade_scan.get_mode_instruments", return_value=instruments):
                with patch("analytics.trade_scan.get_benchmark_instruments", return_value={}):
                    with patch("analytics.trade_scan.get_ny_now", return_value=datetime(2026, 4, 1, 10, 0, tzinfo=NY_TZ)):
                        trades, evaluations, _ok, fetch_fail, _benchmarks, _source = run_scan(
                            2000,
                            "low_price",
                            fetch_intraday_fn=fake_intraday,
                            fetch_quote_fn=fake_quote,
                        )

        self.assertEqual(quote_calls, ["VALID"])
        self.assertEqual(len(trades), 1)
        self.assertEqual(len(evaluations), 2)
        self.assertFalse([failure for failure in fetch_fail if "quote:" in failure])

    def test_fetch_instruments_stops_when_market_data_budget_is_exceeded(self):
        instruments = {
            "First": {**self._info(), "symbol": "FIRST"},
            "Second": {**self._info(), "symbol": "SECOND"},
        }
        fetch_calls = []

        def fake_intraday(symbol, **_kwargs):
            fetch_calls.append(symbol)
            return build_post_or_breakout_candles()

        with patch("analytics.trade_scan.time.monotonic", side_effect=[0.0, 0.2]):
            cache, fetch_ok, fetch_fail = fetch_instruments(
                instruments,
                fetch_intraday_fn=fake_intraday,
                time_budget_seconds=0.1,
            )

        self.assertEqual(cache, {})
        self.assertEqual(fetch_ok, [])
        self.assertEqual(fetch_calls, [])
        self.assertIn("Market data time budget exceeded", fetch_fail[0])


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
    def test_low_price_mode_uses_stricter_relative_volume_threshold(self):
        self.assertEqual(_volume_confirmation_threshold("low_price"), 1.3)

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

        with patch.dict(os.environ, {"PAPER_BREAKOUT_CLOSE_BUFFER_PCT": "0"}, clear=False):
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

        with patch.dict(os.environ, {"PAPER_BREAKOUT_CLOSE_BUFFER_PCT": "0"}, clear=False):
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
