import unittest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from orchestration.runtime_context import _validate_ibkr_intraday_freshness


NY_TZ = ZoneInfo("America/New_York")


class IbkrIntradayFreshnessTests(unittest.TestCase):
    def test_rejects_previous_day_bar_during_market_hours(self):
        candles = [{"datetime": "2026-05-04 15:59:00", "close": 27.26}]
        now_ny = datetime(2026, 5, 5, 9, 45, tzinfo=NY_TZ)

        with self.assertRaisesRegex(RuntimeError, "IBKR intraday candles are stale"):
            _validate_ibkr_intraday_freshness(candles, symbol="HIMS", interval="1min", now_ny=now_ny)

    def test_accepts_delayed_same_day_bar_within_threshold(self):
        candles = [{"datetime": "2026-05-05 10:40:00", "close": 109.35}]
        now_ny = datetime(2026, 5, 5, 10, 56, tzinfo=NY_TZ)

        result = _validate_ibkr_intraday_freshness(candles, symbol="INTC", interval="1min", now_ny=now_ny)

        self.assertIs(result, candles)

    def test_allows_previous_session_bar_outside_market_hours(self):
        candles = [{"datetime": "2026-05-05 15:59:00", "close": 109.35}]
        now_ny = datetime(2026, 5, 5, 18, 0, tzinfo=NY_TZ)

        result = _validate_ibkr_intraday_freshness(candles, symbol="INTC", interval="1min", now_ny=now_ny)

        self.assertIs(result, candles)

    def test_freshness_check_can_be_disabled(self):
        candles = [{"datetime": "2026-05-04 15:59:00", "close": 27.26}]
        now_ny = datetime(2026, 5, 5, 9, 45, tzinfo=NY_TZ)

        with patch.dict("os.environ", {"IBKR_INTRADAY_FRESHNESS_CHECK_ENABLED": "false"}, clear=False):
            result = _validate_ibkr_intraday_freshness(candles, symbol="HIMS", interval="1min", now_ny=now_ny)

        self.assertIs(result, candles)


if __name__ == "__main__":
    unittest.main()
