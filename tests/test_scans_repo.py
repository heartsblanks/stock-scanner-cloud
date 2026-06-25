import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from repositories.scans_repo import get_recent_failed_breakout_symbols, get_recent_watch_ready_symbols


class ScanRepoTests(unittest.TestCase):
    @patch("repositories.scans_repo.ensure_scan_gate_observations_schema")
    @patch("repositories.scans_repo.fetch_all")
    def test_recent_watch_ready_symbols_returns_distinct_prioritized_symbols(self, mock_fetch_all, _mock_schema):
        mock_fetch_all.return_value = [
            {"symbol": "SOFI"},
            {"symbol": "HIMS"},
        ]

        rows = get_recent_watch_ready_symbols(
            mode="low_price",
            broker="IBKR",
            now_utc=datetime(2026, 6, 25, 15, 0, tzinfo=timezone.utc),
            max_age_minutes=20,
            limit=10,
        )

        self.assertEqual(rows, ["SOFI", "HIMS"])
        query = mock_fetch_all.call_args.args[0]
        self.assertIn("final_reason = 'watch_ready_near_breakout'", query)

    @patch("repositories.scans_repo.ensure_scan_gate_observations_schema")
    @patch("repositories.scans_repo.fetch_all")
    def test_failed_breakout_cooldown_does_not_include_watch_ready_reason(self, mock_fetch_all, _mock_schema):
        mock_fetch_all.return_value = []

        get_recent_failed_breakout_symbols(
            mode="low_price",
            broker="IBKR",
            now_utc=datetime(2026, 6, 25, 15, 0, tzinfo=timezone.utc),
            cooldown_minutes=45,
        )

        query = mock_fetch_all.call_args.args[0]
        self.assertNotIn("near_breakout_watch", query)
        self.assertNotIn("watch_ready_near_breakout", query)


if __name__ == "__main__":
    unittest.main()
