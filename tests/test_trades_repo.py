import unittest
from unittest import SkipTest
from unittest.mock import patch

try:
    from repositories.trades_repo import (
        get_dashboard_summary,
        get_latest_mode_ranking_order,
        refresh_mode_rankings,
        upsert_trade_lifecycle,
    )
except ModuleNotFoundError as exc:
    if exc.name == "psycopg":
        raise SkipTest("psycopg dependency is not available in this local unittest environment")
    raise


class TradeLifecyclePersistenceTests(unittest.TestCase):
    @patch("repositories.trades_repo.execute")
    @patch("repositories.trades_repo.fetch_one")
    def test_update_preserves_existing_mode_when_new_mode_is_blank(self, mock_fetch_one, mock_execute):
        mock_fetch_one.return_value = {"id": 42}

        upsert_trade_lifecycle(
            trade_key="trade-1",
            symbol="NVDA",
            mode="",
            status="CLOSED",
        )

        query = mock_execute.call_args.args[0]
        params = mock_execute.call_args.args[1]

        self.assertIn("mode = COALESCE(NULLIF(%(mode)s, ''), mode)", query)
        self.assertEqual(params["mode"], "")
        self.assertEqual(params["id"], 42)

    @patch("repositories.trades_repo.execute")
    @patch("repositories.trades_repo.fetch_one")
    def test_update_preserves_existing_broker_when_new_broker_is_blank(self, mock_fetch_one, mock_execute):
        mock_fetch_one.return_value = {"id": 42}

        upsert_trade_lifecycle(
            trade_key="trade-2",
            symbol="AAPL",
            broker="",
            status="OPEN",
        )

        query = mock_execute.call_args.args[0]
        params = mock_execute.call_args.args[1]

        self.assertIn("broker = COALESCE(NULLIF(%(broker)s, ''), broker)", query)
        self.assertEqual(params["broker"], "")
        self.assertEqual(params["id"], 42)

    @patch("repositories.trades_repo.execute")
    @patch("repositories.trades_repo.fetch_one")
    def test_trade_key_collision_inserts_disambiguated_row_when_identity_mismatches(self, mock_fetch_one, mock_execute):
        mock_fetch_one.side_effect = [
            {
                "id": 211,
                "trade_key": "76",
                "symbol": "TSLA",
                "broker": "IBKR",
                "order_id": "76",
                "parent_order_id": "76",
            },
            None,
        ]

        upsert_trade_lifecycle(
            trade_key="76",
            symbol="SOFI",
            broker="IBKR",
            order_id="76",
            parent_order_id="76",
            status="CLOSED",
            exit_reason="STALE_OPEN_RECONCILED",
        )

        query = mock_execute.call_args.args[0]
        params = mock_execute.call_args.args[1]

        self.assertIn("INSERT INTO trade_lifecycles", query)
        self.assertEqual(params["trade_key"], "IBKR:SOFI:76")
        self.assertEqual(params["symbol"], "SOFI")

    @patch("repositories.trades_repo.execute")
    @patch("repositories.trades_repo.fetch_one")
    def test_trade_key_collision_updates_existing_disambiguated_row_when_present(self, mock_fetch_one, mock_execute):
        mock_fetch_one.side_effect = [
            {
                "id": 211,
                "trade_key": "76",
                "symbol": "TSLA",
                "broker": "IBKR",
                "order_id": "76",
                "parent_order_id": "76",
            },
            {
                "id": 312,
                "trade_key": "IBKR:SOFI:76",
                "symbol": "SOFI",
                "broker": "IBKR",
                "order_id": "76",
                "parent_order_id": "76",
            },
        ]

        upsert_trade_lifecycle(
            trade_key="76",
            symbol="SOFI",
            broker="IBKR",
            order_id="76",
            parent_order_id="76",
            status="CLOSED",
        )

        query = mock_execute.call_args.args[0]
        params = mock_execute.call_args.args[1]

        self.assertIn("UPDATE trade_lifecycles", query)
        self.assertEqual(params["id"], 312)
        self.assertEqual(params["trade_key"], "IBKR:SOFI:76")


class DashboardSummaryDateScopeTests(unittest.TestCase):
    @patch("repositories.trades_repo.get_equity_curve")
    @patch("repositories.trades_repo.get_hourly_outcome_quality")
    @patch("repositories.trades_repo.get_hourly_performance")
    @patch("repositories.trades_repo.get_external_exit_summary")
    @patch("repositories.trades_repo.get_exit_reason_breakdown")
    @patch("repositories.trades_repo.get_mode_performance")
    @patch("repositories.trades_repo.get_symbol_performance")
    @patch("repositories.trades_repo.get_trade_lifecycle_summary_for_date")
    def test_dashboard_summary_passes_target_date_to_all_sections(
        self,
        mock_base_summary,
        mock_symbols,
        mock_modes,
        mock_exit_reasons,
        mock_external_exit,
        mock_hourly_perf,
        mock_hourly_quality,
        mock_equity_curve,
    ):
        target_date = "2026-04-09"
        mock_base_summary.return_value = {"date": target_date}
        mock_symbols.return_value = []
        mock_modes.return_value = []
        mock_exit_reasons.return_value = []
        mock_external_exit.return_value = None
        mock_hourly_perf.return_value = []
        mock_hourly_quality.return_value = []
        mock_equity_curve.return_value = []

        payload = get_dashboard_summary(target_date=target_date)

        self.assertEqual(payload["summary"]["date"], target_date)
        mock_symbols.assert_called_once_with(limit=10, target_date=target_date)
        mock_modes.assert_called_once_with(limit=10, target_date=target_date)
        mock_exit_reasons.assert_called_once_with(limit=20, target_date=target_date)
        mock_external_exit.assert_called_once_with(target_date=target_date)
        mock_hourly_perf.assert_called_once_with(limit=24, target_date=target_date)
        self.assertEqual(mock_hourly_quality.call_count, 2)
        self.assertEqual(mock_equity_curve.call_args.kwargs, {"limit": 5000, "target_date": target_date})


class ModeRankingTests(unittest.TestCase):
    @patch("repositories.trades_repo.fetch_all")
    def test_get_rolling_mode_performance_treats_blank_broker_as_ibkr(self, mock_fetch_all):
        from repositories.trades_repo import get_rolling_mode_performance

        mock_fetch_all.return_value = []

        get_rolling_mode_performance(broker="IBKR", window_days=5, as_of_date="2026-04-10", min_closed_trade_count=2)

        query = mock_fetch_all.call_args.args[0]
        self.assertIn("COALESCE(NULLIF(broker, ''), 'IBKR')", query)

    @patch("repositories.trades_repo.execute")
    @patch("repositories.trades_repo.get_rolling_mode_performance")
    def test_refresh_mode_rankings_appends_missing_expected_modes(self, mock_rolling, mock_execute):
        mock_rolling.return_value = [
            {"mode": "core_two", "trade_count": 4, "closed_trade_count": 4, "winning_trade_count": 3, "losing_trade_count": 1, "realized_pnl_total": 120.5, "win_rate_percent": 75.0},
            {"mode": "core_one", "trade_count": 5, "closed_trade_count": 5, "winning_trade_count": 3, "losing_trade_count": 2, "realized_pnl_total": 80.25, "win_rate_percent": 60.0},
        ]

        payload = refresh_mode_rankings(
            broker="IBKR",
            expected_modes=["core_one", "core_two", "core_three"],
            window_days=5,
            as_of_date="2026-04-10",
            min_closed_trade_count=2,
        )

        self.assertEqual(payload["mode_order"], ["core_two", "core_one", "core_three"])
        self.assertEqual(payload["ranked_mode_count"], 2)
        self.assertEqual(payload["total_mode_count"], 3)
        self.assertEqual(mock_execute.call_count, 4)

    @patch("repositories.trades_repo.get_latest_mode_ranking_rows")
    def test_get_latest_mode_ranking_order_preserves_expected_mode_fallback(self, mock_rows):
        mock_rows.return_value = [
            {"mode": "core_two", "rank": 1},
            {"mode": "core_one", "rank": 2},
        ]

        ordered_modes = get_latest_mode_ranking_order(
            broker="IBKR",
            expected_modes=["core_one", "core_two", "core_three"],
            window_days=5,
        )

        self.assertEqual(ordered_modes, ["core_two", "core_one", "core_three"])


if __name__ == "__main__":
    unittest.main()
