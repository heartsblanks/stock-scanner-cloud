import unittest
from unittest import SkipTest
from unittest.mock import patch

try:
    from repositories.trades_repo import (
        get_dashboard_summary,
        get_stale_ibkr_closed_trade_lifecycles,
        get_latest_mode_ranking_order,
        refresh_symbol_rankings,
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
        mock_symbols.assert_called_once_with(limit=10, target_date=target_date, broker=None)
        mock_modes.assert_called_once_with(limit=10, target_date=target_date, broker=None)
        mock_exit_reasons.assert_called_once_with(limit=20, target_date=target_date, broker=None)
        mock_external_exit.assert_called_once_with(target_date=target_date, broker=None)
        mock_hourly_perf.assert_called_once_with(limit=24, target_date=target_date, broker=None)
        self.assertEqual(mock_hourly_quality.call_count, 2)
        self.assertEqual(mock_equity_curve.call_args.kwargs, {"limit": 5000, "target_date": target_date, "broker": None})


class ModeRankingTests(unittest.TestCase):
    @patch("repositories.trades_repo.fetch_all")
    def test_get_rolling_mode_performance_treats_blank_broker_as_ibkr(self, mock_fetch_all):
        from repositories.trades_repo import get_rolling_mode_performance

        mock_fetch_all.return_value = []

        get_rolling_mode_performance(broker="IBKR", window_days=5, as_of_date="2026-04-10", min_closed_trade_count=2)

        query = mock_fetch_all.call_args.args[0]
        self.assertIn("COALESCE(NULLIF(broker, ''), 'IBKR')", query)
        self.assertIn("TIME_STOP_CLOSE_REQUESTED_PENDING_FILL_SYNC", query)

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


class SymbolRankingTests(unittest.TestCase):
    @patch("repositories.trades_repo.execute")
    @patch("repositories.trades_repo.ensure_symbol_rankings_schema")
    @patch("repositories.trades_repo.get_rolling_symbol_performance")
    def test_refresh_symbol_rankings_scores_and_demotes_weak_symbols(
        self,
        mock_rolling,
        mock_ensure_schema,
        mock_execute,
    ):
        mock_rolling.return_value = [
            {
                "mode": "core_two",
                "symbol": "CSCO",
                "priority": 8,
                "trade_count": 2,
                "closed_trade_count": 2,
                "winning_trade_count": 0,
                "losing_trade_count": 2,
                "realized_pnl_total": -4.0,
                "average_realized_pnl": -2.0,
                "win_rate_percent": 0.0,
                "candidate_count": 3,
                "placed_count": 2,
                "skipped_count": 0,
                "rejected_count": 1,
            },
            {
                "mode": "core_two",
                "symbol": "TMUS",
                "priority": 8,
                "trade_count": 2,
                "closed_trade_count": 2,
                "winning_trade_count": 2,
                "losing_trade_count": 0,
                "realized_pnl_total": 5.0,
                "average_realized_pnl": 2.5,
                "win_rate_percent": 100.0,
                "candidate_count": 2,
                "placed_count": 2,
                "skipped_count": 0,
                "rejected_count": 0,
            },
        ]

        payload = refresh_symbol_rankings(
            broker="IBKR",
            expected_modes=["core_two"],
            window_days=5,
            as_of_date="2026-05-06",
            min_closed_trade_count=2,
        )

        self.assertEqual(payload["symbol_count"], 2)
        self.assertEqual(payload["demoted_count"], 1)
        insert_params = [call.args[1] for call in mock_execute.call_args_list[1:]]
        tmus = next(params for params in insert_params if params["symbol"] == "TMUS")
        csco = next(params for params in insert_params if params["symbol"] == "CSCO")
        self.assertEqual(tmus["rank"], 1)
        self.assertFalse(tmus["demoted"])
        self.assertTrue(csco["demoted"])
        self.assertEqual(csco["demotion_reason"], "negative_pnl_low_win_rate")

    @patch("repositories.trades_repo.fetch_all")
    @patch("repositories.trades_repo.ensure_symbol_rankings_schema")
    def test_get_latest_symbol_ranking_rows_filters_by_mode(self, mock_ensure_schema, mock_fetch_all):
        from repositories.trades_repo import get_latest_symbol_ranking_rows

        mock_fetch_all.return_value = []

        get_latest_symbol_ranking_rows(broker="IBKR", window_days=5, mode="core_two")

        query = mock_fetch_all.call_args.args[0]
        params = mock_fetch_all.call_args.args[1]
        self.assertIn("AND mode = %(mode)s", query)
        self.assertEqual(params["mode"], "core_two")
        self.assertEqual(params["broker"], "IBKR")


class StaleLifecycleSelectionTests(unittest.TestCase):
    @patch("repositories.trades_repo.fetch_all")
    def test_stale_selector_includes_guarded_manual_close_rows(self, mock_fetch_all):
        mock_fetch_all.return_value = []

        get_stale_ibkr_closed_trade_lifecycles(target_date="2026-04-17", limit=25)

        query = mock_fetch_all.call_args.args[0]
        params = mock_fetch_all.call_args.args[1]
        self.assertIn("UPPER(COALESCE(exit_reason, '')) = 'MANUAL_CLOSE'", query)
        self.assertIn("COALESCE(exit_price, 0) = COALESCE(entry_price, 0)", query)
        self.assertIn("COALESCE(exit_order_id, '') = COALESCE(parent_order_id, '')", query)
        self.assertEqual(params["target_date"], "2026-04-17")
        self.assertEqual(params["limit"], 25)


if __name__ == "__main__":
    unittest.main()
