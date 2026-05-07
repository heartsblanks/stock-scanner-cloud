import unittest
from unittest.mock import patch

from services.symbol_eligibility_service import refresh_symbol_eligibility_for_date


class SymbolEligibilityRankingTests(unittest.TestCase):
    def _fetch_candles(self, symbol, **kwargs):
        return [{"close": 20.0}]

    def _fetch_mixed_price_candles(self, symbol, **kwargs):
        prices = {
            "HIGH": 600.0,
        }
        return [{"close": prices.get(symbol, 20.0)}]

    @patch("services.symbol_eligibility_service.replace_symbol_session_eligibility_rows")
    @patch("services.symbol_eligibility_service.get_latest_symbol_ranking_rows")
    @patch("services.symbol_eligibility_service.get_instrument_groups")
    @patch("services.symbol_eligibility_service.sync_quality_candidate_instruments")
    def test_refresh_caps_price_eligible_symbols_by_ranking_and_demotion(
        self,
        mock_sync_catalog,
        mock_groups,
        mock_rankings,
        mock_replace,
    ):
        mock_sync_catalog.return_value = {"ok": True, "synced_count": 12}
        mock_groups.return_value = {
            "core_two": {
                "Cisco": {"symbol": "CSCO", "priority": 8, "currency": "USD"},
                "T-Mobile US": {"symbol": "TMUS", "priority": 8, "currency": "USD"},
                "PepsiCo": {"symbol": "PEP", "priority": 8, "currency": "USD"},
                "PayPal": {"symbol": "PYPL", "priority": 7, "currency": "USD"},
            }
        }
        mock_rankings.return_value = [
            {"symbol": "TMUS", "rank": 1, "score": 110, "demoted": False},
            {"symbol": "CSCO", "rank": 2, "score": 90, "demoted": True},
            {"symbol": "PEP", "rank": 3, "score": 85, "demoted": False},
            {"symbol": "PYPL", "rank": 4, "score": 70, "demoted": False},
        ]

        with patch.dict("os.environ", {"SYMBOL_ELIGIBILITY_MAX_SYMBOLS_PER_MODE": "2"}, clear=False):
            result = refresh_symbol_eligibility_for_date(
                target_session_date="2026-05-07",
                fetch_intraday_fn=self._fetch_candles,
                modes=["core_two"],
            )

        rows = mock_replace.call_args.kwargs["rows"]
        by_symbol = {row["symbol"]: row for row in rows}
        self.assertTrue(by_symbol["TMUS"]["eligible"])
        self.assertTrue(by_symbol["PEP"]["eligible"])
        self.assertFalse(by_symbol["CSCO"]["eligible"])
        self.assertEqual(by_symbol["CSCO"]["ineligible_reason"], "symbol_rank_demoted")
        self.assertFalse(by_symbol["PYPL"]["eligible"])
        self.assertEqual(by_symbol["PYPL"]["ineligible_reason"], "ranked_below_live_allowlist")
        self.assertEqual(result["eligible_count"], 2)
        self.assertEqual(result["catalog_sync"]["synced_count"], 12)

    @patch("services.symbol_eligibility_service.replace_symbol_session_eligibility_rows")
    @patch("services.symbol_eligibility_service.get_latest_symbol_ranking_rows")
    @patch("services.symbol_eligibility_service.get_instrument_groups")
    @patch("services.symbol_eligibility_service.sync_quality_candidate_instruments")
    def test_refresh_falls_back_to_priority_when_rankings_are_empty(
        self,
        mock_sync_catalog,
        mock_groups,
        mock_rankings,
        mock_replace,
    ):
        mock_sync_catalog.return_value = {"ok": True, "synced_count": 12}
        mock_groups.return_value = {
            "core_three": {
                "High Priority": {"symbol": "HP", "priority": 10, "currency": "USD"},
                "Mid Priority": {"symbol": "MP", "priority": 8, "currency": "USD"},
                "Low Priority": {"symbol": "LP", "priority": 6, "currency": "USD"},
            }
        }
        mock_rankings.return_value = []

        with patch.dict("os.environ", {"SYMBOL_ELIGIBILITY_MAX_SYMBOLS_PER_MODE": "2"}, clear=False):
            refresh_symbol_eligibility_for_date(
                target_session_date="2026-05-07",
                fetch_intraday_fn=self._fetch_candles,
                modes=["core_three"],
            )

        rows = mock_replace.call_args.kwargs["rows"]
        by_symbol = {row["symbol"]: row for row in rows}
        self.assertTrue(by_symbol["HP"]["eligible"])
        self.assertTrue(by_symbol["MP"]["eligible"])
        self.assertFalse(by_symbol["LP"]["eligible"])
        self.assertEqual(by_symbol["LP"]["ineligible_reason"], "ranked_below_live_allowlist")

    @patch("services.symbol_eligibility_service.replace_symbol_session_eligibility_rows")
    @patch("services.symbol_eligibility_service.get_latest_symbol_ranking_rows")
    @patch("services.symbol_eligibility_service.get_instrument_groups")
    @patch("services.symbol_eligibility_service.sync_quality_candidate_instruments")
    def test_refresh_disables_symbol_cap_with_zero_but_keeps_demotions_and_price_cap(
        self,
        mock_sync_catalog,
        mock_groups,
        mock_rankings,
        mock_replace,
    ):
        mock_sync_catalog.return_value = {"ok": True, "synced_count": 12}
        mock_groups.return_value = {
            "core_two": {
                "Alpha": {"symbol": "AAA", "priority": 8, "currency": "USD"},
                "Beta": {"symbol": "BBB", "priority": 8, "currency": "USD"},
                "Demoted": {"symbol": "DEM", "priority": 8, "currency": "USD"},
                "High Price": {"symbol": "HIGH", "priority": 8, "currency": "USD"},
            }
        }
        mock_rankings.return_value = [
            {"symbol": "AAA", "rank": 1, "score": 100, "demoted": False},
            {"symbol": "BBB", "rank": 2, "score": 90, "demoted": False},
            {"symbol": "DEM", "rank": 3, "score": 80, "demoted": True},
            {"symbol": "HIGH", "rank": 4, "score": 70, "demoted": False},
        ]

        with patch.dict(
            "os.environ",
            {
                "SYMBOL_ELIGIBILITY_MAX_SYMBOLS_PER_MODE": "0",
                "SYMBOL_ELIGIBILITY_MAX_NOTIONAL": "500",
            },
            clear=False,
        ):
            result = refresh_symbol_eligibility_for_date(
                target_session_date="2026-05-07",
                fetch_intraday_fn=self._fetch_mixed_price_candles,
                modes=["core_two"],
            )

        rows = mock_replace.call_args.kwargs["rows"]
        by_symbol = {row["symbol"]: row for row in rows}
        self.assertTrue(by_symbol["AAA"]["eligible"])
        self.assertTrue(by_symbol["BBB"]["eligible"])
        self.assertFalse(by_symbol["DEM"]["eligible"])
        self.assertEqual(by_symbol["DEM"]["ineligible_reason"], "symbol_rank_demoted")
        self.assertFalse(by_symbol["HIGH"]["eligible"])
        self.assertEqual(by_symbol["HIGH"]["ineligible_reason"], "price_above_cap_500.00")
        self.assertEqual(result["eligible_count"], 2)
        self.assertTrue(result["modes"][0]["ranking_filter"]["cap_disabled"])
        self.assertEqual(result["modes"][0]["ranking_filter"]["ranked_below_count"], 0)

    @patch("services.symbol_eligibility_service.replace_symbol_session_eligibility_rows")
    @patch("services.symbol_eligibility_service.get_latest_symbol_ranking_rows")
    @patch("services.symbol_eligibility_service.get_instrument_groups")
    @patch("services.symbol_eligibility_service.sync_quality_candidate_instruments")
    def test_refresh_disables_symbol_cap_with_empty_value(
        self,
        mock_sync_catalog,
        mock_groups,
        mock_rankings,
        mock_replace,
    ):
        mock_sync_catalog.return_value = {"ok": True, "synced_count": 12}
        mock_groups.return_value = {
            "core_three": {
                "High Priority": {"symbol": "HP", "priority": 10, "currency": "USD"},
                "Mid Priority": {"symbol": "MP", "priority": 8, "currency": "USD"},
                "Low Priority": {"symbol": "LP", "priority": 6, "currency": "USD"},
            }
        }
        mock_rankings.return_value = []

        with patch.dict("os.environ", {"SYMBOL_ELIGIBILITY_MAX_SYMBOLS_PER_MODE": ""}, clear=False):
            result = refresh_symbol_eligibility_for_date(
                target_session_date="2026-05-07",
                fetch_intraday_fn=self._fetch_candles,
                modes=["core_three"],
            )

        rows = mock_replace.call_args.kwargs["rows"]
        by_symbol = {row["symbol"]: row for row in rows}
        self.assertTrue(by_symbol["HP"]["eligible"])
        self.assertTrue(by_symbol["MP"]["eligible"])
        self.assertTrue(by_symbol["LP"]["eligible"])
        self.assertEqual(result["eligible_count"], 3)
        self.assertTrue(result["modes"][0]["ranking_filter"]["cap_disabled"])


if __name__ == "__main__":
    unittest.main()
