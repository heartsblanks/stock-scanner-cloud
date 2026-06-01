import unittest
from unittest.mock import MagicMock, patch

from analytics.instruments import (
    _DEFAULT_INSTRUMENT_GROUPS,
    MANDATORY_MODES,
    _normalize_primary_exchange,
    _rows_to_groups,
    get_instrument_groups,
    sync_quality_candidate_instruments,
)


class InstrumentRegistryTests(unittest.TestCase):
    def test_rows_to_groups_treats_none_literals_as_missing_market_metadata(self):
        rows = []
        for mode_index, mode in enumerate(
            [
                "low_price",
                "primary",
                "secondary",
                "third",
                "fourth",
                "fifth",
                "sixth",
                "us_test",
                "core_one",
                "core_two",
                "core_three",
            ],
            start=1,
        ):
            rows.append(
                {
                    "mode": mode,
                    "display_name": f"{mode.title()} Name",
                    "symbol": f"T{mode_index}",
                    "instrument_type": "stock",
                    "priority": 10,
                    "market": "NASDAQ",
                    "exchange": "NONE" if mode == "core_one" else None,
                    "primary_exchange": "NONE" if mode == "core_one" else None,
                    "currency": "NONE" if mode == "core_one" else None,
                }
            )

        instrument_groups = _rows_to_groups(rows)
        info = instrument_groups["core_one"]["Core_One Name"]
        self.assertIsNone(info["exchange"])
        self.assertIsNone(info["primary_exchange"])
        self.assertIsNone(info["currency"])

    def test_normalize_primary_exchange_maps_nms_to_nasdaq(self):
        self.assertEqual(_normalize_primary_exchange("NMS"), "NASDAQ")
        self.assertEqual(_normalize_primary_exchange("nasdaq"), "NASDAQ")
        self.assertIsNone(_normalize_primary_exchange("NONE"))

    def test_rows_to_groups_allows_legacy_modes_to_be_empty(self):
        rows = []
        for mode_index, mode in enumerate(MANDATORY_MODES, start=1):
            rows.append(
                {
                    "mode": mode,
                    "display_name": f"{mode.title()} Name",
                    "symbol": f"M{mode_index}",
                    "instrument_type": "stock",
                    "priority": 10,
                    "market": "NASDAQ",
                    "exchange": None,
                    "primary_exchange": None,
                    "currency": None,
                }
            )

        instrument_groups = _rows_to_groups(rows)

        self.assertEqual(instrument_groups["us_test"], {})
        self.assertEqual(instrument_groups["core_one"], {})
        for mode in MANDATORY_MODES:
            self.assertTrue(instrument_groups[mode])

    @patch("analytics.instruments.get_db_cursor")
    def test_sync_quality_candidates_upserts_by_symbol_and_moves_us_test_rows(self, mock_get_db_cursor):
        mock_cursor = MagicMock()
        mock_get_db_cursor.return_value.__enter__.return_value = mock_cursor

        result = sync_quality_candidate_instruments()

        self.assertTrue(result["ok"])
        self.assertIn("SOFI", result["symbols"])
        self.assertIn("HIMS", result["symbols"])
        self.assertIn("PLUG", result["symbols"])
        self.assertIn("KEEL", result["symbols"])
        self.assertIn("XPEV", result["symbols"])
        self.assertEqual(result["modes"], ["low_price"])
        self.assertTrue(result["legacy_modes_deactivated"])
        deactivate_query = mock_cursor.execute.call_args.args[0]
        self.assertIn("WHERE mode <> 'low_price'", deactivate_query)
        query = mock_cursor.executemany.call_args.args[0]
        rows = mock_cursor.executemany.call_args.args[1]
        sofi = next(row for row in rows if row["symbol"] == "SOFI")
        hims = next(row for row in rows if row["symbol"] == "HIMS")
        plug = next(row for row in rows if row["symbol"] == "PLUG")
        keel = next(row for row in rows if row["symbol"] == "KEEL")
        self.assertIn("ON CONFLICT (symbol)", query)
        self.assertIn("mode = EXCLUDED.mode", query)
        self.assertEqual(sofi["mode"], "low_price")
        self.assertEqual(hims["market"], "NYSE")
        self.assertEqual(plug["mode"], "low_price")
        self.assertEqual(plug["priority"], 7)
        self.assertEqual(keel["mode"], "low_price")
        self.assertEqual(keel["priority"], 7)

    def test_instrument_symbols_are_unique_within_each_mode(self):
        instrument_groups = _DEFAULT_INSTRUMENT_GROUPS

        for mode, instruments in instrument_groups.items():
            self.assertTrue(instruments, f"{mode} should not be empty")
            seen_symbols = set()
            for display_name, info in instruments.items():
                symbol = info["symbol"]
                self.assertNotIn(symbol, seen_symbols, f"duplicate symbol found: {symbol}")
                self.assertTrue(display_name)
                seen_symbols.add(symbol)

    def test_mandatory_instrument_modes_have_seed_symbols(self):
        instrument_groups = _DEFAULT_INSTRUMENT_GROUPS
        for mode, instruments in instrument_groups.items():
            if mode in MANDATORY_MODES:
                self.assertTrue(instruments, f"{mode} should not be empty")


if __name__ == "__main__":
    unittest.main()
