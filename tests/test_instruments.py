import unittest

from analytics.instruments import _normalize_primary_exchange, _rows_to_groups, get_instrument_groups


class InstrumentRegistryTests(unittest.TestCase):
    def test_rows_to_groups_treats_none_literals_as_missing_market_metadata(self):
        rows = []
        for mode_index, mode in enumerate(
            [
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

    def test_instrument_symbols_are_unique_across_modes(self):
        instrument_groups = get_instrument_groups()
        seen_symbols = set()

        for mode, instruments in instrument_groups.items():
            self.assertTrue(instruments, f"{mode} should not be empty")
            for display_name, info in instruments.items():
                symbol = info["symbol"]
                self.assertNotIn(symbol, seen_symbols, f"duplicate symbol found: {symbol}")
                self.assertTrue(display_name)
                seen_symbols.add(symbol)

    def test_each_instrument_mode_stays_within_six_symbol_target(self):
        instrument_groups = get_instrument_groups()
        for mode, instruments in instrument_groups.items():
            if mode.startswith("core_"):
                self.assertLessEqual(len(instruments), 6, f"{mode} should stay at six or fewer symbols")
            else:
                self.assertLessEqual(len(instruments), 6, f"{mode} should stay at six or fewer symbols")


if __name__ == "__main__":
    unittest.main()
