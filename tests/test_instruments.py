import unittest

from analytics.instruments import INSTRUMENT_GROUPS


class InstrumentRegistryTests(unittest.TestCase):
    def test_instrument_symbols_are_unique_across_modes(self):
        seen_symbols = set()

        for mode, instruments in INSTRUMENT_GROUPS.items():
            self.assertTrue(instruments, f"{mode} should not be empty")
            for display_name, info in instruments.items():
                symbol = info["symbol"]
                self.assertNotIn(symbol, seen_symbols, f"duplicate symbol found: {symbol}")
                self.assertTrue(display_name)
                seen_symbols.add(symbol)

    def test_each_instrument_mode_stays_within_six_symbol_target(self):
        for mode, instruments in INSTRUMENT_GROUPS.items():
            if mode.startswith("core_"):
                self.assertLessEqual(len(instruments), 6, f"{mode} should stay at six or fewer symbols")
            else:
                self.assertLessEqual(len(instruments), 6, f"{mode} should stay at six or fewer symbols")


if __name__ == "__main__":
    unittest.main()
