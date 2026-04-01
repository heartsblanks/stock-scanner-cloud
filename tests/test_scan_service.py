import unittest
from types import SimpleNamespace
from unittest.mock import patch

from services.scan_service import (
    _apply_hard_notional_cap,
    _apply_confidence_loss_sizing,
    _apply_minimum_viable_position_sizing,
    _get_live_alpaca_account_equity,
)


class ScanServiceSizingTests(unittest.TestCase):
    def test_minimum_viable_position_sizing_compresses_slots_for_expensive_symbol(self):
        metrics = {
            "entry": 6000.0,
            "remaining_allocatable_capital": 50000.0,
            "remaining_slots": 10,
            "shares": 0,
            "risk_per_share": 100.0,
            "cash_affordable_shares": 0,
        }

        _apply_minimum_viable_position_sizing(metrics)

        self.assertEqual(metrics["effective_remaining_slots"], 8)
        self.assertEqual(metrics["shares"], 1)
        self.assertEqual(metrics["notional_capped_shares"], 1)
        self.assertEqual(metrics["cash_affordable_shares"], 1)
        self.assertAlmostEqual(metrics["per_trade_notional"], 6250.0, places=4)
        self.assertAlmostEqual(metrics["actual_position_cost"], 6000.0, places=4)

    def test_confidence_loss_sizing_reduces_notional_and_share_count(self):
        metrics = {
            "per_trade_notional": 5000.0,
            "entry": 250.0,
            "risk_per_share": 5.0,
        }

        _apply_confidence_loss_sizing(
            metrics,
            confidence_multiplier=1.0,
            loss_multiplier=0.5,
            final_multiplier=0.5,
        )

        self.assertEqual(metrics["confidence_multiplier"], 1.0)
        self.assertEqual(metrics["loss_multiplier"], 0.5)
        self.assertEqual(metrics["final_multiplier"], 0.5)
        self.assertAlmostEqual(metrics["adjusted_per_trade_notional"], 2500.0, places=4)
        self.assertEqual(metrics["shares"], 10)
        self.assertAlmostEqual(metrics["actual_position_cost"], 2500.0, places=4)
        self.assertAlmostEqual(metrics["actual_risk"], 50.0, places=4)

    def test_hard_notional_cap_prevents_confidence_expansion_above_remaining_cap_and_env_cap(self):
        metrics = {
            "entry": 48.2,
            "risk_per_share": 0.7455,
            "per_trade_notional": 101701.5253,
            "adjusted_per_trade_notional": 101701.5253,
            "remaining_allocatable_capital": 89736.64,
        }

        with patch.dict("os.environ", {"ALPACA_MAX_NOTIONAL": "10000"}, clear=False):
            _apply_hard_notional_cap(metrics)

        self.assertAlmostEqual(metrics["hard_max_notional"], 10000.0, places=4)
        self.assertEqual(metrics["shares"], 207)
        self.assertAlmostEqual(metrics["per_trade_notional"], 9977.4, places=4)
        self.assertAlmostEqual(metrics["adjusted_per_trade_notional"], 9977.4, places=4)
        self.assertAlmostEqual(metrics["actual_position_cost"], 9977.4, places=4)

    def test_live_account_equity_reads_from_alpaca_package_after_repo_move(self):
        original_import = __import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "alpaca.paper":
                return SimpleNamespace(get_account=lambda: {"equity": "12345.67"})
            return original_import(name, globals, locals, fromlist, level)

        with patch("builtins.__import__", side_effect=fake_import):
            equity = _get_live_alpaca_account_equity({})

        self.assertAlmostEqual(equity, 12345.67, places=2)


if __name__ == "__main__":
    unittest.main()
