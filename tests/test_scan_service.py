import unittest
from types import SimpleNamespace
from unittest.mock import patch

from services.scan_service import (
    _apply_hard_notional_cap,
    _apply_low_price_notional_cap,
    _apply_confidence_loss_sizing,
    _apply_minimum_viable_position_sizing,
    evaluate_symbol_performance_gate,
    execute_full_scan,
    _get_live_alpaca_account_equity,
)


class ScanServiceSizingTests(unittest.TestCase):
    def test_symbol_performance_gate_blocks_consistently_losing_symbol(self):
        blocked, reason, details = evaluate_symbol_performance_gate(
            [
                {"realized_pnl_percent": -0.8},
                {"realized_pnl_percent": -0.6},
                {"realized_pnl_percent": -1.1},
            ]
        )

        self.assertTrue(blocked)
        self.assertIn("symbol_performance_blocked", reason)
        self.assertEqual(details["loss_count"], 3)
        self.assertEqual(details["win_count"], 0)

    def test_symbol_performance_gate_allows_mixed_recent_symbol_results(self):
        blocked, reason, details = evaluate_symbol_performance_gate(
            [
                {"realized_pnl_percent": -0.8},
                {"realized_pnl_percent": 0.5},
                {"realized_pnl_percent": -0.4},
            ]
        )

        self.assertFalse(blocked)
        self.assertEqual(reason, "")
        self.assertEqual(details["win_count"], 1)

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

    def test_low_price_notional_cap_tightens_exposure_for_cheap_symbols(self):
        metrics = {
            "entry": 9.75,
            "risk_per_share": 0.1,
            "per_trade_notional": 10000.0,
            "adjusted_per_trade_notional": 10000.0,
            "shares": 1025,
        }

        _apply_low_price_notional_cap(metrics)

        self.assertLessEqual(metrics["per_trade_notional"], 5000.0)
        self.assertEqual(metrics["shares"], int(metrics["per_trade_notional"] / 9.75))

    def test_live_account_equity_reads_from_alpaca_package_after_repo_move(self):
        original_import = __import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "alpaca.paper":
                return SimpleNamespace(get_account=lambda: {"equity": "12345.67"})
            return original_import(name, globals, locals, fromlist, level)

        with patch("builtins.__import__", side_effect=fake_import):
            equity = _get_live_alpaca_account_equity({})

        self.assertAlmostEqual(equity, 12345.67, places=2)

    def test_execute_full_scan_defaults_attempt_broker_to_active_broker(self):
        inserted_attempts = []

        def fake_run_scan(account_size, mode, current_open_positions=0, current_open_exposure=0.0):
            return (
                [],
                [
                    {
                        "decision": "INVALID",
                        "final_reason": "Opening range not available.",
                        "metrics": {
                            "symbol": "CLOV",
                            "direction": "BUY",
                        },
                    }
                ],
                True,
                0,
                {"SP500": "NEUTRAL", "NASDAQ": "NEUTRAL"},
                f"IBKR_{mode.upper()}",
            )

        result = execute_full_scan(
            {"mode": "third", "paper_trade": True, "scan_source": "SCHEDULED"},
            market_time_check=lambda: (True, "Market timing OK."),
            build_scan_id=lambda timestamp_utc, mode: f"{mode}-scan",
            market_phase_from_timestamp=lambda timestamp_utc: "MIDDAY",
            append_signal_log=lambda row: None,
            safe_insert_paper_trade_attempt=lambda **kwargs: inserted_attempts.append(kwargs),
            safe_insert_scan_run=lambda **kwargs: None,
            parse_iso_utc=lambda ts: ts,
            run_scan=fake_run_scan,
            trade_to_dict=lambda trade: trade,
            debug_to_dict=lambda evaluation: evaluation,
            paper_candidate_from_evaluation=lambda evaluation: None,
            evaluate_symbol=lambda *args, **kwargs: None,
            get_latest_open_paper_trade_for_symbol=lambda symbol: None,
            is_symbol_in_paper_cooldown=lambda symbol, now_utc: (False, ""),
            place_paper_orders_from_trade=lambda trade: [],
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            safe_insert_broker_order=lambda **kwargs: None,
            upsert_trade_lifecycle=lambda **kwargs: None,
            to_float_or_none=lambda value: float(value) if value not in (None, "") else None,
            MIN_CONFIDENCE=75,
            resolve_account_size=lambda payload: 1000000.0,
            active_broker="IBKR",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(len(inserted_attempts), 1)
        self.assertEqual(inserted_attempts[0]["broker"], "IBKR")
        self.assertEqual(inserted_attempts[0]["symbol"], "CLOV")


if __name__ == "__main__":
    unittest.main()
