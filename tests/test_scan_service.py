import unittest
import sys
from types import SimpleNamespace
from types import ModuleType
from unittest.mock import patch

from services.scan_service import (
    _apply_hard_notional_cap,
    _apply_low_price_notional_cap,
    _apply_confidence_loss_sizing,
    _apply_minimum_viable_position_sizing,
    _cap_account_size,
    _requires_fractional_above_cap,
    evaluate_symbol_performance_gate,
    execute_full_scan,
    _get_live_ibkr_account_equity,
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

    def test_minimum_viable_position_sizing_does_not_bypass_hard_notional_cap(self):
        metrics = {
            "entry": 600.0,
            "remaining_allocatable_capital": 1000.0,
            "remaining_slots": 4,
            "shares": 0,
            "risk_per_share": 10.0,
            "cash_affordable_shares": 1,
            "per_trade_notional": 250.0,
            "hard_max_notional": 250.0,
        }

        _apply_minimum_viable_position_sizing(metrics)

        self.assertEqual(metrics["shares"], 0)
        self.assertAlmostEqual(metrics["per_trade_notional"], 250.0, places=4)

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

        with patch.dict("os.environ", {"PAPER_MAX_NOTIONAL": "10000"}, clear=False):
            _apply_hard_notional_cap(metrics)

        self.assertAlmostEqual(metrics["hard_max_notional"], 10000.0, places=4)
        self.assertEqual(metrics["shares"], 207)
        self.assertAlmostEqual(metrics["per_trade_notional"], 9977.4, places=4)
        self.assertAlmostEqual(metrics["adjusted_per_trade_notional"], 9977.4, places=4)
        self.assertAlmostEqual(metrics["actual_position_cost"], 9977.4, places=4)

    def test_hard_notional_cap_supports_fractional_shares_when_enabled(self):
        metrics = {
            "entry": 600.0,
            "risk_per_share": 10.0,
            "per_trade_notional": 1000.0,
            "adjusted_per_trade_notional": 1000.0,
            "remaining_allocatable_capital": 1000.0,
        }

        with patch.dict(
            "os.environ",
            {
                "ENABLE_FRACTIONAL_SHARES": "true",
                "FRACTIONAL_SHARE_DECIMALS": "4",
                "PAPER_MAX_NOTIONAL": "250",
            },
            clear=False,
        ):
            _apply_hard_notional_cap(metrics)

        self.assertAlmostEqual(metrics["hard_max_notional"], 250.0, places=4)
        self.assertGreater(metrics["shares"], 0)
        self.assertLess(metrics["shares"], 1)
        self.assertAlmostEqual(metrics["per_trade_notional"], 249.96, places=2)

    def test_hard_notional_cap_defaults_to_250_when_env_is_missing(self):
        metrics = {
            "entry": 100.0,
            "risk_per_share": 2.0,
            "per_trade_notional": 1000.0,
            "adjusted_per_trade_notional": 1000.0,
            "remaining_allocatable_capital": 1000.0,
        }

        with patch.dict("os.environ", {"PAPER_MAX_NOTIONAL": ""}, clear=False):
            _apply_hard_notional_cap(metrics)

        self.assertAlmostEqual(metrics["hard_max_notional"], 250.0, places=4)
        self.assertEqual(metrics["shares"], 2)
        self.assertAlmostEqual(metrics["per_trade_notional"], 200.0, places=4)

    def test_account_size_cap_defaults_to_1000(self):
        with patch.dict("os.environ", {"PAPER_ACCOUNT_HARD_CAP": "", "SCHEDULED_PAPER_ACCOUNT_SIZE": ""}, clear=False):
            self.assertEqual(_cap_account_size(5000.0), 1000.0)
            self.assertEqual(_cap_account_size(750.0), 750.0)

    def test_requires_fractional_above_cap_when_entry_exceeds_250(self):
        with patch.dict("os.environ", {"ENABLE_FRACTIONAL_SHARES": "false", "PAPER_MAX_NOTIONAL": "250"}, clear=False):
            blocked, reason = _requires_fractional_above_cap({"entry": 600.0, "hard_max_notional": 250.0})
        self.assertTrue(blocked)
        self.assertIn("entry_price_above_whole_share_cap_250.00", reason)

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

    def test_live_account_equity_reads_from_ibkr_broker(self):
        fake_broker = SimpleNamespace(get_account=lambda: {"equity": "12345.67"})

        fake_runtime_context = ModuleType("orchestration.runtime_context")
        fake_runtime_context.IBKR_PAPER_BROKER = fake_broker

        with patch.dict(sys.modules, {"orchestration.runtime_context": fake_runtime_context}):
            equity = _get_live_ibkr_account_equity({})

        self.assertAlmostEqual(equity, 12345.67, places=2)

    def test_execute_full_scan_defaults_attempt_broker_to_active_broker(self):
        inserted_attempts = []

        def fake_run_scan(
            account_size,
            mode,
            current_open_positions=0,
            current_open_exposure=0.0,
            disable_strategy_gates=False,
        ):
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

    def test_execute_full_scan_skips_above_cap_symbol_without_fractional_and_caps_account_size(self):
        inserted_attempts = []
        place_calls = []
        captured_account_sizes = []

        candidate_metrics = {
            "symbol": "META",
            "direction": "BUY",
            "entry": 600.0,
            "stop": 590.0,
            "target": 620.0,
            "shares": 1.0,
            "per_trade_notional": 600.0,
            "remaining_allocatable_capital": 1000.0,
            "remaining_slots": 4,
            "risk_per_share": 10.0,
            "final_confidence": 99.0,
        }

        def fake_run_scan(
            account_size,
            mode,
            current_open_positions=0,
            current_open_exposure=0.0,
            disable_strategy_gates=False,
        ):
            captured_account_sizes.append(account_size)
            evaluation = {
                "decision": "VALID",
                "final_reason": "valid_signal",
                "metrics": dict(candidate_metrics),
            }
            return (
                [{"name": "META", "final_reason": "valid_signal", "metrics": dict(candidate_metrics)}],
                [evaluation],
                True,
                0,
                {"SP500": "NEUTRAL", "NASDAQ": "NEUTRAL"},
                f"IBKR_{mode.upper()}",
            )

        def fake_candidate_from_evaluation(evaluation):
            return {
                "name": "META",
                "decision": evaluation.get("decision", "VALID"),
                "final_reason": evaluation.get("final_reason", "valid_signal"),
                "metrics": dict(evaluation.get("metrics", {})),
            }

        with patch.dict(
            "os.environ",
            {
                "ENABLE_FRACTIONAL_SHARES": "false",
                "PAPER_MAX_NOTIONAL": "250",
                "PAPER_ACCOUNT_HARD_CAP": "1000",
            },
            clear=False,
        ):
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
                paper_candidate_from_evaluation=fake_candidate_from_evaluation,
                evaluate_symbol=lambda *args, **kwargs: None,
                get_latest_open_paper_trade_for_symbol=lambda symbol: None,
                is_symbol_in_paper_cooldown=lambda symbol, now_utc: (False, ""),
                place_paper_orders_from_trade=lambda trade, max_notional=None: place_calls.append(
                    (trade, max_notional)
                )
                or [],
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
        self.assertEqual(captured_account_sizes[0], 1000.0)
        self.assertEqual(result["account_size_hard_cap"], 1000.0)
        self.assertEqual(len(place_calls), 0)
        paper_result = result.get("paper_trade_result", {})
        self.assertTrue(paper_result.get("attempted"))
        self.assertFalse(paper_result.get("placed"))
        self.assertTrue(
            any("entry_price_above_whole_share_cap_250.00" in str(reason) for reason in paper_result.get("skip_reasons", []))
        )

    def test_execute_full_scan_passes_allowlist_to_run_scan_when_supported(self):
        captured_allowed_symbols = []

        def fake_run_scan(
            account_size,
            mode,
            current_open_positions=0,
            current_open_exposure=0.0,
            disable_strategy_gates=False,
            allowed_symbols=None,
        ):
            captured_allowed_symbols.append(list(allowed_symbols or []))
            return (
                [],
                [],
                [],
                [],
                {"SP500": "NEUTRAL", "NASDAQ": "NEUTRAL"},
                f"IBKR_{mode.upper()}",
            )

        with patch(
            "services.scan_service._resolve_scan_symbol_allowlist",
            return_value={
                "filter_applied": True,
                "mode": "primary",
                "requested_session_date": "2026-04-20",
                "source_session_date": "2026-04-20",
                "fallback_used": False,
                "allowed_count": 2,
                "excluded_count": 0,
                "allowed_symbols": ["QCOM", "CSCO"],
            },
        ):
            result = execute_full_scan(
                {"mode": "primary", "paper_trade": False, "scan_source": "SCHEDULED"},
                market_time_check=lambda: (True, "Market timing OK."),
                build_scan_id=lambda timestamp_utc, mode: f"{mode}-scan",
                market_phase_from_timestamp=lambda timestamp_utc: "OPEN",
                append_signal_log=lambda row: None,
                safe_insert_paper_trade_attempt=lambda **kwargs: None,
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
                resolve_account_size=lambda payload: 1000.0,
                active_broker="IBKR",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(captured_allowed_symbols[0], ["QCOM", "CSCO"])
        self.assertTrue(result["symbol_allowlist"]["filter_applied"])
        self.assertEqual(result["symbol_allowlist"]["allowed_count"], 2)


if __name__ == "__main__":
    unittest.main()
