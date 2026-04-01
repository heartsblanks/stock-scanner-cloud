import unittest

from orchestration.app_orchestration import handle_scan_request


class AppOrchestrationTests(unittest.TestCase):
    def test_handle_scan_request_injects_risk_context_for_paper_trade(self):
        captured = {}

        def fake_execute_full_scan(scan_payload, **kwargs):
            captured["payload"] = scan_payload
            captured["kwargs"] = kwargs
            return {"ok": True}

        result = handle_scan_request(
            {"mode": "primary", "paper_trade": True},
            get_current_open_position_state=lambda: (2, 1234.56),
            get_risk_exposure_summary=lambda: {
                "daily_realized_pnl": -100.0,
                "daily_unrealized_pnl": 25.5,
            },
            execute_full_scan=fake_execute_full_scan,
            market_time_check=lambda: (True, "ok"),
            build_scan_id=lambda ts, mode: "scan-1",
            market_phase_from_timestamp=lambda ts: "MORNING",
            append_signal_log=lambda row: None,
            safe_insert_paper_trade_attempt=lambda **kwargs: None,
            safe_insert_scan_run=lambda **kwargs: None,
            parse_iso_utc=lambda ts: ts,
            run_scan=lambda *args, **kwargs: ([], [], True, 0, {}, "PRIMARY"),
            trade_to_dict=lambda trade: trade,
            debug_to_dict=lambda trade: trade,
            paper_candidate_from_evaluation=lambda ev: ev,
            evaluate_symbol=lambda *args, **kwargs: {},
            get_latest_open_paper_trade_for_symbol=lambda symbol: None,
            is_symbol_in_paper_cooldown=lambda symbol, ts: (False, ""),
            place_paper_bracket_order_from_trade=lambda trade: {},
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            safe_insert_broker_order=lambda **kwargs: None,
            to_float_or_none=lambda value: float(value) if value not in (None, "") else None,
            min_confidence=70,
            upsert_trade_lifecycle=lambda **kwargs: None,
        )

        self.assertEqual(result, {"ok": True})
        self.assertEqual(captured["payload"]["current_open_positions"], 2)
        self.assertEqual(captured["payload"]["current_open_exposure"], 1234.56)
        self.assertEqual(captured["payload"]["daily_realized_pnl"], -100.0)
        self.assertEqual(captured["payload"]["daily_unrealized_pnl"], 25.5)


if __name__ == "__main__":
    unittest.main()
