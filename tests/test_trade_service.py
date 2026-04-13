import unittest

from orchestration.scan_context import parse_iso_utc, to_float_or_none
from services.trade_service import execute_close_all_paper_positions


class TradeServiceTests(unittest.TestCase):
    def test_ibkr_eod_close_heals_db_when_broker_position_is_already_flat(self):
        logged_events = []
        inserted_events = []
        inserted_orders = []
        captured_lifecycle = {}

        result = execute_close_all_paper_positions(
            get_open_positions=lambda: [
                {
                    "symbol": "NVDA",
                    "qty": 52,
                    "side": "long",
                    "current_price": "188.50",
                    "broker": "IBKR",
                }
            ],
            get_managed_open_paper_trades_for_eod_close=lambda: [
                {
                    "timestamp_utc": "2026-04-10T15:36:53+00:00",
                    "symbol": "NVDA",
                    "name": "NVIDIA",
                    "mode": "core_one",
                    "side": "BUY",
                    "shares": "52",
                    "entry_price": "189.59",
                    "stop_price": "187.532",
                    "target_price": "193.706",
                    "broker": "IBKR",
                    "broker_order_id": "40",
                    "broker_parent_order_id": "40",
                }
            ],
            cancel_open_orders_for_symbol=lambda symbol: ["41", "42"],
            close_position=lambda symbol, cancel_orders=True: {
                "id": "65",
                "status": "Submitted",
                "position_closed": True,
                "filled_qty": 52,
                "filled_avg_price": "",
                "status_transitions": [
                    {"attempt": 0, "status": "PendingSubmit"},
                    {"attempt": 1, "status": "Submitted", "broker_position_open": False},
                ],
            },
            get_order_by_id=lambda order_id, nested=False: {"status": "unknown"},
            safe_insert_broker_order=lambda **kwargs: inserted_orders.append(kwargs),
            append_trade_log=lambda row: logged_events.append(row),
            safe_insert_trade_event=lambda **kwargs: inserted_events.append(kwargs),
            upsert_trade_lifecycle=lambda **kwargs: captured_lifecycle.update(kwargs),
            to_float_or_none=to_float_or_none,
            parse_iso_utc=parse_iso_utc,
        )

        self.assertEqual(result["closed_count"], 1)
        self.assertEqual(result["skipped_count"], 0)
        self.assertTrue(result["results"][0]["closed"])
        self.assertEqual(result["results"][0]["close_order_id"], "65")
        self.assertEqual(inserted_orders[0]["order_id"], "65")
        self.assertEqual(inserted_events[0]["event_type"], "EOD_CLOSE")
        self.assertEqual(captured_lifecycle["status"], "CLOSED")
        self.assertEqual(captured_lifecycle["broker"], "IBKR")
        self.assertEqual(captured_lifecycle["exit_order_id"], "65")
        self.assertEqual(logged_events[0]["event_type"], "EOD_CLOSE")

    def test_ibkr_eod_close_reports_real_broker_failure_when_position_remains_open(self):
        result = execute_close_all_paper_positions(
            get_open_positions=lambda: [
                {
                    "symbol": "NVDA",
                    "qty": 52,
                    "side": "long",
                    "current_price": "188.50",
                    "broker": "IBKR",
                }
            ],
            get_managed_open_paper_trades_for_eod_close=lambda: [
                {
                    "timestamp_utc": "2026-04-10T15:36:53+00:00",
                    "symbol": "NVDA",
                    "name": "NVIDIA",
                    "mode": "core_one",
                    "side": "BUY",
                    "shares": "52",
                    "entry_price": "189.59",
                    "stop_price": "187.532",
                    "target_price": "193.706",
                    "broker": "IBKR",
                    "broker_order_id": "40",
                    "broker_parent_order_id": "40",
                }
            ],
            cancel_open_orders_for_symbol=lambda symbol: ["41", "42"],
            close_position=lambda symbol, cancel_orders=True: {
                "id": "65",
                "status": "Cancelled",
                "position_closed": False,
                "close_failed": True,
                "reason": "broker_close_not_confirmed",
                "status_transitions": [
                    {"attempt": 0, "status": "PendingSubmit"},
                    {"attempt": 12, "status": "Cancelled", "broker_position_open": True},
                ],
            },
            get_order_by_id=lambda order_id, nested=False: {"status": "Cancelled"},
            safe_insert_broker_order=lambda **kwargs: None,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            upsert_trade_lifecycle=lambda **kwargs: None,
            to_float_or_none=to_float_or_none,
            parse_iso_utc=parse_iso_utc,
        )

        self.assertEqual(result["closed_count"], 0)
        self.assertEqual(result["skipped_count"], 1)
        self.assertFalse(result["results"][0]["closed"])
        self.assertEqual(result["results"][0]["reason"], "broker_close_failed")
        self.assertEqual(result["results"][0]["order_id"], "65")

    def test_ibkr_eod_close_does_not_count_unresolved_order_as_closed(self):
        result = execute_close_all_paper_positions(
            get_open_positions=lambda: [
                {
                    "symbol": "NVDA",
                    "qty": 52,
                    "side": "long",
                    "current_price": "188.50",
                    "broker": "IBKR",
                }
            ],
            get_managed_open_paper_trades_for_eod_close=lambda: [
                {
                    "timestamp_utc": "2026-04-10T15:36:53+00:00",
                    "symbol": "NVDA",
                    "name": "NVIDIA",
                    "mode": "core_one",
                    "side": "BUY",
                    "shares": "52",
                    "entry_price": "189.59",
                    "stop_price": "187.532",
                    "target_price": "193.706",
                    "broker": "IBKR",
                    "broker_order_id": "40",
                    "broker_parent_order_id": "40",
                }
            ],
            cancel_open_orders_for_symbol=lambda symbol: ["41", "42"],
            close_position=lambda symbol, cancel_orders=True: {
                "id": "65",
                "status": "Submitted",
                "position_closed": False,
                "close_failed": False,
                "reason": "",
                "status_transitions": [
                    {"attempt": 0, "status": "PendingSubmit"},
                    {"attempt": 12, "status": "Submitted", "broker_position_open": True},
                ],
            },
            get_order_by_id=lambda order_id, nested=False: {"status": "Submitted"},
            safe_insert_broker_order=lambda **kwargs: None,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            upsert_trade_lifecycle=lambda **kwargs: None,
            to_float_or_none=to_float_or_none,
            parse_iso_utc=parse_iso_utc,
        )

        self.assertEqual(result["closed_count"], 0)
        self.assertEqual(result["skipped_count"], 1)
        self.assertFalse(result["results"][0]["closed"])
        self.assertEqual(result["results"][0]["reason"], "broker_close_unresolved")
        self.assertEqual(result["results"][0]["order_id"], "65")


if __name__ == "__main__":
    unittest.main()
