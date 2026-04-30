import unittest
from unittest.mock import patch

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
            sync_order_by_id=lambda order_id: {"id": order_id, "status": "unknown"},
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
        self.assertEqual(captured_lifecycle["side"], "BUY")
        self.assertEqual(captured_lifecycle["broker"], "IBKR")
        self.assertEqual(captured_lifecycle["exit_order_id"], "65")
        self.assertEqual(logged_events[0]["event_type"], "EOD_CLOSE")

    def test_ibkr_eod_close_reports_real_broker_failure_when_position_remains_open(self):
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
            sync_order_by_id=lambda order_id: {"id": order_id, "status": "Cancelled"},
            safe_insert_broker_order=lambda **kwargs: None,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            upsert_trade_lifecycle=lambda **kwargs: captured_lifecycle.update(kwargs),
            to_float_or_none=to_float_or_none,
            parse_iso_utc=parse_iso_utc,
        )

        self.assertEqual(result["closed_count"], 0)
        self.assertEqual(result["skipped_count"], 1)
        self.assertFalse(result["results"][0]["closed"])
        self.assertEqual(result["results"][0]["reason"], "broker_close_failed")
        self.assertEqual(result["results"][0]["order_id"], "65")
        self.assertEqual(captured_lifecycle["status"], "OPEN")
        self.assertEqual(captured_lifecycle["side"], "BUY")
        self.assertEqual(captured_lifecycle["exit_order_id"], "65")

    def test_ibkr_eod_close_uses_sync_lookup_when_open_order_read_would_fail(self):
        inserted_events = []
        captured_lifecycle = {}

        result = execute_close_all_paper_positions(
            get_open_positions=lambda: [
                {
                    "symbol": "CMCSA",
                    "qty": 7,
                    "side": "long",
                    "current_price": "34.12",
                    "broker": "IBKR",
                }
            ],
            get_managed_open_paper_trades_for_eod_close=lambda: [
                {
                    "timestamp_utc": "2026-04-23T16:35:54+00:00",
                    "symbol": "CMCSA",
                    "name": "Comcast",
                    "mode": "core_two",
                    "side": "BUY",
                    "shares": "7",
                    "entry_price": "34.31",
                    "stop_price": "33.90",
                    "target_price": "35.10",
                    "broker": "IBKR",
                    "broker_order_id": "797",
                    "broker_parent_order_id": "797",
                }
            ],
            cancel_open_orders_for_symbol=lambda symbol: ["798", "799"],
            close_position=lambda symbol, cancel_orders=True: {
                "id": "810",
                "status": "PendingSubmit",
                "position_closed": False,
                "close_failed": True,
                "reason": "broker_close_not_confirmed",
                "status_transitions": [
                    {"attempt": 0, "status": "PendingSubmit"},
                    {"attempt": 12, "status": "PendingSubmit", "broker_position_open": True},
                ],
            },
            get_order_by_id=lambda order_id, nested=False: (_ for _ in ()).throw(
                RuntimeError("IBKR bridge request failed during GET /orders/810: 503")
            ),
            sync_order_by_id=lambda order_id: {
                "id": order_id,
                "status": "filled",
                "filled_qty": 7,
                "avg_fill_price": "34.02",
                "filled_at": "2026-04-23T19:55:58+00:00",
            },
            safe_insert_broker_order=lambda **kwargs: None,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: inserted_events.append(kwargs),
            upsert_trade_lifecycle=lambda **kwargs: captured_lifecycle.update(kwargs),
            to_float_or_none=to_float_or_none,
            parse_iso_utc=parse_iso_utc,
        )

        self.assertEqual(result["closed_count"], 1)
        self.assertEqual(result["skipped_count"], 0)
        self.assertTrue(result["results"][0]["closed"])
        self.assertEqual(result["results"][0]["close_order_id"], "810")
        self.assertEqual(captured_lifecycle["status"], "CLOSED")
        self.assertEqual(captured_lifecycle["exit_order_id"], "810")
        self.assertEqual(captured_lifecycle["exit_price"], 34.02)
        self.assertEqual(captured_lifecycle["realized_pnl"], -2.03)
        self.assertEqual(inserted_events[0]["event_type"], "EOD_CLOSE")
        self.assertEqual(inserted_events[0]["price"], 34.02)

    def test_ibkr_eod_close_does_not_count_unresolved_order_as_closed(self):
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
                "position_closed": False,
                "close_failed": False,
                "reason": "",
                "status_transitions": [
                    {"attempt": 0, "status": "PendingSubmit"},
                    {"attempt": 12, "status": "Submitted", "broker_position_open": True},
                ],
            },
            get_order_by_id=lambda order_id, nested=False: {"status": "Submitted"},
            sync_order_by_id=lambda order_id: {"id": order_id, "status": "Submitted"},
            safe_insert_broker_order=lambda **kwargs: None,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            upsert_trade_lifecycle=lambda **kwargs: captured_lifecycle.update(kwargs),
            to_float_or_none=to_float_or_none,
            parse_iso_utc=parse_iso_utc,
        )

        self.assertEqual(result["closed_count"], 0)
        self.assertEqual(result["skipped_count"], 1)
        self.assertFalse(result["results"][0]["closed"])
        self.assertEqual(result["results"][0]["reason"], "broker_close_unresolved")
        self.assertEqual(result["results"][0]["order_id"], "65")
        self.assertEqual(captured_lifecycle["status"], "OPEN")
        self.assertEqual(captured_lifecycle["exit_order_id"], "65")

    def test_ibkr_eod_close_timeout_is_classified_explicitly(self):
        result = execute_close_all_paper_positions(
            get_open_positions=lambda: [
                {
                    "symbol": "PLTR",
                    "qty": 75,
                    "side": "long",
                    "current_price": "133.36",
                    "broker": "IBKR",
                }
            ],
            get_managed_open_paper_trades_for_eod_close=lambda: [
                {
                    "timestamp_utc": "2026-04-13T16:59:31+00:00",
                    "symbol": "PLTR",
                    "name": "Palantir",
                    "mode": "core_one",
                    "side": "BUY",
                    "shares": "75",
                    "entry_price": "132.97",
                    "stop_price": "130.723",
                    "target_price": "137.464",
                    "broker": "IBKR",
                    "broker_order_id": "136",
                    "broker_parent_order_id": "136",
                }
            ],
            cancel_open_orders_for_symbol=lambda symbol: [],
            close_position=lambda symbol, cancel_orders=True: (_ for _ in ()).throw(
                RuntimeError("IBKR bridge timeout during POST /positions/close after 20s")
            ),
            get_order_by_id=lambda order_id, nested=False: {"status": "unknown"},
            sync_order_by_id=lambda order_id: {"id": order_id, "status": "unknown"},
            safe_insert_broker_order=lambda **kwargs: None,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            upsert_trade_lifecycle=lambda **kwargs: None,
            to_float_or_none=to_float_or_none,
            parse_iso_utc=parse_iso_utc,
        )

        self.assertEqual(result["closed_count"], 0)
        self.assertEqual(result["skipped_count"], 1)
        self.assertEqual(result["results"][0]["reason"], "bridge_timeout")
        self.assertIn("IBKR bridge timeout", result["results"][0]["details"])
        self.assertIn("IBKR bridge timeout", result["results"][0]["bridge_issue"])

    def test_ibkr_eod_close_stops_after_batch_time_budget(self):
        with patch("services.trade_service.time.monotonic", side_effect=[0.0, 1.0, 121.0, 121.0]):
            result = execute_close_all_paper_positions(
                get_open_positions=lambda: [
                    {"symbol": "PLTR", "qty": 75, "side": "long", "current_price": "133.36", "broker": "IBKR"},
                    {"symbol": "SMCI", "qty": 386, "side": "long", "current_price": "25.92", "broker": "IBKR"},
                ],
                get_managed_open_paper_trades_for_eod_close=lambda: [
                    {
                        "timestamp_utc": "2026-04-13T16:59:31+00:00",
                        "symbol": "PLTR",
                        "name": "Palantir",
                        "mode": "core_one",
                        "side": "BUY",
                        "shares": "75",
                        "entry_price": "132.97",
                        "stop_price": "130.723",
                        "target_price": "137.464",
                        "broker": "IBKR",
                        "broker_order_id": "136",
                        "broker_parent_order_id": "136",
                    },
                    {
                        "timestamp_utc": "2026-04-13T16:48:53+00:00",
                        "symbol": "SMCI",
                        "name": "Super Micro",
                        "mode": "core_three",
                        "side": "BUY",
                        "shares": "386",
                        "entry_price": "25.88",
                        "stop_price": "25.348",
                        "target_price": "26.944",
                        "broker": "IBKR",
                        "broker_order_id": "112",
                        "broker_parent_order_id": "112",
                    },
                ],
                cancel_open_orders_for_symbol=lambda symbol: [],
                close_position=lambda symbol, cancel_orders=True: {
                    "id": f"close-{symbol}",
                    "status": "Filled",
                    "position_closed": True,
                    "filled_qty": 1,
                    "filled_avg_price": "1.0",
                    "status_transitions": [],
                },
                get_order_by_id=lambda order_id, nested=False: {
                    "status": "Filled",
                    "filled_qty": 1,
                    "filled_avg_price": "1.0",
                },
                sync_order_by_id=lambda order_id: {
                    "id": order_id,
                    "status": "Filled",
                    "filled_qty": 1,
                    "filled_avg_price": "1.0",
                },
                safe_insert_broker_order=lambda **kwargs: None,
                append_trade_log=lambda row: None,
                safe_insert_trade_event=lambda **kwargs: None,
                upsert_trade_lifecycle=lambda **kwargs: None,
                to_float_or_none=to_float_or_none,
                parse_iso_utc=parse_iso_utc,
            )

        self.assertTrue(result["partial"])
        self.assertEqual(result["stopped_reason"], "ibkr_batch_time_budget_exceeded")
        self.assertEqual(result["closed_count"], 1)
        self.assertEqual(result["results"][-1]["reason"], "batch_time_budget_exceeded")


if __name__ == "__main__":
    unittest.main()
