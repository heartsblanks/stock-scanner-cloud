import unittest
from unittest.mock import patch

from orchestration.scan_context import parse_iso_utc, to_float_or_none
from services.sync_service import execute_sync_paper_trades


class SyncServiceTests(unittest.TestCase):
    def test_sync_recovers_orphan_ibkr_open_trade_from_broker_open_state(self):
        captured_events = []
        captured_broker_orders = []
        captured_lifecycles = []

        result = execute_sync_paper_trades(
            get_open_paper_trades=lambda: [],
            sync_order_by_id_for_broker=lambda broker, parent_id: {
                "symbol": "PLTR",
                "parent_status": "Filled",
                "entry_filled_avg_price": "151.05",
                "entry_filled_at": "2026-04-22T14:45:51+00:00",
                "entry_filled_qty": "1",
                "client_order_id": "scanner-PLTR-BUY-1518900-1",
            },
            paper_trade_exit_already_logged=lambda parent_order_id, exit_event: False,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: captured_events.append(kwargs),
            safe_insert_broker_order=lambda **kwargs: captured_broker_orders.append(kwargs),
            upsert_trade_lifecycle=lambda **kwargs: captured_lifecycles.append(kwargs),
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
            get_open_state_for_broker=lambda broker: {
                "positions": [
                    {
                        "symbol": "PLTR",
                        "qty": 1,
                        "side": "long",
                        "avg_entry_price": 152.05,
                    }
                ],
                "orders": [
                    {
                        "id": "792",
                        "symbol": "PLTR",
                        "type": "limit",
                        "limit_price": 155.76,
                        "status": "Submitted",
                        "parent_id": "791",
                    },
                    {
                        "id": "793",
                        "symbol": "PLTR",
                        "type": "stp",
                        "stop_price": 149.95,
                        "status": "PreSubmitted",
                        "parent_id": "791",
                    },
                ],
            },
            close_position_for_broker=lambda broker, symbol: {"ok": True},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(len(captured_events), 1)
        self.assertEqual(captured_events[0]["symbol"], "PLTR")
        self.assertEqual(captured_events[0]["event_type"], "OPEN")
        self.assertEqual(captured_events[0]["mode"], "orphan")
        self.assertEqual(captured_events[0]["order_id"], "791")
        self.assertEqual(len(captured_broker_orders), 1)
        self.assertEqual(captured_broker_orders[0]["order_id"], "791")
        self.assertEqual(captured_broker_orders[0]["status"], "Filled")
        self.assertGreaterEqual(len(captured_lifecycles), 1)
        self.assertEqual(captured_lifecycles[0]["trade_key"], "IBKR:PLTR:791")
        self.assertEqual(captured_lifecycles[0]["status"], "OPEN")
        self.assertEqual(captured_lifecycles[0]["mode"], "orphan")
        self.assertEqual(captured_lifecycles[0]["target_price"], 155.76)
        self.assertEqual(captured_lifecycles[0]["stop_price"], 149.95)
        self.assertIn("orphan_open_recovered", [row.get("reason") for row in result["results"]])

    def test_sync_close_falls_back_to_price_based_pnl_when_broker_reports_zero(self):
        captured_lifecycle = {}

        def upsert_trade_lifecycle(**kwargs):
            captured_lifecycle.update(kwargs)

        result = execute_sync_paper_trades(
            get_open_paper_trades=lambda: [
                {
                    "timestamp_utc": "2026-04-21T14:35:03+00:00",
                    "symbol": "RIVN",
                    "name": "Rivian",
                    "mode": "primary",
                    "side": "BUY",
                    "shares": "14",
                    "entry_price": "17.38",
                    "stop_price": "17.2505",
                    "target_price": "17.639",
                    "broker_order_id": "815",
                    "broker_parent_order_id": "815",
                }
            ],
            sync_order_by_id=lambda parent_id: {
                "exit_event": "MANUAL_CLOSE",
                "exit_price": "17.24",
                "exit_status": "filled",
                "exit_filled_qty": "14",
                "exit_filled_avg_price": "17.24",
                "exit_order_id": "817",
                "exit_reason": "BROKER_FILLED_EXIT",
                "parent_status": "filled",
                "exit_filled_at": "2026-04-21T14:59:20+00:00",
                "exit_realized_pnl": "0",
                "exit_realized_pnl_confirmed": True,
            },
            paper_trade_exit_already_logged=lambda parent_order_id, exit_event: False,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            safe_insert_broker_order=lambda **kwargs: None,
            upsert_trade_lifecycle=upsert_trade_lifecycle,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
            get_open_positions=lambda: [],
            close_position=lambda symbol: {"ok": True},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["synced_count"], 1)
        self.assertAlmostEqual(captured_lifecycle["realized_pnl"], -1.96, places=2)
        self.assertAlmostEqual(captured_lifecycle["realized_pnl_percent"], -0.805524, places=6)

    def test_ibkr_time_stop_closes_slow_trade_and_captures_price_based_pnl(self):
        captured_lifecycles = []
        close_calls = []

        result = execute_sync_paper_trades(
            get_open_paper_trades=lambda: [
                {
                    "timestamp_utc": "2026-04-21T14:35:03+00:00",
                    "symbol": "RIVN",
                    "name": "Rivian",
                    "mode": "primary",
                    "side": "BUY",
                    "shares": "10",
                    "entry_price": "100.00",
                    "stop_price": "99.00",
                    "target_price": "102.00",
                    "broker_order_id": "815",
                    "broker_parent_order_id": "815",
                    "broker": "IBKR",
                }
            ],
            sync_order_by_id_for_broker=lambda broker, parent_id: {
                "id": parent_id,
                "status": "submitted",
                "parent_status": "Submitted",
            },
            paper_trade_exit_already_logged=lambda parent_order_id, exit_event: False,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            safe_insert_broker_order=lambda **kwargs: None,
            upsert_trade_lifecycle=lambda **kwargs: captured_lifecycles.append(kwargs),
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
            get_open_state_for_broker=lambda broker: {
                "positions": [
                    {
                        "symbol": "RIVN",
                        "qty": 10,
                        "side": "long",
                        "current_price": "100.10",
                    }
                ],
                "orders": [],
            },
            close_position_for_broker=lambda broker, symbol: close_calls.append((broker, symbol)) or {
                "ok": True,
                "order_id": "900",
                "status": "Submitted",
                "position_closed": True,
                "filled_qty": "",
                "filled_avg_price": "100.10",
            },
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["synced_count"], 1)
        self.assertEqual(result["results"][-1]["exit_event"], "TIME_STOP")
        self.assertEqual(close_calls, [("IBKR", "RIVN")])
        self.assertEqual(captured_lifecycles[-1]["status"], "CLOSED")
        self.assertEqual(captured_lifecycles[-1]["exit_reason"], "TIME_STOP")
        self.assertAlmostEqual(captured_lifecycles[-1]["exit_price"], 100.10, places=2)
        self.assertAlmostEqual(captured_lifecycles[-1]["realized_pnl"], 1.0, places=2)

    def test_ibkr_time_stop_does_not_finalize_without_confirmed_fill_price(self):
        captured_lifecycles = []

        result = execute_sync_paper_trades(
            get_open_paper_trades=lambda: [
                {
                    "timestamp_utc": "2026-04-21T14:35:03+00:00",
                    "symbol": "RIVN",
                    "mode": "primary",
                    "side": "BUY",
                    "shares": "10",
                    "entry_price": "100.00",
                    "stop_price": "99.00",
                    "target_price": "102.00",
                    "broker_order_id": "815",
                    "broker_parent_order_id": "815",
                    "broker": "IBKR",
                }
            ],
            sync_order_by_id_for_broker=lambda broker, parent_id: {
                "id": parent_id,
                "status": "submitted",
                "parent_status": "Submitted",
            },
            paper_trade_exit_already_logged=lambda parent_order_id, exit_event: False,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            safe_insert_broker_order=lambda **kwargs: None,
            upsert_trade_lifecycle=lambda **kwargs: captured_lifecycles.append(kwargs),
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
            get_open_state_for_broker=lambda broker: {
                "positions": [{"symbol": "RIVN", "qty": 10, "side": "long", "current_price": "100.10"}],
                "orders": [],
            },
            close_position_for_broker=lambda broker, symbol: {
                "ok": True,
                "order_id": "900",
                "status": "Submitted",
                "position_closed": True,
                "filled_qty": "",
                "filled_avg_price": "",
            },
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["synced_count"], 0)
        self.assertEqual(result["results"][-1]["reason"], "time_stop_close_requested")
        self.assertEqual(captured_lifecycles[-1]["status"], "CLOSED")
        self.assertEqual(captured_lifecycles[-1]["exit_reason"], "TIME_STOP_CLOSE_REQUESTED_PENDING_FILL_SYNC")
        self.assertIsNone(captured_lifecycles[-1]["exit_price"])
        self.assertIsNone(captured_lifecycles[-1]["realized_pnl"])

    def test_sync_close_prefers_broker_entry_fill_price_for_pnl(self):
        captured_lifecycle = {}

        def upsert_trade_lifecycle(**kwargs):
            captured_lifecycle.update(kwargs)

        result = execute_sync_paper_trades(
            get_open_paper_trades=lambda: [
                {
                    "timestamp_utc": "2026-04-21T14:35:29+00:00",
                    "symbol": "CLOV",
                    "name": "Clover",
                    "mode": "third",
                    "side": "BUY",
                    "shares": "21",
                    "entry_price": "2.465",
                    "stop_price": "2.39486",
                    "target_price": "2.60528",
                    "broker_order_id": "833",
                    "broker_parent_order_id": "833",
                }
            ],
            sync_order_by_id=lambda parent_id: {
                "entry_filled_avg_price": "2.48",
                "exit_event": "MANUAL_CLOSE",
                "exit_price": "2.38",
                "exit_status": "filled",
                "exit_filled_qty": "21",
                "exit_filled_avg_price": "2.38",
                "exit_order_id": "835",
                "exit_reason": "BROKER_FILLED_EXIT",
                "parent_status": "filled",
                "exit_filled_at": "2026-04-21T14:55:39+00:00",
                "exit_realized_pnl": "0",
                "exit_realized_pnl_confirmed": True,
            },
            paper_trade_exit_already_logged=lambda parent_order_id, exit_event: False,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            safe_insert_broker_order=lambda **kwargs: None,
            upsert_trade_lifecycle=upsert_trade_lifecycle,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
            get_open_positions=lambda: [],
            close_position=lambda symbol: {"ok": True},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["synced_count"], 1)
        self.assertAlmostEqual(captured_lifecycle["entry_price"], 2.48, places=2)
        self.assertAlmostEqual(captured_lifecycle["realized_pnl"], -2.10, places=2)
        self.assertAlmostEqual(captured_lifecycle["realized_pnl_percent"], -4.032258, places=6)

    def test_sync_close_preserves_long_direction_and_negative_pnl_for_losing_buy_trade(self):
        captured_lifecycle = {}

        def upsert_trade_lifecycle(**kwargs):
            captured_lifecycle.update(kwargs)

        result = execute_sync_paper_trades(
            get_open_paper_trades=lambda: [
                {
                    "timestamp_utc": "2026-03-31T14:20:10+00:00",
                    "symbol": "NU",
                    "name": "Nu",
                    "mode": "core_one",
                    "side": "BUY",
                    "shares": "424",
                    "entry_price": "14.01",
                    "stop_price": "13.95",
                    "target_price": "14.25",
                    "broker_order_id": "entry-1",
                    "broker_parent_order_id": "parent-1",
                    "linked_signal_timestamp_utc": "2026-03-31T14:20:00+00:00",
                    "linked_signal_entry": "14.01",
                    "linked_signal_stop": "13.95",
                    "linked_signal_target": "14.25",
                    "linked_signal_confidence": "80",
                }
            ],
            sync_order_by_id=lambda parent_id: {
                "exit_event": "STOP_HIT",
                "exit_price": "13.95",
                "exit_status": "filled",
                "exit_filled_qty": "424",
                "exit_filled_avg_price": "13.95",
                "exit_order_id": "exit-1",
                "exit_reason": "STOP_HIT",
                "parent_status": "filled",
                "exit_filled_at": "2026-03-31T14:25:02+00:00",
                "exit_realized_pnl": "-25.44",
                "exit_realized_pnl_confirmed": True,
            },
            paper_trade_exit_already_logged=lambda parent_order_id, exit_event: False,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            safe_insert_broker_order=lambda **kwargs: None,
            upsert_trade_lifecycle=upsert_trade_lifecycle,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
            get_open_positions=lambda: [],
            close_position=lambda symbol: {"ok": True},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["synced_count"], 1)
        self.assertEqual(captured_lifecycle["symbol"], "NU")
        self.assertEqual(captured_lifecycle["side"], "BUY")
        self.assertEqual(captured_lifecycle["direction"], "LONG")
        self.assertEqual(captured_lifecycle["broker"], "IBKR")
        self.assertAlmostEqual(captured_lifecycle["realized_pnl"], -25.44, places=2)
        self.assertAlmostEqual(captured_lifecycle["realized_pnl_percent"], -0.428266, places=6)

    def test_ibkr_unknown_parent_is_reconciled_closed_when_no_open_position(self):
        captured_lifecycle = {}

        def upsert_trade_lifecycle(**kwargs):
            captured_lifecycle.update(kwargs)

        result = execute_sync_paper_trades(
            get_open_paper_trades=lambda: [
                {
                    "timestamp_utc": "2026-04-09T14:20:10+00:00",
                    "symbol": "NIO",
                    "name": "NIO Inc",
                    "mode": "first-hour-breakout",
                    "side": "BUY",
                    "shares": "10",
                    "entry_price": "5.00",
                    "stop_price": "4.90",
                    "target_price": "5.30",
                    "broker_order_id": "10",
                    "broker_parent_order_id": "10",
                    "broker": "IBKR",
                }
            ],
            sync_order_by_id_for_broker=lambda broker, parent_id: {
                "status": "unknown",
                "parent_status": "",
                "updated_at": "2026-04-09T14:25:00+00:00",
            },
            paper_trade_exit_already_logged=lambda parent_order_id, exit_event: False,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            safe_insert_broker_order=lambda **kwargs: None,
            upsert_trade_lifecycle=upsert_trade_lifecycle,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
            get_open_positions_for_broker=lambda broker: [],
            close_position_for_broker=lambda broker, symbol: {"ok": True},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["synced_count"], 0)
        self.assertEqual(result["results"][0]["broker"], "IBKR")
        self.assertTrue(result["results"][0]["stale_reconciled"])
        self.assertFalse(result["results"][0]["synced"])
        self.assertEqual(result["results"][0]["reason"], "pending_exit_recon")
        self.assertEqual(captured_lifecycle["status"], "CLOSED")
        self.assertEqual(captured_lifecycle["exit_reason"], "BROKER_POSITION_FLAT_PENDING_FILL_SYNC")
        self.assertEqual(captured_lifecycle["exit_time"], parse_iso_utc("2026-04-09T14:25:00+00:00"))

    def test_ibkr_unknown_parent_pending_recon_preserves_stored_exit_order_id(self):
        captured_lifecycle = {}

        def upsert_trade_lifecycle(**kwargs):
            captured_lifecycle.update(kwargs)

        result = execute_sync_paper_trades(
            get_open_paper_trades=lambda: [
                {
                    "timestamp_utc": "2026-04-22T16:05:02+00:00",
                    "symbol": "RIVN",
                    "name": "Rivian",
                    "mode": "core_two",
                    "side": "BUY",
                    "shares": "14",
                    "entry_price": "18.04",
                    "stop_price": "17.70",
                    "target_price": "18.80",
                    "broker_order_id": "809",
                    "broker_parent_order_id": "809",
                    "broker_exit_order_id": "822",
                    "exit_order_id": "822",
                    "broker": "IBKR",
                }
            ],
            sync_order_by_id_for_broker=lambda broker, parent_id: {
                "status": "unknown",
                "parent_status": "",
                "updated_at": "2026-04-23T16:18:31+00:00",
            },
            paper_trade_exit_already_logged=lambda parent_order_id, exit_event: False,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            safe_insert_broker_order=lambda **kwargs: None,
            upsert_trade_lifecycle=upsert_trade_lifecycle,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
            get_open_positions_for_broker=lambda broker: [],
            close_position_for_broker=lambda broker, symbol: {"ok": True},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["results"][0]["reason"], "pending_exit_recon")
        self.assertEqual(captured_lifecycle["status"], "CLOSED")
        self.assertEqual(captured_lifecycle["exit_order_id"], "822")

    def test_ibkr_sync_timeout_is_classified_explicitly(self):
        result = execute_sync_paper_trades(
            get_open_paper_trades=lambda: [
                {
                    "symbol": "PLTR",
                    "broker_parent_order_id": "136",
                    "broker": "IBKR",
                }
            ],
            sync_order_by_id_for_broker=lambda broker, parent_id: (_ for _ in ()).throw(
                RuntimeError("IBKR bridge timeout during GET /orders/136/sync after 8s")
            ),
            paper_trade_exit_already_logged=lambda parent_order_id, exit_event: False,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            safe_insert_broker_order=lambda **kwargs: None,
            upsert_trade_lifecycle=lambda **kwargs: None,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
            get_open_positions_for_broker=lambda broker: [{"symbol": "PLTR"}],
            close_position_for_broker=lambda broker, symbol: {"ok": True},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["synced_count"], 0)
        self.assertEqual(result["results"][0]["reason"], "bridge_timeout")
        self.assertIn("IBKR bridge timeout", result["results"][0]["details"])
        self.assertIn("IBKR bridge timeout", result["results"][0]["bridge_issue"])

    def test_ibkr_sync_timeout_reconciles_closed_when_symbol_missing_from_live_positions(self):
        captured_lifecycle = {}

        def upsert_trade_lifecycle(**kwargs):
            captured_lifecycle.update(kwargs)

        result = execute_sync_paper_trades(
            get_open_paper_trades=lambda: [
                {
                    "timestamp_utc": "2026-04-13T16:45:14+00:00",
                    "symbol": "SOFI",
                    "name": "SoFi",
                    "mode": "primary",
                    "side": "BUY",
                    "shares": "299",
                    "entry_price": "16.68",
                    "stop_price": "16.4945",
                    "target_price": "17.051",
                    "broker_order_id": "76",
                    "broker_parent_order_id": "76",
                    "broker": "IBKR",
                }
            ],
            sync_order_by_id_for_broker=lambda broker, parent_id: (_ for _ in ()).throw(
                RuntimeError("IBKR bridge timeout during GET /orders/76/sync after 8s")
            ),
            paper_trade_exit_already_logged=lambda parent_order_id, exit_event: False,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            safe_insert_broker_order=lambda **kwargs: None,
            upsert_trade_lifecycle=upsert_trade_lifecycle,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
            get_open_positions_for_broker=lambda broker: [],
            get_open_state_for_broker=lambda broker: {"positions": [], "orders": []},
            close_position_for_broker=lambda broker, symbol: {"ok": True},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["synced_count"], 0)
        self.assertEqual(result["results"][0]["reason"], "bridge_timeout")
        self.assertEqual(captured_lifecycle, {})

    def test_ibkr_sync_timeout_does_not_reconcile_when_related_order_still_open(self):
        result = execute_sync_paper_trades(
            get_open_paper_trades=lambda: [
                {
                    "timestamp_utc": "2026-04-15T16:37:54+00:00",
                    "symbol": "INTC",
                    "name": "Intel",
                    "mode": "core_three",
                    "side": "BUY",
                    "shares": "152",
                    "entry_price": "65.37",
                    "stop_price": "64.467",
                    "target_price": "67.176",
                    "broker_order_id": "166",
                    "broker_parent_order_id": "166",
                    "broker": "IBKR",
                }
            ],
            sync_order_by_id_for_broker=lambda broker, parent_id: (_ for _ in ()).throw(
                RuntimeError("IBKR bridge timeout during GET /orders/166/sync after 8s")
            ),
            paper_trade_exit_already_logged=lambda parent_order_id, exit_event: False,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            safe_insert_broker_order=lambda **kwargs: None,
            upsert_trade_lifecycle=lambda **kwargs: None,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
            get_open_positions_for_broker=lambda broker: [],
            get_open_state_for_broker=lambda broker: {
                "positions": [],
                "orders": [
                    {"id": "167", "parent_id": "166", "symbol": "INTC", "status": "Submitted"},
                ],
            },
            close_position_for_broker=lambda broker, symbol: {"ok": True},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["synced_count"], 0)
        self.assertEqual(result["results"][0]["reason"], "bridge_timeout")

    def test_ibkr_sync_rejects_cross_symbol_identity_mismatch(self):
        captured_lifecycle = {}

        def upsert_trade_lifecycle(**kwargs):
            captured_lifecycle.update(kwargs)

        result = execute_sync_paper_trades(
            get_open_paper_trades=lambda: [
                {
                    "timestamp_utc": "2026-04-13T16:58:57+00:00",
                    "symbol": "RKLB",
                    "name": "Rocket Lab",
                    "mode": "third",
                    "side": "BUY",
                    "shares": "142",
                    "entry_price": "70.42",
                    "stop_price": "69.531",
                    "target_price": "72.198",
                    "broker_order_id": "118",
                    "broker_parent_order_id": "118",
                    "broker": "IBKR",
                }
            ],
            sync_order_by_id_for_broker=lambda broker, parent_id: {
                "status": "closed",
                "parent_order_id": "118",
                "parent_status": "Filled",
                "symbol": "SOUN",
                "client_order_id": "scanner-SOUN-LONG-75150-665",
                "entry_filled_qty": "665",
                "entry_filled_avg_price": "7.515",
                "exit_event": "MANUAL_CLOSE",
                "exit_order_id": "119",
                "exit_status": "Filled",
                "exit_price": "7.86",
                "exit_filled_qty": "665",
                "exit_filled_avg_price": "7.86",
                "exit_reason": "BROKER_FILLED_EXIT",
                "exit_filled_at": "2026-04-15T16:05:34+00:00",
            },
            paper_trade_exit_already_logged=lambda parent_order_id, exit_event: False,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            safe_insert_broker_order=lambda **kwargs: None,
            upsert_trade_lifecycle=upsert_trade_lifecycle,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
            get_open_state_for_broker=lambda broker: {
                "positions": [{"symbol": "RKLB"}],
                "orders": [],
            },
            close_position_for_broker=lambda broker, symbol: {"ok": True},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["synced_count"], 0)
        self.assertEqual(result["results"][0]["reason"], "identity_conflict")
        self.assertIn("symbol_mismatch", result["results"][0]["identity_conflict_reason"])
        self.assertEqual(captured_lifecycle, {})

    def test_ibkr_sync_rejects_client_order_id_identity_mismatch(self):
        result = execute_sync_paper_trades(
            get_open_paper_trades=lambda: [
                {
                    "timestamp_utc": "2026-04-15T14:36:45+00:00",
                    "symbol": "QS",
                    "name": "QS",
                    "mode": "secondary",
                    "side": "BUY",
                    "shares": "701",
                    "entry_price": "7.13",
                    "stop_price": "6.955",
                    "target_price": "7.48",
                    "broker_order_id": "112",
                    "broker_parent_order_id": "112",
                    "broker": "IBKR",
                }
            ],
            sync_order_by_id_for_broker=lambda broker, parent_id: {
                "status": "closed",
                "parent_order_id": "112",
                "parent_status": "Filled",
                "symbol": "QS",
                "client_order_id": "scanner-QS-LONG-70000-701",
                "entry_filled_qty": "701",
                "entry_filled_avg_price": "7.0",
                "exit_event": "MANUAL_CLOSE",
                "exit_order_id": "114",
                "exit_status": "Filled",
                "exit_price": "6.96",
                "exit_filled_qty": "701",
                "exit_filled_avg_price": "6.96",
                "exit_reason": "BROKER_FILLED_EXIT",
                "exit_filled_at": "2026-04-15T16:55:09+00:00",
            },
            paper_trade_exit_already_logged=lambda parent_order_id, exit_event: False,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            safe_insert_broker_order=lambda **kwargs: None,
            upsert_trade_lifecycle=lambda **kwargs: None,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
            get_open_state_for_broker=lambda broker: {
                "positions": [{"symbol": "QS"}],
                "orders": [],
            },
            close_position_for_broker=lambda broker, symbol: {"ok": True},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["synced_count"], 0)
        self.assertEqual(result["results"][0]["reason"], "identity_conflict")
        self.assertIn("client_order_id_mismatch", result["results"][0]["identity_conflict_reason"])

    def test_ibkr_stale_reconcile_repairs_lifecycle_when_exit_event_already_logged(self):
        captured_lifecycle = {}
        append_calls = []
        trade_event_calls = []
        broker_order_calls = []

        def upsert_trade_lifecycle(**kwargs):
            captured_lifecycle.update(kwargs)

        result = execute_sync_paper_trades(
            get_open_paper_trades=lambda: [
                {
                    "timestamp_utc": "2026-04-13T16:45:14+00:00",
                    "symbol": "SOFI",
                    "name": "SoFi",
                    "mode": "primary",
                    "side": "BUY",
                    "shares": "299",
                    "entry_price": "16.68",
                    "stop_price": "16.4945",
                    "target_price": "17.051",
                    "broker_order_id": "76",
                    "broker_parent_order_id": "76",
                    "broker": "IBKR",
                }
            ],
            sync_order_by_id_for_broker=lambda broker, parent_id: (_ for _ in ()).throw(
                RuntimeError("IBKR bridge timeout during GET /orders/76/sync after 8s")
            ),
            paper_trade_exit_already_logged=lambda parent_order_id, exit_event: True,
            append_trade_log=lambda row: append_calls.append(row),
            safe_insert_trade_event=lambda **kwargs: trade_event_calls.append(kwargs),
            safe_insert_broker_order=lambda **kwargs: broker_order_calls.append(kwargs),
            upsert_trade_lifecycle=upsert_trade_lifecycle,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
            get_open_positions_for_broker=lambda broker: [],
            close_position_for_broker=lambda broker, symbol: {"ok": True},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["synced_count"], 0)
        self.assertEqual(result["results"][0]["reason"], "bridge_timeout")
        self.assertEqual(captured_lifecycle, {})
        self.assertEqual(append_calls, [])
        self.assertEqual(trade_event_calls, [])
        self.assertEqual(broker_order_calls, [])

    def test_ibkr_sync_stops_after_batch_time_budget(self):
        with patch("services.sync_service.time.monotonic", side_effect=[0.0, 1.0, 1.0, 91.0, 91.0]):
            result = execute_sync_paper_trades(
                get_open_paper_trades=lambda: [
                    {"symbol": "PLTR", "broker_parent_order_id": "136", "broker": "IBKR"},
                    {"symbol": "SMCI", "broker_parent_order_id": "112", "broker": "IBKR"},
                ],
                sync_order_by_id_for_broker=lambda broker, parent_id: {
                    "exit_event": "TARGET_HIT",
                    "exit_price": "1.0",
                    "exit_status": "filled",
                    "exit_filled_qty": "1",
                    "exit_filled_avg_price": "1.0",
                    "exit_order_id": f"exit-{parent_id}",
                    "exit_reason": "TARGET_HIT",
                    "parent_status": "filled",
                    "exit_filled_at": "2026-04-14T14:25:02+00:00",
                    "exit_realized_pnl": "0.5",
                    "exit_realized_pnl_confirmed": True,
                },
                paper_trade_exit_already_logged=lambda parent_order_id, exit_event: False,
                append_trade_log=lambda row: None,
                safe_insert_trade_event=lambda **kwargs: None,
                safe_insert_broker_order=lambda **kwargs: None,
                upsert_trade_lifecycle=lambda **kwargs: None,
                parse_iso_utc=parse_iso_utc,
                to_float_or_none=to_float_or_none,
                get_open_positions_for_broker=lambda broker: [],
                close_position_for_broker=lambda broker, symbol: {"ok": True},
            )

        self.assertTrue(result["ok"])
        self.assertTrue(result["partial"])
        self.assertEqual(result["stopped_reason"], "ibkr_batch_time_budget_exceeded")
        self.assertEqual(result["synced_count"], 1)
        self.assertEqual(result["results"][-1]["reason"], "batch_time_budget_exceeded")

    def test_ibkr_sync_prioritizes_rows_missing_from_live_broker_state_before_time_budget_is_hit(self):
        synced_parent_ids = []

        def sync_order_by_id_for_broker(broker, parent_id):
            synced_parent_ids.append(parent_id)
            return {
                "exit_event": "TARGET_HIT",
                "exit_price": "1.0",
                "exit_status": "filled",
                "exit_filled_qty": "1",
                "exit_filled_avg_price": "1.0",
                "exit_order_id": f"exit-{parent_id}",
                "exit_reason": "TARGET_HIT",
                "parent_status": "filled",
                "exit_filled_at": "2026-04-16T14:25:02+00:00",
                "exit_realized_pnl": "0.5",
                "exit_realized_pnl_confirmed": True,
            }

        with patch("services.sync_service.time.monotonic", side_effect=[0.0, 1.0, 1.0, 91.0, 91.0]):
            result = execute_sync_paper_trades(
                get_open_paper_trades=lambda: [
                    {
                        "timestamp_utc": "2026-04-10T15:36:53+00:00",
                        "symbol": "NVDA",
                        "broker_parent_order_id": "40",
                        "broker": "IBKR",
                    },
                    {
                        "timestamp_utc": "2026-04-13T16:46:08+00:00",
                        "symbol": "QBTS",
                        "broker_parent_order_id": "88",
                        "broker": "IBKR",
                    },
                    {
                        "timestamp_utc": "2026-04-15T14:16:34+00:00",
                        "symbol": "JOBY",
                        "broker_parent_order_id": "64",
                        "broker": "IBKR",
                    },
                ],
                sync_order_by_id_for_broker=sync_order_by_id_for_broker,
                paper_trade_exit_already_logged=lambda parent_order_id, exit_event: False,
                append_trade_log=lambda row: None,
                safe_insert_trade_event=lambda **kwargs: None,
                safe_insert_broker_order=lambda **kwargs: None,
                upsert_trade_lifecycle=lambda **kwargs: None,
                parse_iso_utc=parse_iso_utc,
                to_float_or_none=to_float_or_none,
                get_open_state_for_broker=lambda broker: {
                    "positions": [
                        {"symbol": "NVDA"},
                        {"symbol": "QBTS"},
                    ],
                    "orders": [],
                },
                close_position_for_broker=lambda broker, symbol: {"ok": True},
            )

        self.assertTrue(result["ok"])
        self.assertTrue(result["partial"])
        self.assertEqual(result["synced_count"], 1)
        self.assertEqual(synced_parent_ids, ["64"])
        self.assertEqual(result["results"][0]["parent_order_id"], "64")
        self.assertTrue(result["results"][0]["synced"])
        self.assertFalse(result["results"][0]["stale_reconciled"])

    def test_ibkr_sync_respects_per_run_sync_cap(self):
        synced_parent_ids = []

        with patch.dict("os.environ", {"IBKR_SYNC_MAX_PER_RUN": "1"}, clear=False):
            result = execute_sync_paper_trades(
                get_open_paper_trades=lambda: [
                    {"symbol": "PLTR", "broker_parent_order_id": "136", "broker": "IBKR"},
                    {"symbol": "SMCI", "broker_parent_order_id": "112", "broker": "IBKR"},
                ],
                sync_order_by_id_for_broker=lambda broker, parent_id: synced_parent_ids.append(parent_id) or {
                    "parent_status": "submitted",
                },
                paper_trade_exit_already_logged=lambda parent_order_id, exit_event: False,
                append_trade_log=lambda row: None,
                safe_insert_trade_event=lambda **kwargs: None,
                safe_insert_broker_order=lambda **kwargs: None,
                upsert_trade_lifecycle=lambda **kwargs: None,
                parse_iso_utc=parse_iso_utc,
                to_float_or_none=to_float_or_none,
                get_open_positions_for_broker=lambda broker: [],
                close_position_for_broker=lambda broker, symbol: {"ok": True},
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["ibkr_sync_attempted"], 1)
        self.assertEqual(synced_parent_ids, ["112"])
        self.assertEqual(result["results"][0]["reason"], "still_open")
        self.assertEqual(result["results"][1]["reason"], "sync_cap_reached")

    def test_ibkr_sync_timeout_cooldown_skips_remaining_rows(self):
        synced_parent_ids = []

        with patch.dict(
            "os.environ",
            {
                "IBKR_SYNC_TIMEOUT_CIRCUIT_THRESHOLD": "2",
                "IBKR_SYNC_TIMEOUT_CIRCUIT_COOLDOWN_SECONDS": "300",
                "IBKR_SYNC_MAX_PER_RUN": "10",
            },
            clear=False,
        ):
            result = execute_sync_paper_trades(
                get_open_paper_trades=lambda: [
                    {"symbol": "PLTR", "broker_parent_order_id": "136", "broker": "IBKR"},
                    {"symbol": "SMCI", "broker_parent_order_id": "112", "broker": "IBKR"},
                    {"symbol": "NVDA", "broker_parent_order_id": "88", "broker": "IBKR"},
                ],
                sync_order_by_id_for_broker=lambda broker, parent_id: (
                    synced_parent_ids.append(parent_id)
                    or (_ for _ in ()).throw(
                        RuntimeError(f"IBKR bridge timeout during GET /orders/{parent_id}/sync after 8s")
                    )
                ),
                paper_trade_exit_already_logged=lambda parent_order_id, exit_event: False,
                append_trade_log=lambda row: None,
                safe_insert_trade_event=lambda **kwargs: None,
                safe_insert_broker_order=lambda **kwargs: None,
                upsert_trade_lifecycle=lambda **kwargs: None,
                parse_iso_utc=parse_iso_utc,
                to_float_or_none=to_float_or_none,
                get_open_positions_for_broker=lambda broker: [{"symbol": "PLTR"}, {"symbol": "SMCI"}, {"symbol": "NVDA"}],
                close_position_for_broker=lambda broker, symbol: {"ok": True},
            )

        self.assertTrue(result["ok"])
        self.assertEqual(synced_parent_ids, ["112", "136"])
        self.assertEqual(result["ibkr_timeout_streak"], 2)
        self.assertTrue(result["ibkr_cooldown_active"])
        self.assertEqual(result["results"][0]["reason"], "bridge_timeout")
        self.assertEqual(result["results"][1]["reason"], "bridge_timeout")
        self.assertEqual(result["results"][2]["reason"], "bridge_cooldown_active")

    def test_ibkr_sync_dedupes_duplicate_parent_order_rows_before_sync(self):
        synced_parent_ids = []
        result = execute_sync_paper_trades(
            get_open_paper_trades=lambda: [
                {
                    "id": "11",
                    "timestamp_utc": "2026-04-16T14:20:10+00:00",
                    "symbol": "PLTR",
                    "broker_parent_order_id": "136",
                    "broker": "IBKR",
                },
                {
                    "id": "12",
                    "timestamp_utc": "2026-04-16T14:21:10+00:00",
                    "symbol": "PLTR",
                    "broker_parent_order_id": "136",
                    "broker": "IBKR",
                },
            ],
            sync_order_by_id_for_broker=lambda broker, parent_id: synced_parent_ids.append(parent_id) or {
                "parent_status": "submitted",
            },
            paper_trade_exit_already_logged=lambda parent_order_id, exit_event: False,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            safe_insert_broker_order=lambda **kwargs: None,
            upsert_trade_lifecycle=lambda **kwargs: None,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
            get_open_positions_for_broker=lambda broker: [],
            close_position_for_broker=lambda broker, symbol: {"ok": True},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["raw_open_paper_trade_count"], 2)
        self.assertEqual(result["open_paper_trade_count"], 1)
        self.assertEqual(result["deduped_duplicate_open_rows"], 1)
        self.assertEqual(result["ibkr_sync_attempted"], 1)
        self.assertEqual(synced_parent_ids, ["136"])
        self.assertEqual(result["results"][0]["reason"], "duplicate_parent_order_deduped")
        self.assertEqual(result["results"][1]["reason"], "still_open")

    def test_ibkr_sync_uses_batch_prefetch_when_available(self):
        batch_calls = []
        captured_lifecycles = []

        def upsert_trade_lifecycle(**kwargs):
            captured_lifecycles.append(kwargs)

        result = execute_sync_paper_trades(
            get_open_paper_trades=lambda: [
                {
                    "timestamp_utc": "2026-04-16T14:20:10+00:00",
                    "symbol": "PLTR",
                    "name": "Palantir",
                    "mode": "core_one",
                    "side": "BUY",
                    "shares": "10",
                    "entry_price": "20.00",
                    "stop_price": "19.00",
                    "target_price": "22.00",
                    "broker_parent_order_id": "136",
                    "broker_order_id": "136",
                    "broker": "IBKR",
                },
                {
                    "timestamp_utc": "2026-04-16T14:25:10+00:00",
                    "symbol": "SMCI",
                    "name": "SMCI",
                    "mode": "core_two",
                    "side": "BUY",
                    "shares": "8",
                    "entry_price": "100.00",
                    "stop_price": "95.00",
                    "target_price": "110.00",
                    "broker_parent_order_id": "112",
                    "broker_order_id": "112",
                    "broker": "IBKR",
                },
            ],
            sync_order_by_id_for_broker=lambda broker, parent_id: (_ for _ in ()).throw(
                AssertionError("single-order sync should not run when batch prefetch is enabled")
            ),
            sync_orders_by_ids_for_broker=lambda broker, order_ids: (
                batch_calls.append((broker, list(order_ids)))
                or {
                    "136": {
                        "id": "136",
                        "status": "closed",
                        "parent_status": "Filled",
                        "symbol": "PLTR",
                        "exit_event": "TARGET_HIT",
                        "exit_order_id": "236",
                        "exit_status": "Filled",
                        "exit_price": "21.0",
                        "exit_filled_qty": "10",
                        "exit_filled_avg_price": "21.0",
                        "exit_reason": "TARGET_HIT",
                        "exit_filled_at": "2026-04-16T14:40:00+00:00",
                        "exit_realized_pnl": "10.0",
                        "exit_realized_pnl_confirmed": True,
                    },
                    "112": {
                        "id": "112",
                        "status": "closed",
                        "parent_status": "Filled",
                        "symbol": "SMCI",
                        "exit_event": "TARGET_HIT",
                        "exit_order_id": "212",
                        "exit_status": "Filled",
                        "exit_price": "108.0",
                        "exit_filled_qty": "8",
                        "exit_filled_avg_price": "108.0",
                        "exit_reason": "TARGET_HIT",
                        "exit_filled_at": "2026-04-16T14:45:00+00:00",
                        "exit_realized_pnl": "8.0",
                        "exit_realized_pnl_confirmed": True,
                    },
                }
            ),
            paper_trade_exit_already_logged=lambda parent_order_id, exit_event: False,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            safe_insert_broker_order=lambda **kwargs: None,
            upsert_trade_lifecycle=upsert_trade_lifecycle,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
            get_open_positions_for_broker=lambda broker: [],
            close_position_for_broker=lambda broker, symbol: {"ok": True},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(batch_calls, [("IBKR", ["136", "112"])])
        self.assertEqual(result["ibkr_batch_sync_prefetched"], 2)
        self.assertEqual(result["ibkr_sync_attempted"], 2)
        self.assertEqual(result["synced_count"], 2)
        self.assertEqual(len(captured_lifecycles), 2)

    def test_ibkr_batch_only_mode_skips_per_order_fallback_after_batch_failure(self):
        single_sync_calls = []

        with patch.dict("os.environ", {"IBKR_SYNC_BATCH_ONLY_MODE": "true"}, clear=False):
            result = execute_sync_paper_trades(
                get_open_paper_trades=lambda: [
                    {"symbol": "PLTR", "broker_parent_order_id": "136", "broker": "IBKR"},
                    {"symbol": "SMCI", "broker_parent_order_id": "112", "broker": "IBKR"},
                ],
                sync_order_by_id_for_broker=lambda broker, parent_id: (
                    single_sync_calls.append((broker, parent_id))
                    or {"status": "filled"}
                ),
                sync_orders_by_ids_for_broker=lambda broker, order_ids: (_ for _ in ()).throw(
                    RuntimeError("batch bridge timeout")
                ),
                paper_trade_exit_already_logged=lambda parent_order_id, exit_event: False,
                append_trade_log=lambda row: None,
                safe_insert_trade_event=lambda **kwargs: None,
                safe_insert_broker_order=lambda **kwargs: None,
                upsert_trade_lifecycle=lambda **kwargs: None,
                parse_iso_utc=parse_iso_utc,
                to_float_or_none=to_float_or_none,
                get_open_state_for_broker=lambda broker: {
                    "positions": [
                        {"symbol": "PLTR"},
                        {"symbol": "SMCI"},
                    ],
                    "orders": [],
                },
                close_position_for_broker=lambda broker, symbol: {"ok": True},
            )

        self.assertTrue(result["ok"])
        self.assertEqual(single_sync_calls, [])
        self.assertTrue(result["ibkr_batch_only_mode"])
        self.assertEqual(result["ibkr_sync_attempted"], 2)
        self.assertEqual(result["ibkr_batch_sync_prefetched"], 2)
        self.assertEqual(result["results"][0]["reason"], "still_open")
        self.assertEqual(result["results"][1]["reason"], "still_open")

    def test_still_open_sync_refreshes_open_lifecycle_row(self):
        captured_lifecycles = []

        def upsert_trade_lifecycle(**kwargs):
            captured_lifecycles.append(kwargs)

        result = execute_sync_paper_trades(
            get_open_paper_trades=lambda: [
                {
                    "timestamp_utc": "2026-04-18T13:20:10+00:00",
                    "symbol": "PLTR",
                    "name": "Palantir",
                    "mode": "core_one",
                    "side": "BUY",
                    "shares": "10",
                    "entry_price": "20.00",
                    "stop_price": "19.50",
                    "target_price": "22.00",
                    "broker_parent_order_id": "136",
                    "broker_order_id": "136",
                    "broker": "IBKR",
                },
            ],
            sync_order_by_id_for_broker=lambda broker, parent_id: {
                "id": parent_id,
                "status": "submitted",
                "parent_status": "Submitted",
                "symbol": "PLTR",
            },
            paper_trade_exit_already_logged=lambda parent_order_id, exit_event: False,
            append_trade_log=lambda row: None,
            safe_insert_trade_event=lambda **kwargs: None,
            safe_insert_broker_order=lambda **kwargs: None,
            upsert_trade_lifecycle=upsert_trade_lifecycle,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
            get_open_positions_for_broker=lambda broker: [{"symbol": "PLTR"}],
            close_position_for_broker=lambda broker, symbol: {"ok": True},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["synced_count"], 0)
        self.assertEqual(result["results"][0]["reason"], "still_open")
        self.assertTrue(result["results"][0]["lifecycle_refreshed"])
        self.assertEqual(len(captured_lifecycles), 1)
        self.assertEqual(captured_lifecycles[0]["status"], "OPEN")
        self.assertEqual(captured_lifecycles[0]["symbol"], "PLTR")
        self.assertEqual(captured_lifecycles[0]["parent_order_id"], "136")
        self.assertEqual(captured_lifecycles[0]["exit_reason"], "")


if __name__ == "__main__":
    unittest.main()
