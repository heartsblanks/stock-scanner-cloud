from datetime import UTC, datetime
import unittest

from orchestration.scan_context import parse_iso_utc, to_float_or_none
from services.ibkr_repair_service import repair_ibkr_stale_closes


class IbkrRepairServiceTests(unittest.TestCase):
    def test_prefers_trade_event_before_live_sync(self):
        captured_lifecycle: dict = {}
        sync_called = False

        def failing_sync(_broker, _parent_order_id):
            nonlocal sync_called
            sync_called = True
            raise AssertionError("live sync should not be called when trade event data exists")

        result = repair_ibkr_stale_closes(
            target_date="2026-04-10",
            get_stale_ibkr_closed_trade_lifecycles=lambda **kwargs: [
                {
                    "trade_key": "88",
                    "symbol": "QBTS",
                    "mode": "fourth",
                    "side": "BUY",
                    "direction": "LONG",
                    "status": "CLOSED",
                    "entry_time": datetime(2026, 4, 10, 15, 36, 0, tzinfo=UTC),
                    "entry_price": "14.49",
                    "exit_time": None,
                    "exit_price": None,
                    "stop_price": "14.266",
                    "target_price": "14.938",
                    "exit_reason": "STALE_OPEN_RECONCILED",
                    "shares": "345",
                    "signal_timestamp": None,
                    "signal_entry": None,
                    "signal_stop": None,
                    "signal_target": None,
                    "signal_confidence": None,
                    "order_id": "88",
                    "parent_order_id": "88",
                    "exit_order_id": "",
                }
            ],
            sync_order_by_id_for_broker=failing_sync,
            get_latest_exit_trade_event_for_parent_order_id=lambda parent_order_id, broker=None: {
                "event_type": "STOP_HIT",
                "event_time": datetime(2026, 4, 10, 16, 1, 9, tzinfo=UTC),
                "price": "14.27",
                "status": "CLOSED",
                "order_id": "90",
            },
            upsert_trade_lifecycle=lambda **kwargs: captured_lifecycle.update(kwargs),
            safe_insert_broker_order=lambda **kwargs: None,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["repaired_count"], 1)
        self.assertFalse(sync_called)
        self.assertEqual(captured_lifecycle["exit_reason"], "STOP_HIT")
        self.assertEqual(captured_lifecycle["exit_order_id"], "90")

    def test_repairs_stale_rows_from_live_sync_results(self):
        captured_lifecycle: dict = {}
        captured_broker_orders: list[dict] = []

        result = repair_ibkr_stale_closes(
            target_date="2026-04-10",
            get_stale_ibkr_closed_trade_lifecycles=lambda **kwargs: [
                {
                    "trade_key": "34",
                    "symbol": "RIVN",
                    "mode": "primary",
                    "side": "BUY",
                    "direction": "LONG",
                    "status": "CLOSED",
                    "entry_time": datetime(2026, 4, 10, 15, 35, 13, tzinfo=UTC),
                    "entry_price": "15.71",
                    "exit_time": datetime(2026, 4, 10, 16, 5, 26, tzinfo=UTC),
                    "exit_price": "15.71",
                    "stop_price": "15.493",
                    "target_price": "16.144",
                    "exit_reason": "STALE_OPEN_RECONCILED",
                    "shares": "317",
                    "signal_timestamp": None,
                    "signal_entry": None,
                    "signal_stop": None,
                    "signal_target": None,
                    "signal_confidence": None,
                    "order_id": "34",
                    "parent_order_id": "34",
                }
            ],
            sync_order_by_id_for_broker=lambda broker, parent_order_id: {
                "exit_order_id": "36",
                "exit_price": 15.47,
                "exit_status": "Filled",
                "exit_filled_qty": 317,
                "exit_filled_avg_price": 15.47,
                "exit_filled_at": "2026-04-10T16:01:09+00:00",
                "exit_reason": "BROKER_FILLED_EXIT",
            },
            get_latest_exit_trade_event_for_parent_order_id=lambda parent_order_id, broker=None: None,
            upsert_trade_lifecycle=lambda **kwargs: captured_lifecycle.update(kwargs),
            safe_insert_broker_order=lambda **kwargs: captured_broker_orders.append(kwargs),
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["repaired_count"], 1)
        self.assertEqual(captured_lifecycle["exit_order_id"], "36")
        self.assertEqual(captured_lifecycle["exit_price"], 15.47)
        self.assertEqual(captured_lifecycle["exit_reason"], "BROKER_FILLED_EXIT_REPAIRED")
        self.assertAlmostEqual(captured_lifecycle["realized_pnl"], -76.08, places=2)
        self.assertEqual(captured_broker_orders[0]["order_id"], "36")

    def test_skips_rows_when_live_sync_cannot_provide_exit_data(self):
        result = repair_ibkr_stale_closes(
            target_date="2026-04-10",
            get_stale_ibkr_closed_trade_lifecycles=lambda **kwargs: [
                {
                    "trade_key": "10",
                    "symbol": "NIO",
                    "side": "SELL",
                    "direction": "SHORT",
                    "status": "CLOSED",
                    "entry_time": datetime(2026, 4, 9, 14, 25, 20, tzinfo=UTC),
                    "entry_price": "6.2",
                    "shares": "1612",
                    "order_id": "10",
                    "parent_order_id": "10",
                }
            ],
            sync_order_by_id_for_broker=lambda broker, parent_order_id: {"status": "unknown"},
            get_latest_exit_trade_event_for_parent_order_id=lambda parent_order_id, broker=None: None,
            upsert_trade_lifecycle=lambda **kwargs: None,
            safe_insert_broker_order=lambda **kwargs: None,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["repaired_count"], 0)
        self.assertEqual(result["skipped_count"], 1)
        self.assertEqual(result["results"][0]["reason"], "repair_data_unavailable")

    def test_repairs_stale_rows_from_trade_events_when_broker_sync_no_longer_has_fill(self):
        captured_lifecycle: dict = {}

        result = repair_ibkr_stale_closes(
            target_date="2026-04-10",
            get_stale_ibkr_closed_trade_lifecycles=lambda **kwargs: [
                {
                    "trade_key": "88",
                    "symbol": "QBTS",
                    "mode": "fourth",
                    "side": "BUY",
                    "direction": "LONG",
                    "status": "CLOSED",
                    "entry_time": datetime(2026, 4, 10, 15, 36, 0, tzinfo=UTC),
                    "entry_price": "14.49",
                    "exit_time": None,
                    "exit_price": None,
                    "stop_price": "14.266",
                    "target_price": "14.938",
                    "exit_reason": "STALE_OPEN_RECONCILED",
                    "shares": "345",
                    "signal_timestamp": None,
                    "signal_entry": None,
                    "signal_stop": None,
                    "signal_target": None,
                    "signal_confidence": None,
                    "order_id": "88",
                    "parent_order_id": "88",
                    "exit_order_id": "",
                }
            ],
            sync_order_by_id_for_broker=lambda broker, parent_order_id: {"status": "unknown"},
            get_latest_exit_trade_event_for_parent_order_id=lambda parent_order_id, broker=None: {
                "event_type": "STOP_HIT",
                "event_time": datetime(2026, 4, 10, 16, 1, 9, tzinfo=UTC),
                "price": "14.27",
                "status": "CLOSED",
                "order_id": "90",
            },
            upsert_trade_lifecycle=lambda **kwargs: captured_lifecycle.update(kwargs),
            safe_insert_broker_order=lambda **kwargs: None,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["repaired_count"], 1)
        self.assertEqual(captured_lifecycle["exit_order_id"], "90")
        self.assertEqual(captured_lifecycle["exit_reason"], "STOP_HIT")
        self.assertAlmostEqual(captured_lifecycle["exit_price"], 14.27, places=2)
        self.assertIsNotNone(captured_lifecycle["realized_pnl"])

    def test_stops_when_repair_time_budget_is_exceeded(self):
        current_times = iter([0.0, 31.0])

        result = repair_ibkr_stale_closes(
            target_date="2026-04-10",
            get_stale_ibkr_closed_trade_lifecycles=lambda **kwargs: [
                {
                    "trade_key": "34",
                    "symbol": "RIVN",
                    "order_id": "34",
                    "parent_order_id": "34",
                }
            ],
            sync_order_by_id_for_broker=lambda broker, parent_order_id: {
                "exit_order_id": "36",
                "exit_price": 15.47,
                "exit_status": "Filled",
                "exit_filled_qty": 317,
                "exit_filled_avg_price": 15.47,
                "exit_filled_at": "2026-04-10T16:01:09+00:00",
                "exit_reason": "BROKER_FILLED_EXIT",
            },
            get_latest_exit_trade_event_for_parent_order_id=lambda parent_order_id, broker=None: None,
            upsert_trade_lifecycle=lambda **kwargs: None,
            safe_insert_broker_order=lambda **kwargs: None,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
            max_duration_seconds=30.0,
            current_time_fn=lambda: next(current_times),
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["repaired_count"], 0)
        self.assertEqual(result["skipped_count"], 1)
        self.assertEqual(result["results"][0]["reason"], "repair_time_budget_exceeded")

    def test_remaps_pending_fill_sync_reason_when_repairing_from_existing_lifecycle(self):
        captured_lifecycle: dict = {}
        sync_called = False

        def should_not_sync(_broker, _parent_order_id):
            nonlocal sync_called
            sync_called = True
            raise AssertionError("live sync should not run when existing lifecycle has sufficient exit data")

        result = repair_ibkr_stale_closes(
            target_date="2026-04-17",
            get_stale_ibkr_closed_trade_lifecycles=lambda **kwargs: [
                {
                    "trade_key": "202",
                    "symbol": "INTC",
                    "mode": "core_three",
                    "side": "BUY",
                    "direction": "LONG",
                    "status": "CLOSED",
                    "entry_time": datetime(2026, 4, 16, 16, 17, 32, tzinfo=UTC),
                    "entry_price": "67.76",
                    "exit_time": datetime(2026, 4, 17, 19, 40, 0, tzinfo=UTC),
                    "exit_price": "66.90",
                    "stop_price": "66.34",
                    "target_price": "70.60",
                    "exit_reason": "BROKER_POSITION_FLAT_PENDING_FILL_SYNC",
                    "shares": "146",
                    "signal_timestamp": None,
                    "signal_entry": None,
                    "signal_stop": None,
                    "signal_target": None,
                    "signal_confidence": None,
                    "order_id": "202",
                    "parent_order_id": "202",
                    "exit_order_id": "",
                }
            ],
            sync_order_by_id_for_broker=should_not_sync,
            get_latest_exit_trade_event_for_parent_order_id=lambda parent_order_id, broker=None: None,
            upsert_trade_lifecycle=lambda **kwargs: captured_lifecycle.update(kwargs),
            safe_insert_broker_order=lambda **kwargs: None,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["repaired_count"], 1)
        self.assertFalse(sync_called)
        self.assertEqual(captured_lifecycle["exit_reason"], "BROKER_FILLED_EXIT_REPAIRED")
        self.assertAlmostEqual(captured_lifecycle["exit_price"], 66.9, places=2)
        self.assertLess(captured_lifecycle["realized_pnl"], 0)

    def test_marks_manual_close_without_fill_as_terminal_unverified(self):
        captured_lifecycle: dict = {}
        captured_broker_orders: list[dict] = []

        result = repair_ibkr_stale_closes(
            target_date="2026-04-17",
            get_stale_ibkr_closed_trade_lifecycles=lambda **kwargs: [
                {
                    "trade_key": "IBKR:AFRM:1033",
                    "symbol": "AFRM",
                    "mode": "core_two",
                    "side": "BUY",
                    "direction": "LONG",
                    "status": "CLOSED",
                    "entry_time": datetime(2026, 4, 17, 18, 30, 0, tzinfo=UTC),
                    "entry_price": "64.53",
                    "exit_time": datetime(2026, 4, 17, 18, 40, 28, tzinfo=UTC),
                    "exit_price": "64.53",
                    "stop_price": "63.88",
                    "target_price": "65.82",
                    "exit_reason": "MANUAL_CLOSE",
                    "shares": "150",
                    "signal_timestamp": None,
                    "signal_entry": None,
                    "signal_stop": None,
                    "signal_target": None,
                    "signal_confidence": None,
                    "order_id": "1033",
                    "parent_order_id": "1033",
                    "exit_order_id": "1033",
                }
            ],
            sync_order_by_id_for_broker=lambda broker, parent_order_id: {
                "exit_order_id": "1033",
                "exit_price": 64.53,
                "exit_time": "2026-04-17T18:40:28+00:00",
                "exit_reason": "MANUAL_CLOSE",
                "exit_status": "Filled",
            },
            get_latest_exit_trade_event_for_parent_order_id=lambda parent_order_id, broker=None: None,
            upsert_trade_lifecycle=lambda **kwargs: captured_lifecycle.update(kwargs),
            safe_insert_broker_order=lambda **kwargs: captured_broker_orders.append(kwargs),
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["repaired_count"], 1)
        self.assertEqual(captured_lifecycle["exit_reason"], "BROKER_CLOSE_UNVERIFIED_NO_FILL_DATA")
        self.assertEqual(len(captured_broker_orders), 0)

    def test_remaps_breakeven_manual_close_trade_event_to_terminal_unverified(self):
        captured_lifecycle: dict = {}
        sync_called = False

        def should_not_sync(_broker, _parent_order_id):
            nonlocal sync_called
            sync_called = True
            raise AssertionError("live sync should not run when trade event data exists")

        result = repair_ibkr_stale_closes(
            target_date="2026-04-17",
            get_stale_ibkr_closed_trade_lifecycles=lambda **kwargs: [
                {
                    "trade_key": "IBKR:TSLA:1111",
                    "symbol": "TSLA",
                    "mode": "core_two",
                    "side": "BUY",
                    "direction": "LONG",
                    "status": "CLOSED",
                    "entry_time": datetime(2026, 4, 17, 18, 30, 0, tzinfo=UTC),
                    "entry_price": "401.68",
                    "exit_time": datetime(2026, 4, 17, 18, 40, 0, tzinfo=UTC),
                    "exit_price": "401.68",
                    "stop_price": "397.66",
                    "target_price": "409.71",
                    "exit_reason": "MANUAL_CLOSE",
                    "shares": "24",
                    "order_id": "1111",
                    "parent_order_id": "1111",
                    "exit_order_id": "1111",
                }
            ],
            sync_order_by_id_for_broker=should_not_sync,
            get_latest_exit_trade_event_for_parent_order_id=lambda parent_order_id, broker=None: {
                "event_type": "MANUAL_CLOSE",
                "event_time": datetime(2026, 4, 17, 18, 40, 0, tzinfo=UTC),
                "price": "401.68",
                "status": "CLOSED",
                "order_id": "1111",
            },
            upsert_trade_lifecycle=lambda **kwargs: captured_lifecycle.update(kwargs),
            safe_insert_broker_order=lambda **kwargs: None,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["repaired_count"], 1)
        self.assertFalse(sync_called)
        self.assertEqual(captured_lifecycle["exit_reason"], "BROKER_CLOSE_UNVERIFIED_NO_FILL_DATA")


if __name__ == "__main__":
    unittest.main()
