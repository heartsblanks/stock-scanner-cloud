from datetime import UTC, datetime
import unittest

from orchestration.scan_context import parse_iso_utc, to_float_or_none
from services.ibkr_repair_service import repair_ibkr_stale_closes


class IbkrRepairServiceTests(unittest.TestCase):
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
            upsert_trade_lifecycle=lambda **kwargs: captured_lifecycle.update(kwargs),
            safe_insert_broker_order=lambda **kwargs: captured_broker_orders.append(kwargs),
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["repaired_count"], 1)
        self.assertEqual(captured_lifecycle["exit_order_id"], "36")
        self.assertEqual(captured_lifecycle["exit_price"], 15.47)
        self.assertEqual(captured_lifecycle["exit_reason"], "BROKER_FILLED_EXIT")
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
            upsert_trade_lifecycle=lambda **kwargs: None,
            safe_insert_broker_order=lambda **kwargs: None,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["repaired_count"], 0)
        self.assertEqual(result["skipped_count"], 1)
        self.assertEqual(result["results"][0]["reason"], "repair_data_unavailable")


if __name__ == "__main__":
    unittest.main()
