import unittest
from unittest.mock import Mock

from orchestration.scan_context import parse_iso_utc, to_float_or_none
from services.sync_service import execute_sync_paper_trades


class SyncServiceTests(unittest.TestCase):
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
        self.assertAlmostEqual(captured_lifecycle["realized_pnl"], -25.44, places=2)
        self.assertAlmostEqual(captured_lifecycle["realized_pnl_percent"], -0.428266, places=6)


if __name__ == "__main__":
    unittest.main()
