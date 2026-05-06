import unittest
import sys
from types import ModuleType
from unittest.mock import patch

if "psycopg" not in sys.modules:
    fake_psycopg = ModuleType("psycopg")
    fake_psycopg_rows = ModuleType("psycopg.rows")
    fake_psycopg_rows.dict_row = object()
    fake_psycopg.rows = fake_psycopg_rows
    sys.modules["psycopg"] = fake_psycopg
    sys.modules["psycopg.rows"] = fake_psycopg_rows

from orchestration.paper_trade_context import get_managed_open_paper_trades_for_eod_close, is_symbol_in_paper_cooldown


class _StubBroker:
    name = "IBKR"

    def __init__(self, positions, orders):
        self._positions = positions
        self._orders = orders

    def get_open_positions(self):
        return self._positions

    def get_open_orders(self):
        return self._orders


class PaperTradeContextTests(unittest.TestCase):
    def test_returns_open_rows_when_broker_snapshot_is_empty(self):
        open_rows = [
            {"symbol": "AAPL", "broker_parent_order_id": "101", "broker_order_id": "101"},
            {"symbol": "MSFT", "broker_parent_order_id": "202", "broker_order_id": "202"},
        ]

        with patch("orchestration.paper_trade_context.get_open_paper_trades_for_broker", return_value=open_rows):
            rows = get_managed_open_paper_trades_for_eod_close(
                broker=_StubBroker(positions=[], orders=[])
            )

        self.assertEqual(rows, open_rows)

    def test_filters_rows_against_live_positions_and_orders_when_available(self):
        open_rows = [
            {"symbol": "AAPL", "broker_parent_order_id": "101", "broker_order_id": "101"},
            {"symbol": "MSFT", "broker_parent_order_id": "202", "broker_order_id": "202"},
        ]

        with patch("orchestration.paper_trade_context.get_open_paper_trades_for_broker", return_value=open_rows):
            rows = get_managed_open_paper_trades_for_eod_close(
                broker=_StubBroker(
                    positions=[{"symbol": "AAPL"}],
                    orders=[{"id": "303", "legs": [{"id": "404"}]}],
                )
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "AAPL")

    def test_broker_filled_losing_exit_blocks_symbol_for_rest_of_day(self):
        with patch(
            "orchestration.paper_trade_context.get_recent_closed_trade_lifecycle_for_symbol",
            return_value={
                "symbol": "RIVN",
                "status": "CLOSED",
                "exit_reason": "BROKER_FILLED_EXIT",
                "exit_time": "2026-05-06T14:30:00+00:00",
                "realized_pnl": -3.55,
            },
        ):
            blocked, reason = is_symbol_in_paper_cooldown("RIVN", "2026-05-06T15:30:00+00:00")

        self.assertTrue(blocked)
        self.assertIn("loss_rest_of_day", reason)


if __name__ == "__main__":
    unittest.main()
