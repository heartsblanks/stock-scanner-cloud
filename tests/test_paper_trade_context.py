import unittest
import sys
from types import ModuleType
from unittest.mock import patch

if "requests" not in sys.modules:
    sys.modules["requests"] = ModuleType("requests")
if "psycopg" not in sys.modules:
    fake_psycopg = ModuleType("psycopg")
    fake_psycopg_rows = ModuleType("psycopg.rows")
    fake_psycopg_rows.dict_row = object()
    fake_psycopg.rows = fake_psycopg_rows
    sys.modules["psycopg"] = fake_psycopg
    sys.modules["psycopg.rows"] = fake_psycopg_rows

from orchestration.paper_trade_context import get_managed_open_paper_trades_for_eod_close


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


if __name__ == "__main__":
    unittest.main()
