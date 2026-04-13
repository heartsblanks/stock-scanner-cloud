import os
import unittest
from unittest.mock import patch

from ibkr_bridge.connector import IbkrGatewayClient


class _FakeOrder:
    def __init__(self, *, order_id, order_ref, action, qty, order_type, parent_id=0, limit_price=0.0, stop_price=0.0):
        self.orderId = order_id
        self.orderRef = order_ref
        self.action = action
        self.totalQuantity = qty
        self.orderType = order_type
        self.parentId = parent_id
        self.lmtPrice = limit_price
        self.auxPrice = stop_price


class _FakeStatus:
    def __init__(self, *, status, filled=0.0, remaining=0.0):
        self.status = status
        self.filled = filled
        self.remaining = remaining


class _FakeContract:
    def __init__(self, symbol, con_id=1):
        self.symbol = symbol
        self.conId = con_id
        self.exchange = "SMART"
        self.currency = "USD"


class _FakeTrade:
    def __init__(self, *, symbol, order_id, status, remaining, action="BUY", order_type="MKT", order_ref="ref"):
        self.contract = _FakeContract(symbol)
        self.order = _FakeOrder(
            order_id=order_id,
            order_ref=order_ref,
            action=action,
            qty=1,
            order_type=order_type,
        )
        self.orderStatus = _FakeStatus(status=status, remaining=remaining)


class _FakeExecution:
    def __init__(self, *, order_id, order_ref, price, shares, avg_price=None, side="BOT", time="2026-04-10T15:37:03+00:00"):
        self.orderId = order_id
        self.orderRef = order_ref
        self.price = price
        self.shares = shares
        self.avgPrice = avg_price if avg_price is not None else price
        self.side = side
        self.time = time


class _FakeFill:
    def __init__(self, *, symbol, execution):
        self.contract = _FakeContract(symbol)
        self.execution = execution


class _FakePositionRow:
    def __init__(self, *, symbol, qty, avg_cost, account="DU123", con_id=1):
        self.account = account
        self.contract = _FakeContract(symbol, con_id=con_id)
        self.position = qty
        self.avgCost = avg_cost


class _FakeMarketOrder:
    def __init__(self, action, qty):
        self.action = action
        self.totalQuantity = qty
        self.orderRef = ""


class _FakeCloseTrade:
    def __init__(self, *, symbol, order_id, status, filled=0.0, remaining=0.0, avg_fill_price=0.0):
        self.contract = _FakeContract(symbol)
        self.order = _FakeOrder(
            order_id=order_id,
            order_ref=f"scanner-close-{symbol}",
            action="SELL",
            qty=remaining or filled or 1,
            order_type="MKT",
        )
        self.orderStatus = _FakeStatus(status=status, filled=filled, remaining=remaining)
        self.fills = []
        self.log = []
        self.advancedError = ""


class _FakeIbForPositions:
    def __init__(self, positions):
        self._positions = positions
        self.req_tickers_calls = 0

    def managedAccounts(self):
        return ["DU123"]

    def positions(self):
        return self._positions

    def reqTickers(self, *contracts):
        self.req_tickers_calls += 1
        return []


class IbkrConnectorTests(unittest.TestCase):
    def test_get_open_orders_filters_cancelled_rows(self):
        client = IbkrGatewayClient.__new__(IbkrGatewayClient)
        client._connect = lambda: object()
        client._fetch_open_trades = lambda ib: [
            _FakeTrade(symbol="NIO", order_id=64, status="PreSubmitted", remaining=1.0),
            _FakeTrade(symbol="NIO", order_id=65, status="Cancelled", remaining=0.0, action="SELL", order_type="LMT"),
        ]

        result = client.get_open_orders()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], "64")
        self.assertEqual(result[0]["status"], "PreSubmitted")

    def test_get_order_keeps_non_open_status_for_direct_lookup(self):
        client = IbkrGatewayClient.__new__(IbkrGatewayClient)
        client._connect = lambda: object()
        client._fetch_open_trades = lambda ib: [
            _FakeTrade(symbol="NIO", order_id=64, status="Cancelled", remaining=0.0),
        ]

        result = client.get_order("64")

        self.assertIsNotNone(result)
        self.assertEqual(result["status"], "Cancelled")
        self.assertEqual(result["id"], "64")

    def test_sync_order_uses_completed_execution_fills_when_order_is_no_longer_open(self):
        client = IbkrGatewayClient.__new__(IbkrGatewayClient)
        client._connect = lambda: object()
        client._fetch_open_trades = lambda ib: []
        client._fetch_recent_fills = lambda ib: [
            _FakeFill(
                symbol="RIVN",
                execution=_FakeExecution(order_id=34, order_ref="scanner-RIVN-BUY-157100-317", price=15.71, shares=317, side="BOT", time="2026-04-10T15:35:13+00:00"),
            ),
            _FakeFill(
                symbol="RIVN",
                execution=_FakeExecution(order_id=36, order_ref="scanner-RIVN-BUY-157100-317", price=15.47, shares=317, side="SLD", time="2026-04-10T16:01:09+00:00"),
            ),
        ]

        result = client.sync_order("34")

        self.assertEqual(result["parent_order_id"], "34")
        self.assertEqual(result["exit_order_id"], "36")
        self.assertEqual(result["exit_price"], 15.47)
        self.assertEqual(result["exit_reason"], "BROKER_FILLED_EXIT")

    def test_get_positions_skips_ticker_enrichment_by_default(self):
        fake_ib = _FakeIbForPositions([
            _FakePositionRow(symbol="NVDA", qty=52, avg_cost=189.60923075, con_id=4815747),
        ])
        client = IbkrGatewayClient.__new__(IbkrGatewayClient)
        client._connect = lambda: fake_ib
        client.config = type("Cfg", (), {"account_id": "", "host": "127.0.0.1", "port": 4002, "client_id": 101, "readonly": False})()

        with patch.dict(os.environ, {}, clear=False):
            result = client.get_positions()

        self.assertEqual(fake_ib.req_tickers_calls, 0)
        self.assertEqual(result[0]["symbol"], "NVDA")
        self.assertEqual(result[0]["current_price"], 189.6092)

    def test_close_position_marks_unresolved_when_position_stays_open(self):
        client = IbkrGatewayClient.__new__(IbkrGatewayClient)
        fake_position = _FakePositionRow(symbol="NVDA", qty=52, avg_cost=189.60923075, con_id=4815747)
        fake_trade = _FakeCloseTrade(symbol="NVDA", order_id=65, status="Submitted", remaining=52.0)

        class _FakeIb:
            def qualifyContracts(self, contract):
                return [contract]

            def placeOrder(self, contract, order):
                return fake_trade

        client._connect = lambda: _FakeIb()
        client._find_position_row = lambda ib, symbol: fake_position
        client._load_order_classes = lambda: (None, _FakeMarketOrder, None, _FakeContract)
        client._normalize_trade = lambda trade: {"id": "65", "status": "Submitted"}
        client._close_poll_config = lambda: (1, 0.0)
        client._order_status_snapshot = lambda trade: {
            "status": "Submitted",
            "filled_qty": 0.0,
            "avg_fill_price": 0.0,
            "filled_at": "",
        }
        client._position_is_open = lambda ib, symbol: True

        result = client.close_position("NVDA")

        self.assertTrue(result["attempted"])
        self.assertTrue(result["placed"])
        self.assertTrue(result["close_failed"])
        self.assertFalse(result["position_closed"])
        self.assertEqual(result["reason"], "broker_close_not_confirmed")
        self.assertEqual(result["status"], "Submitted")


if __name__ == "__main__":
    unittest.main()
