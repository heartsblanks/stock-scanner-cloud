import unittest

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
    def __init__(self, symbol):
        self.symbol = symbol


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


if __name__ == "__main__":
    unittest.main()
