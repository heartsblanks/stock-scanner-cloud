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


if __name__ == "__main__":
    unittest.main()
