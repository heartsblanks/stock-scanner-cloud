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
        self.fills = []


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


class _FakeIbForCloseFlow:
    def __init__(self, *, open_trades_sequence, close_trade):
        self._open_trades_sequence = list(open_trades_sequence)
        self._last_open_trades = self._open_trades_sequence[-1] if self._open_trades_sequence else []
        self._close_trade = close_trade
        self.cancelled_order_ids = []
        self.placed_orders = []

    def _current_open_trades(self):
        if self._open_trades_sequence:
            self._last_open_trades = self._open_trades_sequence.pop(0)
        return self._last_open_trades

    def openTrades(self):
        return self._current_open_trades()

    def reqOpenOrders(self):
        return self._current_open_trades()

    def cancelOrder(self, order):
        self.cancelled_order_ids.append(str(getattr(order, "orderId", "")))

    def placeOrder(self, contract, order):
        self.placed_orders.append({"symbol": getattr(contract, "symbol", ""), "action": getattr(order, "action", ""), "qty": getattr(order, "totalQuantity", 0)})
        return self._close_trade

    def sleep(self, _seconds):
        return None

    def qualifyContracts(self, contract):
        return [contract]


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

    def test_sync_order_prefers_execution_fills_before_open_trade_lookup(self):
        client = IbkrGatewayClient.__new__(IbkrGatewayClient)
        client._connect = lambda: object()
        open_trade_calls = []

        def fake_fetch_open_trades(ib):
            open_trade_calls.append(True)
            return [
                _FakeTrade(symbol="RIVN", order_id=34, status="Submitted", remaining=317.0),
            ]

        client._fetch_open_trades = fake_fetch_open_trades
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

        self.assertEqual(result["exit_order_id"], "36")
        self.assertEqual(open_trade_calls, [])

    def test_sync_order_falls_back_to_open_trade_lookup_when_fills_are_unknown(self):
        client = IbkrGatewayClient.__new__(IbkrGatewayClient)
        client._connect = lambda: object()
        client._fetch_open_trades = lambda ib: [
            _FakeTrade(symbol="NIO", order_id=64, status="Submitted", remaining=1.0),
        ]
        client._fetch_recent_fills = lambda ib: []
        client._fetch_completed_trades = lambda ib: []

        result = client.sync_order("64")

        self.assertEqual(result["id"], "64")
        self.assertEqual(result["status"], "Submitted")
        self.assertEqual(result["parent_status"], "Submitted")

    def test_sync_order_uses_completed_trades_when_recent_fills_are_unavailable(self):
        client = IbkrGatewayClient.__new__(IbkrGatewayClient)
        client._connect = lambda: object()
        client._fetch_open_trades = lambda ib: []
        client._fetch_recent_fills = lambda ib: []

        entry_trade = _FakeTrade(
            symbol="SOFI",
            order_id=76,
            status="Filled",
            remaining=0.0,
            action="BUY",
            order_ref="scanner-SOFI-BUY-166800-299",
        )
        entry_trade.orderStatus.filled = 299
        entry_trade.orderStatus.avgFillPrice = 16.68
        entry_trade.fills = [
            _FakeFill(
                symbol="SOFI",
                execution=_FakeExecution(
                    order_id=76,
                    order_ref="scanner-SOFI-BUY-166800-299",
                    price=16.68,
                    shares=299,
                    side="BOT",
                    time="2026-04-13T16:45:14+00:00",
                ),
            )
        ]

        exit_trade = _FakeTrade(
            symbol="SOFI",
            order_id=78,
            status="Filled",
            remaining=0.0,
            action="SELL",
            order_ref="scanner-SOFI-BUY-166800-299",
        )
        exit_trade.orderStatus.filled = 299
        exit_trade.orderStatus.avgFillPrice = 16.52
        exit_trade.fills = [
            _FakeFill(
                symbol="SOFI",
                execution=_FakeExecution(
                    order_id=78,
                    order_ref="scanner-SOFI-BUY-166800-299",
                    price=16.52,
                    shares=299,
                    side="SLD",
                    time="2026-04-13T19:55:00+00:00",
                ),
            )
        ]

        client._fetch_completed_trades = lambda ib: [entry_trade, exit_trade]

        result = client.sync_order("76")

        self.assertEqual(result["parent_order_id"], "76")
        self.assertEqual(result["exit_order_id"], "78")
        self.assertEqual(result["exit_price"], 16.52)
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
            def openTrades(self):
                return []

            def qualifyContracts(self, contract):
                return [contract]

            def cancelOrder(self, order):
                return None

            def placeOrder(self, contract, order):
                return fake_trade

            def sleep(self, _seconds):
                return None

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
        self.assertEqual(result["canceled_order_ids"], [])

    def test_close_position_waits_for_symbol_open_orders_to_clear_before_submitting_close(self):
        client = IbkrGatewayClient.__new__(IbkrGatewayClient)
        fake_position = _FakePositionRow(symbol="NVDA", qty=52, avg_cost=189.60923075, con_id=4815747)
        child_trade = _FakeTrade(symbol="NVDA", order_id=41, status="Submitted", remaining=52.0, action="SELL", order_type="STP")
        fake_close_trade = _FakeCloseTrade(symbol="NVDA", order_id=65, status="Submitted", remaining=52.0)
        fake_ib = _FakeIbForCloseFlow(
            open_trades_sequence=[
                [child_trade],
                [child_trade],
                [],
            ],
            close_trade=fake_close_trade,
        )

        client._connect = lambda: fake_ib
        client._find_position_row = lambda ib, symbol: fake_position
        client._load_order_classes = lambda: (None, _FakeMarketOrder, None, _FakeContract)
        client._normalize_trade = lambda trade: {"id": "65", "status": "Submitted"}
        client._close_poll_config = lambda: (1, 0.0)
        client._cancel_settle_config = lambda: (2, 0.0)
        client._order_status_snapshot = lambda trade: {
            "order_id": "65",
            "status": "Submitted",
            "filled_qty": 0.0,
            "avg_fill_price": 0.0,
            "filled_at": "",
        }
        client._position_is_open = lambda ib, symbol: True

        result = client.close_position("NVDA")

        self.assertEqual(fake_ib.cancelled_order_ids, ["41"])
        self.assertEqual(fake_ib.placed_orders[0]["symbol"], "NVDA")
        self.assertEqual(result["cancel_settle_transitions"][0]["open_order_count"], 1)
        self.assertEqual(result["cancel_settle_transitions"][-1]["open_order_count"], 0)


if __name__ == "__main__":
    unittest.main()
