import math
import unittest
from unittest.mock import patch

from brokers import get_paper_broker, get_paper_broker_config
from brokers.ibkr_adapter import IbkrPaperBroker, _compact_trade_for_bridge
from brokers.ibkr_bridge_client import get_ibkr_bridge_base_url, get_ibkr_bridge_token, ibkr_bridge_enabled


class BrokerRegistryTests(unittest.TestCase):
    def test_default_broker_config_is_ibkr(self):
        with patch.dict("os.environ", {}, clear=False):
            config = get_paper_broker_config()

        self.assertEqual(config.broker_name, "ibkr")

    def test_ibkr_broker_can_be_selected_but_is_not_ready_yet(self):
        with patch.dict("os.environ", {"PAPER_BROKER": "ibkr"}, clear=False):
            broker = get_paper_broker()

        self.assertIsInstance(broker, IbkrPaperBroker)
        self.assertEqual(broker.name, "ibkr")
        with self.assertRaises(NotImplementedError):
            broker.get_account()

    def test_broker_config_reads_shadow_flags(self):
        with patch.dict(
            "os.environ",
            {
                "PAPER_BROKER": "ibkr",
                "ENABLE_IBKR_SHADOW_MODE": "true",
                "ENABLE_IBKR_MARKET_DATA_COMPARE": "true",
            },
            clear=False,
        ):
            config = get_paper_broker_config()

        self.assertEqual(config.broker_name, "ibkr")
        self.assertTrue(config.shadow_mode_enabled)
        self.assertTrue(config.market_data_compare_enabled)

    def test_ibkr_bridge_config_defaults_to_disabled(self):
        with patch.dict("os.environ", {}, clear=False):
            self.assertEqual(get_ibkr_bridge_base_url(), "")
            self.assertEqual(get_ibkr_bridge_token(), "")
            self.assertFalse(ibkr_bridge_enabled())

    def test_ibkr_adapter_uses_bridge_contract_when_configured(self):
        broker = IbkrPaperBroker()

        with patch.dict("os.environ", {"IBKR_BRIDGE_BASE_URL": "https://ibkr-bridge.example.com"}, clear=False):
            with patch("brokers.ibkr_adapter.ibkr_bridge_get") as mock_get:
                mock_get.return_value = {"equity": "100000"}
                result = broker.get_account()

        self.assertEqual(result, {"equity": "100000"})
        mock_get.assert_called_once_with("/account", timeout=5)

    def test_ibkr_adapter_uses_explicit_timeouts_for_close_and_sync_paths(self):
        broker = IbkrPaperBroker()

        with patch.dict(
            "os.environ",
            {
                "IBKR_BRIDGE_BASE_URL": "https://ibkr-bridge.example.com",
                "IBKR_BRIDGE_CLOSE_TIMEOUT_SECONDS": "21",
                "IBKR_BRIDGE_ORDER_SYNC_TIMEOUT_SECONDS": "9",
                "IBKR_BRIDGE_ORDER_SYNC_BATCH_TIMEOUT_SECONDS": "19",
                "IBKR_BRIDGE_ORDER_STATUS_TIMEOUT_SECONDS": "7",
                "IBKR_BRIDGE_CANCEL_TIMEOUT_SECONDS": "6",
            },
            clear=False,
        ):
            with patch("brokers.ibkr_adapter.ibkr_bridge_post") as mock_post:
                mock_post.return_value = {"canceled_order_ids": ["1"]}
                cancel_result = broker.cancel_open_orders_for_symbol("NVDA")
                self.assertEqual(cancel_result, ["1"])
                mock_post.assert_called_once_with(
                    "/orders/cancel-by-symbol",
                    json_body={"symbol": "NVDA"},
                    timeout=6,
                )

            with patch("brokers.ibkr_adapter.ibkr_bridge_post") as mock_post:
                mock_post.return_value = {"placed": True}
                broker.close_position("NVDA", cancel_orders=True)
                mock_post.assert_called_once_with(
                    "/positions/close",
                    json_body={"symbol": "NVDA", "cancel_orders": True},
                    timeout=21,
                )

            with patch("brokers.ibkr_adapter.ibkr_bridge_get") as mock_get:
                mock_get.return_value = {"status": "filled"}
                broker.sync_order_by_id("123")
                mock_get.assert_called_once_with("/orders/123/sync", timeout=9)

            with patch("brokers.ibkr_adapter.ibkr_bridge_post") as mock_post:
                mock_post.return_value = {"results": [{"id": "123", "status": "filled"}]}
                rows = broker.sync_orders_by_ids(["123"])
                self.assertEqual(rows["123"]["status"], "filled")
                mock_post.assert_called_once_with(
                    "/orders/sync-batch",
                    json_body={"order_ids": ["123"]},
                    timeout=19,
                )

            with patch("brokers.ibkr_adapter.ibkr_bridge_get") as mock_get:
                mock_get.return_value = {"id": "123"}
                broker.get_order_by_id("123", nested=True)
                mock_get.assert_called_once_with(
                    "/orders/123",
                    params={"nested": "true"},
                    timeout=7,
                )

    def test_ibkr_adapter_compacts_and_sanitizes_trade_payload_for_bridge(self):
        trade = {
            "name": "Joby Aviation",
            "final_reason": "Paper candidate",
            "decision": "PAPER_CANDIDATE",
            "metrics": {
                "symbol": "JOBY",
                "direction": "BUY",
                "entry": 5.25,
                "stop": 5.0,
                "target": 5.75,
                "shares": 100,
                "per_trade_notional": math.inf,
                "remaining_allocatable_capital": 10000.0,
                "reward_extension": math.nan,
            },
            "candles": [{"close": math.nan}],
        }

        compact = _compact_trade_for_bridge(trade)

        self.assertEqual(compact["metrics"]["symbol"], "JOBY")
        self.assertEqual(compact["metrics"]["direction"], "BUY")
        self.assertEqual(compact["metrics"]["entry"], 5.25)
        self.assertIsNone(compact["metrics"]["per_trade_notional"])
        self.assertNotIn("candles", compact)

    def test_ibkr_adapter_posts_compact_trade_payload(self):
        broker = IbkrPaperBroker()
        trade = {
            "name": "Joby Aviation",
            "decision": "PAPER_CANDIDATE",
            "metrics": {
                "symbol": "JOBY",
                "direction": "BUY",
                "entry": 5.25,
                "stop": 5.0,
                "target": 5.75,
                "shares": 100,
                "per_trade_notional": math.inf,
                "remaining_allocatable_capital": 10000.0,
            },
            "candles": [{"close": math.nan}],
        }

        with patch.dict("os.environ", {"IBKR_BRIDGE_BASE_URL": "https://ibkr-bridge.example.com"}, clear=False):
            with patch("brokers.ibkr_adapter.ibkr_bridge_post") as mock_post:
                mock_post.return_value = {"placed": True}
                result = broker.place_paper_bracket_order_from_trade(trade)

        self.assertEqual(result, {"placed": True})
        self.assertEqual(mock_post.call_args.args[0], "/orders/paper-bracket")
        posted_trade = mock_post.call_args.kwargs["json_body"]["trade"]
        self.assertEqual(posted_trade["metrics"]["symbol"], "JOBY")
        self.assertIsNone(posted_trade["metrics"]["per_trade_notional"])
        self.assertEqual(posted_trade["entry_order_type"], "MKT")
        self.assertNotIn("candles", posted_trade)


if __name__ == "__main__":
    unittest.main()
