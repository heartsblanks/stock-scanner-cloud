import unittest
from unittest.mock import patch

from brokers import get_paper_broker, get_paper_broker_config
from brokers.ibkr_adapter import IbkrPaperBroker
from brokers.ibkr_bridge_client import get_ibkr_bridge_base_url, get_ibkr_bridge_token, ibkr_bridge_enabled


class BrokerRegistryTests(unittest.TestCase):
    def test_default_broker_config_is_alpaca(self):
        with patch.dict("os.environ", {}, clear=False):
            config = get_paper_broker_config()

        self.assertEqual(config.broker_name, "alpaca")

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
                "PAPER_BROKER": "alpaca",
                "ENABLE_IBKR_SHADOW_MODE": "true",
                "ENABLE_IBKR_MARKET_DATA_COMPARE": "true",
            },
            clear=False,
        ):
            config = get_paper_broker_config()

        self.assertEqual(config.broker_name, "alpaca")
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


if __name__ == "__main__":
    unittest.main()
