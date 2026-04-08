import unittest
from unittest.mock import patch

from brokers import get_paper_broker, get_paper_broker_config


class BrokerRegistryTests(unittest.TestCase):
    def test_default_broker_config_is_alpaca(self):
        with patch.dict("os.environ", {}, clear=False):
            config = get_paper_broker_config()

        self.assertEqual(config.broker_name, "alpaca")

    def test_ibkr_broker_can_be_selected_but_is_not_ready_yet(self):
        with patch.dict("os.environ", {"PAPER_BROKER": "ibkr"}, clear=False):
            broker = get_paper_broker()

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


if __name__ == "__main__":
    unittest.main()
