import unittest
from unittest import SkipTest
from unittest.mock import patch

try:
    from repositories.trades_repo import upsert_trade_lifecycle
except ModuleNotFoundError as exc:
    if exc.name == "psycopg":
        raise SkipTest("psycopg dependency is not available in this local unittest environment")
    raise


class TradeLifecyclePersistenceTests(unittest.TestCase):
    @patch("repositories.trades_repo.execute")
    @patch("repositories.trades_repo.fetch_one")
    def test_update_preserves_existing_mode_when_new_mode_is_blank(self, mock_fetch_one, mock_execute):
        mock_fetch_one.return_value = {"id": 42}

        upsert_trade_lifecycle(
            trade_key="trade-1",
            symbol="NVDA",
            mode="",
            status="CLOSED",
        )

        query = mock_execute.call_args.args[0]
        params = mock_execute.call_args.args[1]

        self.assertIn("mode = COALESCE(NULLIF(%(mode)s, ''), mode)", query)
        self.assertEqual(params["mode"], "")
        self.assertEqual(params["id"], 42)

    @patch("repositories.trades_repo.execute")
    @patch("repositories.trades_repo.fetch_one")
    def test_update_preserves_existing_broker_when_new_broker_is_blank(self, mock_fetch_one, mock_execute):
        mock_fetch_one.return_value = {"id": 42}

        upsert_trade_lifecycle(
            trade_key="trade-2",
            symbol="AAPL",
            broker="",
            status="OPEN",
        )

        query = mock_execute.call_args.args[0]
        params = mock_execute.call_args.args[1]

        self.assertIn("broker = COALESCE(NULLIF(%(broker)s, ''), broker)", query)
        self.assertEqual(params["broker"], "")
        self.assertEqual(params["id"], 42)


if __name__ == "__main__":
    unittest.main()
