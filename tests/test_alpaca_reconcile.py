import unittest

from alpaca.reconcile import (
    LocalTradePair,
    build_reconciliation_detail_row,
    infer_alpaca_exit_from_order_set,
)


class AlpacaReconcileTests(unittest.TestCase):
    def test_infer_external_exit_returns_single_match(self):
        parent_order = {
            "id": "parent-1",
            "symbol": "AAPL",
            "side": "buy",
            "submitted_at": "2026-04-01T14:30:00Z",
            "filled_qty": "1",
            "status": "filled",
            "legs": [],
        }
        all_orders = [
            parent_order,
            {
                "id": "exit-1",
                "symbol": "AAPL",
                "side": "sell",
                "status": "filled",
                "submitted_at": "2026-04-01T15:05:00Z",
                "filled_at": "2026-04-01T15:05:02Z",
                "filled_qty": "1",
                "filled_avg_price": "192.15",
            },
        ]

        reason, qty, price, order_id, candidate_count = infer_alpaca_exit_from_order_set(parent_order, all_orders)

        self.assertEqual(reason, "EXTERNAL_EXIT")
        self.assertEqual(order_id, "exit-1")
        self.assertEqual(qty, 1.0)
        self.assertEqual(price, 192.15)
        self.assertEqual(candidate_count, 1)

    def test_infer_external_exit_marks_ambiguous_when_multiple_candidates_match(self):
        parent_order = {
            "id": "parent-1",
            "symbol": "AAPL",
            "side": "buy",
            "submitted_at": "2026-04-01T14:30:00Z",
            "filled_qty": "1",
            "status": "filled",
            "legs": [],
        }
        all_orders = [
            parent_order,
            {
                "id": "exit-1",
                "symbol": "AAPL",
                "side": "sell",
                "status": "filled",
                "submitted_at": "2026-04-01T15:05:00Z",
                "filled_at": "2026-04-01T15:05:02Z",
                "filled_qty": "1",
                "filled_avg_price": "192.15",
            },
            {
                "id": "exit-2",
                "symbol": "AAPL",
                "side": "sell",
                "status": "filled",
                "submitted_at": "2026-04-01T15:06:00Z",
                "filled_at": "2026-04-01T15:06:02Z",
                "filled_qty": "1",
                "filled_avg_price": "192.10",
            },
        ]

        reason, qty, price, order_id, candidate_count = infer_alpaca_exit_from_order_set(parent_order, all_orders)

        self.assertEqual(reason, "EXTERNAL_EXIT_AMBIGUOUS")
        self.assertIsNone(qty)
        self.assertIsNone(price)
        self.assertEqual(order_id, "")
        self.assertEqual(candidate_count, 2)

    def test_reconciliation_row_stays_unresolved_for_ambiguous_external_exit(self):
        pair = LocalTradePair(
            broker_parent_order_id="parent-1",
            symbol="AAPL",
            mode="core_one",
            entry_timestamp_utc="2026-04-01T14:30:00Z",
            exit_timestamp_utc="2026-04-01T15:07:00Z",
            entry_price=191.0,
            exit_price=192.0,
            shares=1.0,
            exit_reason="EXTERNAL_EXIT",
            client_order_id="scanner-aapl-1",
        )
        parent_order = {
            "id": "parent-1",
            "symbol": "AAPL",
            "side": "buy",
            "submitted_at": "2026-04-01T14:30:00Z",
            "filled_qty": "1",
            "filled_avg_price": "191.00",
            "status": "filled",
            "legs": [],
        }
        all_orders = [
            parent_order,
            {
                "id": "exit-1",
                "symbol": "AAPL",
                "side": "sell",
                "status": "filled",
                "submitted_at": "2026-04-01T15:05:00Z",
                "filled_at": "2026-04-01T15:05:02Z",
                "filled_qty": "1",
                "filled_avg_price": "192.15",
            },
            {
                "id": "exit-2",
                "symbol": "AAPL",
                "side": "sell",
                "status": "filled",
                "submitted_at": "2026-04-01T15:06:00Z",
                "filled_at": "2026-04-01T15:06:02Z",
                "filled_qty": "1",
                "filled_avg_price": "192.10",
            },
        ]

        detail = build_reconciliation_detail_row(pair, parent_order, all_orders)

        self.assertEqual(detail.match_status, "exit_not_resolved")
        self.assertEqual(detail.alpaca_exit_reason, "EXTERNAL_EXIT_AMBIGUOUS")
        self.assertEqual(detail.alpaca_exit_order_id, "AMBIGUOUS:2")


if __name__ == "__main__":
    unittest.main()
