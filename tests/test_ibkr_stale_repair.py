from datetime import UTC, datetime
import unittest

from scripts.repair_ibkr_stale_closes_from_vm_logs import (
    LifecycleRow,
    build_repair_candidates,
    parse_vm_journal,
)


class IbkrStaleRepairTests(unittest.TestCase):
    def test_parse_vm_journal_extracts_execution_pairs_and_portfolio_snapshots(self):
        lines = [
            "Apr 10 17:58:00 ibkr-bridge-vm python[31564]: updatePortfolio: PortfolioItem(contract=Stock(conId=525768800, symbol='RIVN', right='0', primaryExchange='NASDAQ', currency='USD', localSymbol='RIVN', tradingClass='NMS'), position=0.0, marketPrice=15.39000035, marketValue=0.0, averageCost=0.0, unrealizedPNL=0.0, realizedPNL=-73.07, account='DUP742133')",
            "Apr 10 17:58:00 ibkr-bridge-vm python[31564]: execDetails Execution(execId='entry-1', time=datetime.datetime(2026, 4, 10, 15, 35, 23, tzinfo=datetime.timezone.utc), acctNumber='DUP742133', exchange='BYX', side='BOT', shares=317.0, price=15.69, permId=576258107, clientId=101, orderId=34, liquidation=0, cumQty=317.0, avgPrice=15.69, orderRef='scanner-RIVN-BUY-157100-317', evRule='', evMultiplier=0.0, modelCode='', lastLiquidity=2)",
            "Apr 10 17:58:00 ibkr-bridge-vm python[31564]: execDetails Execution(execId='exit-1', time=datetime.datetime(2026, 4, 10, 16, 1, 9, tzinfo=datetime.timezone.utc), acctNumber='DUP742133', exchange='BYX', side='SLD', shares=317.0, price=15.47, permId=576258109, clientId=101, orderId=36, liquidation=0, cumQty=317.0, avgPrice=15.47, orderRef='scanner-RIVN-BUY-157100-317', evRule='', evMultiplier=0.0, modelCode='', lastLiquidity=2)",
            "Apr 10 17:58:00 ibkr-bridge-vm python[31564]: commissionReport: CommissionReport(execId='exit-1', commission=1.747837, currency='USD', realizedPNL=-73.072837, yield_=0.0, yieldRedemptionDate=0)",
        ]

        executions, portfolios = parse_vm_journal(lines, year=2026)

        self.assertEqual(len(executions), 2)
        self.assertEqual(len(portfolios), 1)
        self.assertEqual(executions[1].order_id, "36")
        self.assertEqual(executions[1].realized_pnl, -73.072837)
        self.assertEqual(portfolios[0].symbol, "RIVN")

    def test_build_repair_candidates_prefers_execution_pairs(self):
        lines = [
            "Apr 10 17:58:00 ibkr-bridge-vm python[31564]: execDetails Execution(execId='entry-1', time=datetime.datetime(2026, 4, 10, 15, 35, 23, tzinfo=datetime.timezone.utc), acctNumber='DUP742133', exchange='BYX', side='BOT', shares=317.0, price=15.69, permId=576258107, clientId=101, orderId=34, liquidation=0, cumQty=317.0, avgPrice=15.69, orderRef='scanner-RIVN-BUY-157100-317', evRule='', evMultiplier=0.0, modelCode='', lastLiquidity=2)",
            "Apr 10 17:58:00 ibkr-bridge-vm python[31564]: execDetails Execution(execId='exit-1', time=datetime.datetime(2026, 4, 10, 16, 1, 9, tzinfo=datetime.timezone.utc), acctNumber='DUP742133', exchange='BYX', side='SLD', shares=317.0, price=15.47, permId=576258109, clientId=101, orderId=36, liquidation=0, cumQty=317.0, avgPrice=15.47, orderRef='scanner-RIVN-BUY-157100-317', evRule='', evMultiplier=0.0, modelCode='', lastLiquidity=2)",
            "Apr 10 17:58:00 ibkr-bridge-vm python[31564]: commissionReport: CommissionReport(execId='exit-1', commission=1.747837, currency='USD', realizedPNL=-73.072837, yield_=0.0, yieldRedemptionDate=0)",
        ]
        executions, portfolios = parse_vm_journal(lines, year=2026)
        row = LifecycleRow(
            trade_key="34",
            symbol="RIVN",
            mode="primary",
            side="BUY",
            direction="LONG",
            status="CLOSED",
            entry_time=datetime(2026, 4, 10, 15, 35, 13, tzinfo=UTC),
            entry_price=15.71,
            exit_time=datetime(2026, 4, 10, 16, 5, 26, tzinfo=UTC),
            exit_price=15.71,
            stop_price=15.493,
            target_price=16.144,
            exit_reason="STALE_OPEN_RECONCILED",
            shares=317.0,
            realized_pnl=0.0,
            realized_pnl_percent=0.0,
            signal_timestamp=None,
            signal_entry=None,
            signal_stop=None,
            signal_target=None,
            signal_confidence=None,
            broker="IBKR",
            order_id="34",
            parent_order_id="34",
            exit_order_id="34",
        )

        candidates = build_repair_candidates([row], executions, portfolios)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].exit_order_id, "36")
        self.assertEqual(candidates[0].exit_price, 15.47)
        self.assertEqual(candidates[0].exit_reason, "BROKER_FILLED_EXIT_REPAIRED")
        self.assertEqual(candidates[0].source, "execution_pair")

    def test_build_repair_candidates_falls_back_to_portfolio_snapshot(self):
        lines = [
            "Apr 10 17:58:00 ibkr-bridge-vm python[31564]: updatePortfolio: PortfolioItem(contract=Stock(conId=332794741, symbol='NIO', right='0', primaryExchange='NYSE', currency='USD', localSymbol='NIO', tradingClass='NIO'), position=0.0, marketPrice=6.44000005, marketValue=0.0, averageCost=0.0, unrealizedPNL=0.0, realizedPNL=-97.24, account='DUP742133')",
        ]
        executions, portfolios = parse_vm_journal(lines, year=2026)
        row = LifecycleRow(
            trade_key="10",
            symbol="NIO",
            mode="primary",
            side="SELL",
            direction="SHORT",
            status="CLOSED",
            entry_time=datetime(2026, 4, 9, 14, 25, 20, tzinfo=UTC),
            entry_price=6.2,
            exit_time=datetime(2026, 4, 10, 13, 55, 2, tzinfo=UTC),
            exit_price=6.2,
            stop_price=6.312,
            target_price=5.976,
            exit_reason="STALE_OPEN_RECONCILED",
            shares=1612.0,
            realized_pnl=0.0,
            realized_pnl_percent=0.0,
            signal_timestamp=None,
            signal_entry=None,
            signal_stop=None,
            signal_target=None,
            signal_confidence=None,
            broker="IBKR",
            order_id="10",
            parent_order_id="10",
            exit_order_id="10",
        )

        candidates = build_repair_candidates([row], executions, portfolios)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].source, "portfolio_snapshot")
        self.assertEqual(candidates[0].exit_reason, "BROKER_PORTFOLIO_SNAPSHOT_REPAIRED")
        self.assertAlmostEqual(candidates[0].realized_pnl, -97.24, places=6)

    def test_portfolio_snapshot_fallback_is_skipped_for_multiple_rows_same_symbol(self):
        lines = [
            "Apr 10 17:58:00 ibkr-bridge-vm python[31564]: updatePortfolio: PortfolioItem(contract=Stock(conId=332794741, symbol='NIO', right='0', primaryExchange='NYSE', currency='USD', localSymbol='NIO', tradingClass='NIO'), position=0.0, marketPrice=6.44000005, marketValue=0.0, averageCost=0.0, unrealizedPNL=0.0, realizedPNL=-97.24, account='DUP742133')",
        ]
        executions, portfolios = parse_vm_journal(lines, year=2026)
        row_one = LifecycleRow(
            trade_key="10",
            symbol="NIO",
            mode="primary",
            side="SELL",
            direction="SHORT",
            status="CLOSED",
            entry_time=datetime(2026, 4, 9, 14, 25, 20, tzinfo=UTC),
            entry_price=6.2,
            exit_time=datetime(2026, 4, 10, 13, 55, 2, tzinfo=UTC),
            exit_price=6.2,
            stop_price=6.312,
            target_price=5.976,
            exit_reason="STALE_OPEN_RECONCILED",
            shares=1612.0,
            realized_pnl=0.0,
            realized_pnl_percent=0.0,
            signal_timestamp=None,
            signal_entry=None,
            signal_stop=None,
            signal_target=None,
            signal_confidence=None,
            broker="IBKR",
            order_id="10",
            parent_order_id="10",
            exit_order_id="10",
        )
        row_two = LifecycleRow(
            trade_key="11",
            symbol="NIO",
            mode="secondary",
            side="SELL",
            direction="SHORT",
            status="CLOSED",
            entry_time=datetime(2026, 4, 9, 14, 35, 20, tzinfo=UTC),
            entry_price=6.25,
            exit_time=datetime(2026, 4, 10, 13, 56, 2, tzinfo=UTC),
            exit_price=6.25,
            stop_price=6.35,
            target_price=6.0,
            exit_reason="STALE_OPEN_RECONCILED",
            shares=1500.0,
            realized_pnl=0.0,
            realized_pnl_percent=0.0,
            signal_timestamp=None,
            signal_entry=None,
            signal_stop=None,
            signal_target=None,
            signal_confidence=None,
            broker="IBKR",
            order_id="11",
            parent_order_id="11",
            exit_order_id="11",
        )

        candidates = build_repair_candidates([row_one, row_two], executions, portfolios)

        self.assertEqual(candidates, [])


if __name__ == "__main__":
    unittest.main()
