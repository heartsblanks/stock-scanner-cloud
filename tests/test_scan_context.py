import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from orchestration.scan_context import build_scheduled_scan_payload, paper_candidate_from_evaluation


NY_TZ = ZoneInfo("America/New_York")


class ScanContextTests(unittest.TestCase):
    def test_build_scheduled_scan_payload_uses_provided_mode_order(self):
        now_ny = datetime(2026, 4, 1, 9, 50, tzinfo=NY_TZ)

        payload = build_scheduled_scan_payload({}, now_ny=now_ny, mode_order=["core_three", "core_one"])

        self.assertEqual(payload["mode"], "core_three")
        self.assertEqual(payload["scheduled_mode_order"], ["core_three", "core_one"])

    def test_paper_candidate_requires_valid_decision(self):
        evaluation = {
            "name": "Apple",
            "decision": "REJECTED",
            "final_reason": "below_threshold",
            "metrics": {
                "symbol": "AAPL",
                "direction": "BUY",
                "entry": 100.0,
                "stop": 99.0,
                "target": 102.0,
                "final_confidence": 90.0,
            },
        }

        self.assertIsNone(paper_candidate_from_evaluation(evaluation, 70))

    def test_paper_candidate_requires_confidence_threshold(self):
        evaluation = {
            "name": "Apple",
            "decision": "VALID",
            "final_reason": "candidate",
            "metrics": {
                "symbol": "AAPL",
                "direction": "BUY",
                "entry": 100.0,
                "stop": 99.0,
                "target": 102.0,
                "final_confidence": 65.0,
            },
        }

        self.assertIsNone(paper_candidate_from_evaluation(evaluation, 70))
        self.assertEqual(evaluation["final_reason"], "below_paper_trade_confidence_threshold")

    def test_paper_candidate_normalizes_metrics_for_valid_setup(self):
        evaluation = {
            "name": "Apple",
            "decision": "VALID",
            "final_reason": "candidate",
            "metrics": {
                "symbol": "AAPL",
                "direction": "BUY",
                "price": 100.25,
                "entry": 100.0,
                "stop": 99.0,
                "target": 102.0,
                "shares": "10",
                "risk_per_share": 1.0,
                "actual_position_cost": 1000.0,
                "actual_risk": 10.0,
                "risk_amount": 10.0,
                "final_confidence": 82.0,
            },
            "checks": {"trend": True},
            "info": {"symbol": "AAPL"},
            "candles": [{"close": 100.25}],
            "benchmark_directions": {"SPY": "BUY"},
        }

        candidate = paper_candidate_from_evaluation(evaluation, 70)

        self.assertIsNotNone(candidate)
        self.assertEqual(candidate["decision"], "PAPER_CANDIDATE")
        self.assertEqual(candidate["metrics"]["symbol"], "AAPL")
        self.assertEqual(candidate["metrics"]["shares"], 10)
        self.assertTrue(candidate["metrics"]["paper_eligible"])
        self.assertTrue(candidate["metrics"]["manual_eligible"])
        self.assertEqual(candidate["info"], {"symbol": "AAPL"})

    def test_paper_candidate_blocks_long_when_market_regime_disagrees(self):
        evaluation = {
            "name": "Apple",
            "decision": "VALID",
            "final_reason": "candidate",
            "metrics": {
                "symbol": "AAPL",
                "direction": "BUY",
                "entry": 100.0,
                "stop": 99.0,
                "target": 102.0,
                "final_confidence": 88.0,
            },
            "benchmark_directions": {"SPY": "SELL", "QQQ": "BUY"},
        }

        self.assertIsNone(paper_candidate_from_evaluation(evaluation, 70))
        self.assertEqual(evaluation["final_reason"], "long_market_regime_blocked")

    def test_paper_candidate_allows_short_even_when_benchmarks_are_long(self):
        evaluation = {
            "name": "Apple",
            "decision": "VALID",
            "final_reason": "candidate",
            "metrics": {
                "symbol": "AAPL",
                "direction": "SELL",
                "entry": 100.0,
                "stop": 101.0,
                "target": 98.0,
                "final_confidence": 88.0,
            },
            "benchmark_directions": {"SPY": "BUY", "QQQ": "BUY"},
        }

        candidate = paper_candidate_from_evaluation(evaluation, 70)

        self.assertIsNotNone(candidate)
        self.assertEqual(candidate["metrics"]["direction"], "SELL")


if __name__ == "__main__":
    unittest.main()
