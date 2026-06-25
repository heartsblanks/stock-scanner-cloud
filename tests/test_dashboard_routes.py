import unittest

from flask import Flask

from routes.dashboard import register_dashboard_routes


class DashboardRoutesTests(unittest.TestCase):
    def test_dashboard_daily_returns_minimal_daily_payload(self):
        app = Flask(__name__)
        register_dashboard_routes(
            app,
            get_dashboard_summary=lambda **kwargs: {},
            get_daily_dashboard_summary=lambda **kwargs: {
                "date": kwargs.get("target_date"),
                "broker": kwargs.get("broker"),
                "realized_pnl": 12.5,
                "unrealized_pnl": None,
                "total_day_pnl": 12.5,
                "open_position_count": 1,
                "open_exposure": 250.0,
                "placements_today": 2,
                "placement_rate": 50.0,
                "latest_scan": {"mode": "core_one"},
            },
        )

        response = app.test_client().get("/dashboard-daily?date=2026-04-09&broker=IBKR")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["date"], "2026-04-09")
        self.assertEqual(payload["broker"], "IBKR")
        self.assertEqual(payload["realized_pnl"], 12.5)
        self.assertEqual(payload["placements_today"], 2)


if __name__ == "__main__":
    unittest.main()
