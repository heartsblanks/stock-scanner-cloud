import os
import unittest
from unittest.mock import patch

from flask import Flask

from routes.health import register_health_routes


class HealthRoutesTests(unittest.TestCase):
    def _build_client(self, *, send_telegram_alert=None, telegram_alerts_enabled=None):
        app = Flask(__name__)
        register_health_routes(
            app,
            db_healthcheck=lambda: {"db": "ok"},
            enable_db_logging=True,
            get_ops_summary=lambda: {"summary": "ok"},
            get_recent_paper_trade_attempts=lambda limit, decision_stage=None, broker=None: [],
            get_recent_paper_trade_rejections=lambda limit, broker=None: [],
            get_paper_trade_attempt_daily_summary=lambda limit_days, broker=None: [],
            get_paper_trade_attempt_hourly_summary=lambda limit_days, broker=None: [],
            get_ibkr_operational_status=lambda: {"ok": True},
            telegram_alerts_enabled=telegram_alerts_enabled or (lambda: True),
            send_telegram_alert=send_telegram_alert or (lambda **kwargs: {"ok": True, "sent": True, "reason": "delivered"}),
        )
        return app.test_client()

    def test_admin_test_alert_requires_admin_token(self):
        client = self._build_client()
        with patch.dict(os.environ, {"ADMIN_API_TOKEN": "secret-token"}, clear=False):
            response = client.post("/admin/test-alert", json={})

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["error"], "unauthorized")

    def test_admin_test_alert_sends_when_authorized(self):
        sent = {}
        client = self._build_client(
            send_telegram_alert=lambda **kwargs: sent.update(kwargs) or {"ok": True, "sent": True, "reason": "delivered"},
        )

        with patch.dict(os.environ, {"ADMIN_API_TOKEN": "secret-token"}, clear=False):
            response = client.post(
                "/admin/test-alert",
                json={"message": "hello from test"},
                headers={"X-Admin-Token": "secret-token"},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(sent["message"], "hello from test")
        self.assertEqual(sent["alert_key"], "admin-test-alert")


if __name__ == "__main__":
    unittest.main()
