from __future__ import annotations

from datetime import datetime

from flask import jsonify, request

from core.logging_utils import log_exception


def register_scheduler_routes(
    app,
    *,
    ny_tz,
    execute_market_ops,
    execute_post_close_ops,
    execute_maintenance_ops,
    execute_ibkr_vm_control,
):
    @app.post("/scheduler/market-ops")
    def scheduler_market_ops():
        now_ny = datetime.now(ny_tz)
        try:
            result = execute_market_ops(now_ny=now_ny)
            return jsonify(result)
        except Exception as e:
            log_exception("scheduler market ops failed", e, route="/scheduler/market-ops")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/scheduler/daily-post-close")
    def scheduler_daily_post_close():
        now_ny = datetime.now(ny_tz)
        try:
            result = execute_post_close_ops(now_ny=now_ny)
            return jsonify(result)
        except Exception as e:
            log_exception("scheduler daily post close failed", e, route="/scheduler/daily-post-close")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/scheduler/maintenance")
    def scheduler_maintenance():
        now_ny = datetime.now(ny_tz)
        payload = request.get_json(silent=True) or {}
        retention_days_raw = payload.get("retention_days", 30)
        try:
            retention_days = max(1, int(retention_days_raw))
        except Exception:
            return jsonify({"ok": False, "error": "retention_days must be an integer"}), 400

        try:
            result = execute_maintenance_ops(now_ny=now_ny, retention_days=retention_days)
            return jsonify(result)
        except Exception as e:
            log_exception("scheduler maintenance failed", e, route="/scheduler/maintenance")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/scheduler/ibkr-vm-control")
    def scheduler_ibkr_vm_control():
        now_ny = datetime.now(ny_tz)
        payload = request.get_json(silent=True) or {}
        action = str(payload.get("action", "")).strip().lower()
        force = bool(payload.get("force", False))

        if action not in {"start", "stop"}:
            return jsonify({"ok": False, "error": "action must be 'start' or 'stop'"}), 400

        try:
            result = execute_ibkr_vm_control(now_ny=now_ny, action=action, force=force)
            return jsonify(result)
        except Exception as e:
            log_exception("scheduler ibkr vm control failed", e, route="/scheduler/ibkr-vm-control")
            return jsonify({"ok": False, "error": str(e)}), 500
