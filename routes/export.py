from flask import jsonify
from logging_utils import log_exception


def register_export_routes(app, *, run_daily_snapshot) -> None:
    @app.post("/export-daily-snapshot")
    def export_daily_snapshot_endpoint():
        try:
            result = run_daily_snapshot()
            return jsonify(result)
        except Exception as e:
            log_exception("Daily snapshot export failed", e, route="/export-daily-snapshot")
            return jsonify({"ok": False, "error": str(e)}), 500
