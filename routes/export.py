from flask import jsonify


def register_export_routes(app, *, run_daily_snapshot) -> None:
    @app.post("/export-daily-snapshot")
    def export_daily_snapshot_endpoint():
        try:
            result = run_daily_snapshot()
            return jsonify(result)
        except Exception as e:
            print(f"Daily snapshot export failed: {e}", flush=True)
            return jsonify({"ok": False, "error": str(e)}), 500