from flask import jsonify


def register_health_routes(app, *, db_healthcheck, enable_csv_logging: bool, enable_db_logging: bool) -> None:
    @app.get("/")
    def home():
        return jsonify({
            "ok": True,
            "service": "stock-scanner",
            "logging": {
                "csv_enabled": enable_csv_logging,
                "db_enabled": enable_db_logging,
            },
            "endpoints": [
                "/scan",
                "/scheduled-paper-scan",
                "/log-trade",
                "/sync-paper-trades",
                "/close-paper-positions",
                "/reconcile-paper-trades",
                "/analyze-paper-trades",
                "/analyze-signals",
                "/read-trades-by-date",
                "/db-health",
                "/export-daily-snapshot",
            ],
        })

    @app.get("/db-health")
    def db_health():
        try:
            result = db_healthcheck()
            return jsonify({"ok": True, **result})
        except Exception as e:
            print(f"DB healthcheck failed: {e}", flush=True)
            return jsonify({"ok": False, "error": str(e)}), 500