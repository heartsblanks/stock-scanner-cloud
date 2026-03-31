from flask import jsonify, request
from logging_utils import log_exception


def register_health_routes(app, *, db_healthcheck, enable_db_logging: bool, get_ops_summary, get_recent_alpaca_api_logs, get_recent_alpaca_api_errors) -> None:
    @app.get("/")
    def home():
        return jsonify({
            "ok": True,
            "service": "stock-scanner",
            "logging": {
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
                "/health",
                "/db-health",
                "/export-daily-snapshot",
                "/ops-summary",
                "/alpaca-api-logs/recent",
                "/alpaca-api-logs/errors",
            ],
        })

    @app.get("/health")
    def health():
        return jsonify({
            "ok": True,
            "service": "stock-scanner",
        })

    @app.get("/db-health")
    def db_health():
        try:
            result = db_healthcheck()
            return jsonify({"ok": True, **result})
        except Exception as e:
            log_exception("DB healthcheck failed", e, route="/db-health")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.get("/ops-summary")
    def ops_summary():
        try:
            summary = get_ops_summary()
            return jsonify({
                "ok": True,
                **summary,
            })
        except Exception as e:
            log_exception("Ops summary failed", e, route="/ops-summary")
            return jsonify({"ok": False, "error": str(e)}), 500


    @app.get("/alpaca-api-logs/recent")
    def alpaca_api_logs_recent():
        try:
            limit_raw = request.args.get("limit", "100")
            try:
                limit = max(1, min(1000, int(limit_raw)))
            except Exception:
                return jsonify({"ok": False, "error": "limit must be an integer"}), 400

            rows = get_recent_alpaca_api_logs(limit=limit)
            return jsonify({
                "ok": True,
                "count": len(rows),
                "limit": limit,
                "rows": rows,
            })
        except Exception as e:
            log_exception("alpaca-api-logs/recent failed", e, route="/alpaca-api-logs/recent")
            return jsonify({"ok": False, "error": str(e)}), 500


    @app.get("/alpaca-api-logs/errors")
    def alpaca_api_logs_errors():
        try:
            limit_raw = request.args.get("limit", "100")
            try:
                limit = max(1, min(1000, int(limit_raw)))
            except Exception:
                return jsonify({"ok": False, "error": "limit must be an integer"}), 400

            rows = get_recent_alpaca_api_errors(limit=limit)
            return jsonify({
                "ok": True,
                "count": len(rows),
                "limit": limit,
                "rows": rows,
            })
        except Exception as e:
            log_exception("alpaca-api-logs/errors failed", e, route="/alpaca-api-logs/errors")
            return jsonify({"ok": False, "error": str(e)}), 500
