import time

from flask import jsonify, request
from core.logging_utils import log_exception


def register_health_routes(
    app,
    *,
    db_healthcheck,
    enable_db_logging: bool,
    get_ops_summary,
    get_recent_alpaca_api_logs,
    get_recent_alpaca_api_errors,
    get_recent_paper_trade_attempts,
    get_recent_paper_trade_rejections,
    get_paper_trade_attempt_daily_summary,
    get_paper_trade_attempt_hourly_summary,
    prune_alpaca_api_logs,
) -> None:
    cache: dict[tuple[str, str], tuple[float, dict]] = {}

    def get_cached_json(cache_key: tuple[str, str], ttl_seconds: int, builder):
        now = time.time()
        cached = cache.get(cache_key)
        if cached and now - cached[0] < ttl_seconds:
            return cached[1]

        payload = builder()
        cache[cache_key] = (now, payload)
        return payload

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
                "/paper-trade-attempts/recent",
                "/paper-trade-attempts/rejections",
                "/paper-trade-attempts/daily-summary",
                "/paper-trade-attempts/hourly-summary",
                "/alpaca-api-logs/recent",
                "/alpaca-api-logs/errors",
                "/alpaca-api-logs/prune",
                "/scheduler/market-ops",
                "/scheduler/daily-post-close",
                "/scheduler/maintenance",
                "/scheduler/ibkr-vm-control",
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
            payload = get_cached_json(("ops-summary", ""), 45, lambda: {
                "ok": True,
                **get_ops_summary(),
            })
            return jsonify(payload)
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

    @app.get("/paper-trade-attempts/recent")
    def paper_trade_attempts_recent():
        try:
            limit_raw = request.args.get("limit", "100")
            decision_stage = request.args.get("decision_stage")
            broker = request.args.get("broker")
            try:
                limit = max(1, min(1000, int(limit_raw)))
            except Exception:
                return jsonify({"ok": False, "error": "limit must be an integer"}), 400

            rows = get_recent_paper_trade_attempts(limit=limit, decision_stage=decision_stage, broker=broker)
            return jsonify({
                "ok": True,
                "count": len(rows),
                "limit": limit,
                "decision_stage": decision_stage,
                "broker": broker,
                "rows": rows,
            })
        except Exception as e:
            log_exception("paper-trade-attempts/recent failed", e, route="/paper-trade-attempts/recent")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.get("/paper-trade-attempts/rejections")
    def paper_trade_attempts_rejections():
        try:
            limit_raw = request.args.get("limit", "100")
            broker = request.args.get("broker")
            try:
                limit = max(1, min(1000, int(limit_raw)))
            except Exception:
                return jsonify({"ok": False, "error": "limit must be an integer"}), 400

            rows = get_recent_paper_trade_rejections(limit=limit, broker=broker)
            return jsonify({
                "ok": True,
                "count": len(rows),
                "limit": limit,
                "broker": broker,
                "rows": rows,
            })
        except Exception as e:
            log_exception("paper-trade-attempts/rejections failed", e, route="/paper-trade-attempts/rejections")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.get("/paper-trade-attempts/daily-summary")
    def paper_trade_attempts_daily_summary():
        try:
            limit_days_raw = request.args.get("limit_days", "7")
            broker = request.args.get("broker")
            try:
                limit_days = max(1, min(90, int(limit_days_raw)))
            except Exception:
                return jsonify({"ok": False, "error": "limit_days must be an integer"}), 400

            rows = get_paper_trade_attempt_daily_summary(limit_days=limit_days, broker=broker)
            return jsonify({
                "ok": True,
                "limit_days": limit_days,
                "broker": broker,
                "count": len(rows),
                "rows": rows,
            })
        except Exception as e:
            log_exception("paper-trade-attempts/daily-summary failed", e, route="/paper-trade-attempts/daily-summary")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.get("/paper-trade-attempts/hourly-summary")
    def paper_trade_attempts_hourly_summary():
        try:
            limit_days_raw = request.args.get("limit_days", "7")
            broker = request.args.get("broker")
            try:
                limit_days = max(1, min(90, int(limit_days_raw)))
            except Exception:
                return jsonify({"ok": False, "error": "limit_days must be an integer"}), 400

            rows = get_paper_trade_attempt_hourly_summary(limit_days=limit_days, broker=broker)
            return jsonify({
                "ok": True,
                "limit_days": limit_days,
                "broker": broker,
                "count": len(rows),
                "rows": rows,
            })
        except Exception as e:
            log_exception("paper-trade-attempts/hourly-summary failed", e, route="/paper-trade-attempts/hourly-summary")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/alpaca-api-logs/prune")
    def alpaca_api_logs_prune():
        payload = request.get_json(silent=True) or {}
        retention_days_raw = payload.get("retention_days", 30)
        try:
            retention_days = max(1, int(retention_days_raw))
        except Exception:
            return jsonify({"ok": False, "error": "retention_days must be an integer"}), 400

        try:
            deleted_count = prune_alpaca_api_logs(retention_days=retention_days)
            return jsonify({
                "ok": True,
                "retention_days": retention_days,
                "deleted_count": deleted_count,
            })
        except Exception as e:
            log_exception("alpaca-api-logs/prune failed", e, route="/alpaca-api-logs/prune")
            return jsonify({"ok": False, "error": str(e)}), 500
