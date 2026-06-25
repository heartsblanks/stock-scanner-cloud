import time

from flask import jsonify, request
from core.logging_utils import log_exception



def register_dashboard_routes(
    app,
    *,
    get_dashboard_summary,
    get_daily_dashboard_summary=None,
    get_trade_tuning_report=None,
    get_risk_exposure_summary=None,
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

    @app.get("/dashboard-summary")
    def dashboard_summary():
        try:
            target_date = request.args.get("date")
            broker = str(request.args.get("broker", "")).strip().upper() or None
            payload = get_cached_json(("dashboard-summary", f"{target_date or ''}|{broker or ''}"), 45, lambda: {
                "ok": True,
                "date": target_date,
                "broker": broker,
                **_build_dashboard_summary_payload(get_dashboard_summary(target_date=target_date, broker=broker)),
            })
            return jsonify(payload)
        except Exception as e:
            log_exception("dashboard-summary failed", e, route="/dashboard-summary")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.get("/dashboard-daily")
    def dashboard_daily():
        try:
            if not get_daily_dashboard_summary:
                return jsonify({"ok": False, "error": "Not implemented"}), 501

            target_date = request.args.get("date")
            broker = str(request.args.get("broker", "")).strip().upper() or None
            payload = get_cached_json(("dashboard-daily", f"{target_date or ''}|{broker or ''}"), 20, lambda: {
                "ok": True,
                **get_daily_dashboard_summary(target_date=target_date, broker=broker),
            })
            return jsonify(payload)
        except Exception as e:
            log_exception("dashboard-daily failed", e, route="/dashboard-daily")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.get("/trade-tuning-report")
    def trade_tuning_report():
        try:
            if not get_trade_tuning_report:
                return jsonify({"ok": False, "error": "Not implemented"}), 501

            broker = str(request.args.get("broker", "")).strip().upper() or None
            try:
                limit_days = max(1, min(90, int(request.args.get("limit_days", "7"))))
                limit = max(1, min(100, int(request.args.get("limit", "20"))))
            except Exception:
                return jsonify({"ok": False, "error": "limit_days and limit must be integers"}), 400

            return jsonify({
                "ok": True,
                **get_trade_tuning_report(limit_days=limit_days, broker=broker, limit=limit),
            })
        except Exception as e:
            log_exception("trade-tuning-report failed", e, route="/trade-tuning-report")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.get("/risk-exposure-summary")
    def risk_exposure_summary():
        try:
            if not get_risk_exposure_summary:
                return jsonify({"ok": False, "error": "Not implemented"}), 501

            payload = get_cached_json(("risk-exposure-summary", ""), 30, lambda: {
                "ok": True,
                "summary": get_risk_exposure_summary(),
            })
            return jsonify(payload)
        except Exception as e:
            log_exception("risk-exposure-summary failed", e, route="/risk-exposure-summary")
            return jsonify({"ok": False, "error": str(e)}), 500


def _build_dashboard_summary_payload(summary: dict) -> dict:
    return {
        "summary": summary.get("summary"),
        "top_symbols": summary.get("top_symbols"),
        "mode_performance": summary.get("mode_performance"),
        "exit_reason_breakdown": summary.get("exit_reason_breakdown"),
        "external_exit_summary": summary.get("external_exit_summary"),
        "hourly_performance": summary.get("hourly_performance"),
        "hourly_outcome_quality": summary.get("hourly_outcome_quality"),
        "strategy_hourly_outcome_quality": summary.get("strategy_hourly_outcome_quality"),
        "equity_curve": summary.get("equity_curve"),
        "insights": summary.get("insights"),
    }
