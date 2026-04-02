import time

from flask import jsonify, request
from core.logging_utils import log_exception



def register_dashboard_routes(app, *, get_dashboard_summary, get_alpaca_open_positions=None, get_risk_exposure_summary=None) -> None:
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
            payload = get_cached_json(("dashboard-summary", target_date or ""), 45, lambda: {
                "ok": True,
                "date": target_date,
                **_build_dashboard_summary_payload(get_dashboard_summary(target_date=target_date)),
            })
            return jsonify(payload)
        except Exception as e:
            log_exception("dashboard-summary failed", e, route="/dashboard-summary")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.get("/alpaca-open-positions")
    def alpaca_open_positions():
        try:
            if not get_alpaca_open_positions:
                return jsonify({"ok": False, "error": "Not implemented"}), 501

            positions = get_alpaca_open_positions()
            return jsonify({
                "ok": True,
                "positions": positions,
                "count": len(positions or []),
            })
        except Exception as e:
            log_exception("alpaca-open-positions failed", e, route="/alpaca-open-positions")
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
