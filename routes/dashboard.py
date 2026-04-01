from flask import jsonify, request
from core.logging_utils import log_exception



def register_dashboard_routes(app, *, get_dashboard_summary, get_alpaca_open_positions=None, get_risk_exposure_summary=None) -> None:
    @app.get("/dashboard-summary")
    def dashboard_summary():
        try:
            target_date = request.args.get("date")
            summary = get_dashboard_summary(target_date=target_date)
            return jsonify({
                "ok": True,
                "date": target_date,
                "summary": summary.get("summary"),
                "top_symbols": summary.get("top_symbols"),
                "mode_performance": summary.get("mode_performance"),
                "exit_reason_breakdown": summary.get("exit_reason_breakdown"),
                "hourly_performance": summary.get("hourly_performance"),
                "hourly_outcome_quality": summary.get("hourly_outcome_quality"),
                "equity_curve": summary.get("equity_curve"),
                "insights": summary.get("insights"),
            })
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

            summary = get_risk_exposure_summary()
            return jsonify({
                "ok": True,
                "summary": summary,
            })
        except Exception as e:
            log_exception("risk-exposure-summary failed", e, route="/risk-exposure-summary")
            return jsonify({"ok": False, "error": str(e)}), 500
