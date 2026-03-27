from flask import jsonify, request



def register_dashboard_routes(app, *, get_dashboard_summary) -> None:
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
                "equity_curve": summary.get("equity_curve"),
                "insights": summary.get("insights"),
            })
        except Exception as e:
            print(f"dashboard-summary failed: {e}", flush=True)
            return jsonify({"ok": False, "error": str(e)}), 500