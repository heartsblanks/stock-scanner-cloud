

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
                **summary,
            })
        except Exception as e:
            print(f"dashboard-summary failed: {e}", flush=True)
            return jsonify({"ok": False, "error": str(e)}), 500