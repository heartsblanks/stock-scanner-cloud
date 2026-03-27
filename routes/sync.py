

from flask import jsonify



def register_sync_routes(app, *, sync_paper_trades_handler) -> None:
    @app.post("/sync-paper-trades")
    def sync_paper_trades():
        try:
            result = sync_paper_trades_handler()
            if isinstance(result, tuple) and len(result) == 2:
                body, status_code = result
                return jsonify(body), status_code
            if isinstance(result, dict):
                return jsonify(result)
            return jsonify({"ok": True, "result": result})
        except Exception as e:
            print(f"sync-paper-trades failed: {e}", flush=True)
            return jsonify({"ok": False, "error": str(e)}), 500