

from flask import jsonify, request



def register_scan_routes(
    app,
    *,
    run_scan,
    run_scheduled_paper_scan,
):
    @app.post("/scan")
    def scan():
        payload = request.get_json(silent=True) or {}
        try:
            result = run_scan(payload)
            if isinstance(result, tuple) and len(result) == 2:
                body, status_code = result
                return jsonify(body), status_code
            if isinstance(result, dict):
                return jsonify(result)
            return jsonify({"ok": True, "result": result})
        except Exception as e:
            print(f"scan failed: {e}", flush=True)
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/scheduled-paper-scan")
    def scheduled_paper_scan():
        payload = request.get_json(silent=True) or {}
        try:
            result = run_scheduled_paper_scan(payload)
            if isinstance(result, tuple) and len(result) == 2:
                body, status_code = result
                return jsonify(body), status_code
            if isinstance(result, dict):
                return jsonify(result)
            return jsonify({"ok": True, "result": result})
        except Exception as e:
            print(f"scheduled-paper-scan failed: {e}", flush=True)
            return jsonify({"ok": False, "error": str(e)}), 500