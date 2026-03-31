

from flask import jsonify, request
from logging_utils import log_exception



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
            log_exception("scan failed", e, route="/scan")
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
            log_exception("scheduled-paper-scan failed", e, route="/scheduled-paper-scan")
            return jsonify({"ok": False, "error": str(e)}), 500
