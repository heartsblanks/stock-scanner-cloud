from flask import jsonify, request
from core.logging_utils import log_exception


def register_legacy_reconcile_routes(
    app,
    *,
    build_reconcile_now_response,
    build_reconciliation_runs_response,
    run_reconciliation,
    upload_file_to_gcs,
    reconciliation_bucket,
    reconciliation_object,
    safe_insert_reconciliation_run,
    get_reconciliation_runs,
):
    @app.route("/reconcile-now", methods=["POST"])
    def reconcile_now():
        try:
            result = build_reconcile_now_response(
                run_reconciliation=run_reconciliation,
                upload_file_to_gcs=upload_file_to_gcs,
                reconciliation_bucket=reconciliation_bucket,
                reconciliation_object=reconciliation_object,
                safe_insert_reconciliation_run=safe_insert_reconciliation_run,
            )
            return jsonify(result)
        except Exception as exc:
            log_exception("reconcile-now failed", exc, route="/reconcile-now")
            return jsonify({"ok": False, "error": str(exc)}), 500

    @app.route("/reconciliation-runs", methods=["GET"])
    def reconciliation_runs():
        try:
            limit = int(request.args.get("limit", 20))
            return jsonify(
                build_reconciliation_runs_response(
                    limit=limit,
                    get_reconciliation_runs=get_reconciliation_runs,
                )
            )
        except Exception as exc:
            log_exception("reconciliation-runs failed", exc, route="/reconciliation-runs")
            return jsonify({"ok": False, "error": str(exc)}), 500
