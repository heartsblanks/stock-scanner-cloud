from datetime import datetime, timezone

from flask import jsonify


def register_reconcile_routes(
    app,
    *,
    run_reconciliation,
    upload_file_to_gcs,
    reconciliation_bucket,
    reconciliation_object,
    safe_insert_reconciliation_run,
) -> None:
    @app.post("/reconcile-paper-trades")
    def reconcile_paper_trades():
        try:
            reconciled_rows, output_path = run_reconciliation()
        except Exception as e:
            print(f"Paper reconciliation failed: {e}", flush=True)
            return jsonify({"ok": False, "error": str(e)}), 500

        gcs_uri = ""
        try:
            gcs_uri = upload_file_to_gcs(output_path, reconciliation_bucket, reconciliation_object)
        except Exception as e:
            print(f"Paper reconciliation upload failed: {e}", flush=True)

        safe_insert_reconciliation_run(
            run_time=datetime.now(timezone.utc),
            matched_count=len(reconciled_rows),
            unmatched_count=0,
            notes=f"local_output_path={output_path}; gcs_output_uri={gcs_uri}",
        )
        return jsonify({
            "ok": True,
            "row_count": len(reconciled_rows),
            "local_output_path": str(output_path),
            "gcs_output_uri": gcs_uri,
        })