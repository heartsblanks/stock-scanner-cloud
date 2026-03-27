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
    safe_insert_reconciliation_detail,
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

        unmatched_count = sum(1 for row in reconciled_rows if row.get("match_status") != "matched")

        safe_insert_reconciliation_run(
            run_time=datetime.now(timezone.utc),
            matched_count=len(reconciled_rows) - unmatched_count,
            unmatched_count=unmatched_count,
            notes=f"local_output_path={output_path}; gcs_output_uri={gcs_uri}",
        )
        run_id = None
        try:
            # fetch last inserted run id (simple approach)
            # assumes latest run corresponds to this execution
            from storage import fetch_one
            row = fetch_one(
                "SELECT id FROM reconciliation_runs ORDER BY id DESC LIMIT 1",
                {},
            )
            run_id = row["id"] if row else None
        except Exception as e:
            print(f"Failed to fetch reconciliation run id: {e}", flush=True)

        for row in reconciled_rows:
            try:
                safe_insert_reconciliation_detail(
                    run_id=run_id,
                    broker_parent_order_id=row.get("broker_parent_order_id"),
                    symbol=row.get("symbol"),
                    mode=row.get("mode"),
                    client_order_id=row.get("client_order_id"),
                    local_entry_timestamp_utc=None,
                    local_exit_timestamp_utc=None,
                    local_entry_price=row.get("local_entry_price") or None,
                    alpaca_entry_price=row.get("alpaca_entry_price") or None,
                    local_exit_price=row.get("local_exit_price") or None,
                    alpaca_exit_price=row.get("alpaca_exit_price") or None,
                    local_shares=row.get("local_shares") or None,
                    alpaca_entry_qty=row.get("alpaca_entry_qty") or None,
                    alpaca_exit_qty=row.get("alpaca_exit_qty") or None,
                    local_exit_reason=row.get("local_exit_reason"),
                    alpaca_exit_reason=row.get("alpaca_exit_reason"),
                    alpaca_exit_order_id=row.get("alpaca_exit_order_id"),
                    entry_price_diff=row.get("entry_price_diff") or None,
                    exit_price_diff=row.get("exit_price_diff") or None,
                    match_status=row.get("match_status"),
                )
            except Exception as e:
                print(f"Reconciliation detail insert failed: {e}", flush=True)
        return jsonify({
            "ok": True,
            "row_count": len(reconciled_rows),
            "local_output_path": str(output_path),
            "gcs_output_uri": gcs_uri,
        })