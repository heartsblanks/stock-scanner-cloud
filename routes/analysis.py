from pathlib import Path

from flask import jsonify
from core.logging_utils import log_exception, log_warning


def register_analysis_routes(
    app,
    *,
    run_trade_analysis,
    upload_analysis_file_to_gcs,
    trade_analysis_bucket,
    trade_analysis_summary_object,
    trade_analysis_paired_object,
    run_signal_analysis,
    upload_signal_analysis_file_to_gcs,
    signal_analysis_bucket,
    signal_analysis_summary_object,
    signal_analysis_rows_object,
) -> None:
    @app.post("/analyze-paper-trades")
    def analyze_paper_trades():
        try:
            summary_rows, paired_rows, unmatched_closes = run_trade_analysis()
        except Exception as e:
            log_exception("Paper trade analysis failed", e, route="/analyze-paper-trades")
            return jsonify({"ok": False, "error": str(e)}), 500

        summary_gcs_uri = ""
        paired_gcs_uri = ""

        try:
            summary_gcs_uri = upload_analysis_file_to_gcs(
                Path("trade_analysis_summary.csv"),
                trade_analysis_bucket,
                trade_analysis_summary_object,
            )
        except Exception as e:
            log_warning("Paper trade analysis summary upload failed", route="/analyze-paper-trades", error=str(e))

        try:
            paired_gcs_uri = upload_analysis_file_to_gcs(
                Path("trade_analysis_paired_trades.csv"),
                trade_analysis_bucket,
                trade_analysis_paired_object,
            )
        except Exception as e:
            log_warning("Paper trade analysis paired upload failed", route="/analyze-paper-trades", error=str(e))

        return jsonify({
            "ok": True,
            "summary_row_count": len(summary_rows),
            "paired_row_count": len(paired_rows),
            "unmatched_close_count": len(unmatched_closes),
            "summary_gcs_uri": summary_gcs_uri,
            "paired_gcs_uri": paired_gcs_uri,
        })

    @app.post("/analyze-signals")
    def analyze_signals():
        try:
            summary_rows, signal_rows = run_signal_analysis()
        except Exception as e:
            log_exception("Signal analysis failed", e, route="/analyze-signals")
            return jsonify({"ok": False, "error": str(e)}), 500

        summary_gcs_uri = ""
        rows_gcs_uri = ""

        try:
            summary_gcs_uri = upload_signal_analysis_file_to_gcs(
                Path("signal_analysis_summary.csv"),
                signal_analysis_bucket,
                signal_analysis_summary_object,
            )
        except Exception as e:
            log_warning("Signal analysis summary upload failed", route="/analyze-signals", error=str(e))

        try:
            rows_gcs_uri = upload_signal_analysis_file_to_gcs(
                Path("signal_analysis_rows.csv"),
                signal_analysis_bucket,
                signal_analysis_rows_object,
            )
        except Exception as e:
            log_warning("Signal analysis rows upload failed", route="/analyze-signals", error=str(e))

        return jsonify({
            "ok": True,
            "summary_row_count": len(summary_rows),
            "signal_row_count": len(signal_rows),
            "summary_gcs_uri": summary_gcs_uri,
            "rows_gcs_uri": rows_gcs_uri,
        })
