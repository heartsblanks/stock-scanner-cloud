"""
Slim Flask entry point for Vercel deployment.
Serves only the dashboard API — no IBKR, no scheduler, no scan logic.
"""
from __future__ import annotations

import os

from flask import Flask
from flask_cors import CORS

from core.db import healthcheck as db_healthcheck
from repositories.ops_repo import get_ops_summary
from repositories.trades_repo import (
    get_open_trade_events,
    get_closed_trade_events,
    get_recent_trade_event_rows,
    get_trade_lifecycles,
    get_trade_lifecycles_page,
    get_trade_lifecycle_summary_from_table,
    get_trade_tuning_report,
    get_latest_symbol_ranking_rows,
    get_dashboard_summary,
    get_daily_dashboard_summary,
    upsert_trade_lifecycle,
)
from repositories.scans_repo import get_latest_scan_summary
from repositories.reconcile_repo import (
    get_latest_reconciliation_summary,
    get_recent_reconciliation_mismatches,
    insert_reconciliation_run as safe_insert_reconciliation_run,
    insert_reconciliation_detail as safe_insert_reconciliation_detail,
)
from repositories.scans_repo import (
    get_recent_paper_trade_attempts,
    get_recent_paper_trade_rejections,
    get_paper_trade_attempt_daily_summary,
    get_paper_trade_attempt_hourly_summary,
)
from repositories.maintenance_repo import purge_all_test_data, purge_legacy_broker_data
from repositories.trades_repo import upsert_trade_lifecycle
from routes.health import register_health_routes
from routes.internal import register_internal_routes
from routes.reconcile import register_reconcile_routes
from routes.trades import register_trade_routes
from routes.dashboard import register_dashboard_routes

app = Flask(__name__)
CORS(app)

ENABLE_DB_LOGGING = os.getenv("ENABLE_DB_LOGGING", "false").lower() == "true"


def _ibkr_status_stub() -> dict:
    return {"ok": True, "ibkr_enabled": False, "bridge_enabled": False, "status": "NO_BRIDGE"}


register_health_routes(
    app,
    db_healthcheck=db_healthcheck,
    enable_db_logging=ENABLE_DB_LOGGING,
    get_ops_summary=get_ops_summary,
    get_recent_paper_trade_attempts=get_recent_paper_trade_attempts,
    get_recent_paper_trade_rejections=get_recent_paper_trade_rejections,
    get_paper_trade_attempt_daily_summary=get_paper_trade_attempt_daily_summary,
    get_paper_trade_attempt_hourly_summary=get_paper_trade_attempt_hourly_summary,
    get_symbol_rankings=get_latest_symbol_ranking_rows,
    get_ibkr_operational_status=_ibkr_status_stub,
    telegram_alerts_enabled=lambda: False,
    send_telegram_alert=lambda *_a, **_k: {"ok": False, "reason": "not_configured"},
    purge_all_test_data=purge_all_test_data,
    purge_legacy_broker_data=purge_legacy_broker_data,
    run_instrument_catalog_sync=lambda: {"ok": True, "skipped": True},
    run_symbol_eligibility_refresh=lambda *_a, **_k: {"ok": True, "skipped": True},
    run_market_data_cache_refresh=lambda *_a, **_k: {"ok": True, "skipped": True},
    get_market_data_cache_summary=lambda: {},
)

register_reconcile_routes(
    app,
    run_reconciliation=lambda: {"ok": True, "skipped": True, "reason": "reconciliation_runs_via_github_actions"},
    upload_file_to_gcs=lambda *_a, **_k: None,
    reconciliation_bucket="",
    reconciliation_object="",
    safe_insert_reconciliation_run=safe_insert_reconciliation_run,
    safe_insert_reconciliation_detail=safe_insert_reconciliation_detail,
    get_latest_reconciliation_summary=get_latest_reconciliation_summary,
    get_recent_reconciliation_mismatches=get_recent_reconciliation_mismatches,
)

register_trade_routes(
    app,
    append_trade_log=lambda *_a, **_k: None,
    safe_insert_trade_event=lambda *_a, **_k: None,
    safe_insert_broker_order=lambda *_a, **_k: None,
    close_all_paper_positions=lambda *_a, **_k: {"ok": True, "skipped": True},
    read_trade_rows_for_date=lambda *_a, **_k: [],
    find_instrument_by_symbol=lambda *_a, **_k: None,
    find_best_signal_match=lambda *_a, **_k: None,
    find_latest_open_trade=lambda *_a, **_k: None,
    infer_first_level_hit=lambda *_a, **_k: None,
    to_float_or_none=lambda v: float(v) if v is not None else None,
    parse_iso_utc=lambda s: s,
    get_open_trade_events=get_open_trade_events,
    get_closed_trade_events=get_closed_trade_events,
    get_recent_trade_event_rows=get_recent_trade_event_rows,
    get_latest_scan_summary=get_latest_scan_summary,
    get_trade_lifecycles=get_trade_lifecycles,
    get_trade_lifecycles_page=get_trade_lifecycles_page,
    get_trade_lifecycle_summary_from_table=get_trade_lifecycle_summary_from_table,
    get_open_positions_for_broker_name=lambda *_a, **_k: [],
    get_open_orders_for_broker_name=lambda *_a, **_k: [],
    get_open_state_for_broker_name=lambda *_a, **_k: {"positions": [], "orders": []},
    upsert_trade_lifecycle=upsert_trade_lifecycle,
)

register_dashboard_routes(
    app,
    get_dashboard_summary=get_dashboard_summary,
    get_daily_dashboard_summary=get_daily_dashboard_summary,
    get_trade_tuning_report=get_trade_tuning_report,
    get_risk_exposure_summary=lambda *_a, **_k: {"summary": {}},
)


register_internal_routes(app)

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
