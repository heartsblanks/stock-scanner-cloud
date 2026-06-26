# stock-scanner-cloud

Cloud-hosted trading workflow system. Flask (Cloud Run) + PostgreSQL (Neon) + React/Vite dashboard. IBKR paper trading via a VM-hosted bridge service.

## Stack
- **Backend:** Python/Flask, deployed on Google Cloud Run
- **Database:** Neon PostgreSQL (production); schema in `schema.sql`
- **Frontend:** React/Vite in `dashboard-ui/`
- **Scheduler:** 3 Cloud Scheduler jobs → HTTP endpoints on Cloud Run
- **Broker:** IBKR via `ibkr_bridge/` Flask sidecar on a GCP VM

## Key directories
```
app.py                    # Flask composition root — wiring only, no feature logic
storage.py                # Compatibility facade over repository modules
core/                     # db.py, logging_utils.py, trade_math.py, paper_trade_config.py
orchestration/            # scan_context, paper_trade_context, scheduler_runtime, etc.
services/                 # scan_service.py, sync_service.py, trade_service.py
routes/                   # health, scans, trades, sync, reconcile, analysis, export, dashboard
repositories/             # scans_repo, trades_repo, broker_repo, reconcile_repo, ops_repo
brokers/                  # base.py, ibkr_adapter.py
ibkr/                     # ibkr_http, ibkr_client, ibkr_orders, ibkr_positions, paper, sync, reconcile
analytics/                # instruments.py, instruments.json, trade_analysis, signal_analysis, trade_scan
exports/                  # export_reports, export_daily_snapshot, github_export
scripts/                  # backfills, repair utilities, maintenance helpers
dashboard-ui/src/         # React pages and components
```

## Architecture rules
- Routes are thin — validate input, delegate to services
- Services own business logic
- Repositories own persistence logic; `storage.py` is a legacy compatibility shim only
- `app.py` is a composition root — keep it that way
- Schedulers call HTTP endpoints, not scripts directly
- All new instrument data goes through `analytics/instruments.json` + `analytics/instruments.py`
- Paper-trading risk config is env-driven via `core/paper_trade_config.py`

## Primary database tables
- `trade_lifecycles` — one row per trade, core analytics table
- `trade_events` — event-level log (OPEN, STOP_HIT, TARGET_HIT, EOD_CLOSE, etc.)
- `paper_trade_attempts` — one row per candidate/attempt, primary skip-reason diagnostic
- `scan_runs`, `signal_logs`, `broker_orders`, `broker_api_logs`
- `reconciliation_runs`, `reconciliation_details`

## Scheduler jobs (3 total, Cloud Scheduler free tier)
- `market-ops` → `POST /scheduler/market-ops` — intraday scan+sync, pre-close prep at 15:50 ET, EOD close at 15:55 ET
- `daily-post-close` → `POST /scheduler/daily-post-close` at 16:30 ET — sync, repair, rankings, reconcile, analysis, export
- `maintenance` → `POST /scheduler/maintenance` at 18:00 ET — log pruning

## Change management rule
Before implementing any new feature:
1. Classify as implemented / partial / missing against `ARCHITECTURE.md`
2. Identify impacted modules
3. Implement, then update `ARCHITECTURE.md` status

## What NOT to do
- Don't add logic to `app.py` beyond wiring
- Don't grow `storage.py` — use repository imports for new persistence
- Don't reintroduce `services/paper_trade_service.py` (removed intentionally)
- Don't commit `.env` files, `__pycache__/`, `node_modules/`, `dist/`, export folders
- Don't use external market data providers — scan path is IBKR-only
