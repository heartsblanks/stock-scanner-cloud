

# Stock Scanner Cloud Architecture

## 1. Purpose

This document defines the target architecture of the `stock-scanner-cloud` repository.

It is the system reference for:
- current design
- implemented components
- required components
- known gaps
- future enhancements

The goal is to make requirements explicit first, so that new work can be added intentionally and missing implementations can be identified quickly.

---

## 2. System Overview

`stock-scanner-cloud` is a cloud-hosted trading workflow system that:
- scans markets on a schedule
- generates paper trade candidates
- submits and manages Alpaca paper trades
- synchronizes open and closed trade state from Alpaca
- records trade, broker, reconciliation, and API log data in PostgreSQL
- exports daily snapshots and analysis outputs
- provides a backend API for analytics and operational control
- provides a frontend dashboard for monitoring, analytics, and review

Primary hosting and infrastructure:
- **Compute:** Google Cloud Run
- **Database:** Cloud SQL for PostgreSQL
- **Scheduling:** Google Cloud Scheduler
- **Storage/Exports:** local runtime export staging and GitHub snapshot backup
- **Frontend:** React/Vite dashboard UI
- **Broker integration:** Alpaca paper trading API

---

## 3. High-Level Architecture

```text
Market Scan Schedulers
        |
        v
   Flask API (Cloud Run)
        |
        +-------------------------------+
        |                               |
        v                               v
  Application Services            Export / Analysis Services
        |                               |
        v                               v
 PostgreSQL (Cloud SQL)          CSV / Snapshot / GitHub Backup
        |
        v
 Dashboard API Endpoints
        |
        v
 React Dashboard UI
```

---

## 4. Architectural Principles

1. **Database-first operational state**
   - Core operational data should live in PostgreSQL.
   - CSV files are for export, backup, compatibility, and analysis support.

2. **Routes are thin**
   - Route handlers should validate input and delegate to services.

3. **Services contain business logic**
   - Trading, sync, close, analysis, and export logic should live in service modules.

4. **Schedulers trigger APIs, not scripts directly**
   - Cloud Scheduler should call stable HTTP endpoints.

5. **Trade lifecycle is the analytics foundation**
   - OPEN and CLOSED states must be persisted in a dedicated lifecycle model.

6. **Snapshots and backups are additive**
   - Export and GitHub backup flows should create copies, not mutate live source data.

---

## 5. Repository Structure

### Backend root
- `app.py` — application wiring and top-level service/route integration
- `db.py` — PostgreSQL connection helpers
- `storage.py` — database read/write helpers and analytics queries
- `schema.sql` — database schema definition

### Domain/service modules
- `services/scan_service.py`
- `services/sync_service.py`
- `services/trade_service.py`
- `services/logging_service.py`
- `services/paper_trade_service.py`

### Route modules
- `routes/health.py`
- `routes/scans.py`
- `routes/trades.py`
- `routes/sync.py`
- `routes/reconcile.py`
- `routes/analysis.py`
- `routes/export.py`
- `routes/dashboard.py`

### Broker integration
- `alpaca/alpaca_http.py`
- `alpaca/alpaca_client.py`
- `alpaca/alpaca_orders.py`
- `alpaca/alpaca_positions.py`
- `paper_alpaca.py`
- `alpaca_sync.py`
- `alpaca_reconcile.py`

### Analytics and export
- `trade_analysis.py`
- `signal_analysis.py`
- `export_reports.py`
- `export_daily_snapshot.py`
- `github_export.py`

### Frontend
- `dashboard-ui/`
  - `src/pages/DashboardPage.jsx`
  - `src/components/*`
  - `src/api/*`

---

## 6. Core Runtime Components

### 6.1 Flask API Layer

The Flask app is the runtime entry point deployed on Cloud Run.

Responsibilities:
- expose operational endpoints
- expose scheduled endpoints
- expose analytics endpoints
- expose dashboard endpoints
- delegate implementation to service and storage layers

Examples of endpoint groups:
- scan endpoints
- sync endpoints
- trade endpoints
- reconciliation endpoints
- analysis endpoints
- export endpoints
- dashboard endpoints
- health endpoints

### 6.2 Service Layer

The service layer implements business workflows.

#### Scan service
Responsible for:
- running market scan flow
- producing trade candidates
- placing Alpaca paper trades
- recording scan output
- creating OPEN lifecycle records

#### Sync service
Responsible for:
- checking open paper trades against Alpaca
- detecting exits (stop, target, manual, broker-side closure)
- recording close events
- updating lifecycle records to CLOSED

#### Trade service
Responsible for:
- manual trade operations
- end-of-day position close flow
- writing trade events and lifecycle updates for forced closes

#### Logging service
Responsible for:
- structured logging helpers
- DB logging integration where applicable

---

## 7. External Integrations

### 7.1 Alpaca

Used for:
- paper order placement
- position reads
- position closes
- order status lookup
- order synchronization

All Alpaca requests should be logged into `alpaca_api_logs` where practical.

### 7.2 GitHub

Used for:
- daily export snapshot backup
- long-term export retention outside Cloud Run runtime filesystem

The GitHub export implementation should:
- clone existing repository history
- copy snapshot files into the cloned working tree
- commit only when there are changes
- push safely without force

---

## 8. Database Architecture

Primary database: PostgreSQL.

### 8.1 Primary tables

#### `scan_runs`
Stores scan execution output and metadata.

#### `trade_events`
Stores event-level trade log rows.
Examples:
- OPEN
- STOP_HIT
- TARGET_HIT
- MANUAL_CLOSE
- EOD_CLOSE

#### `trade_lifecycles`
Stores one row per trade lifecycle.
This is the primary analytics table.

Expected lifecycle states:
- OPEN
- CLOSED

Expected fields include:
- trade key
- symbol
- mode
- side / direction
- entry time / price
- exit time / price
- stop / target
- exit reason
- shares
- realized pnl
- realized pnl percent
- duration
- linked signal fields
- broker order references

#### `broker_orders`
Stores broker-side order snapshots relevant for audit and reconciliation.

#### `reconciliation_runs`
Stores summary-level reconciliation run results.

#### `reconciliation_details`
Stores row-level reconciliation comparisons.

#### `alpaca_api_logs`
Stores Alpaca request/response logging with fields including:
- logged_at
- method
- url
- params_json
- request_body_json
- status_code
- response_body
- success
- error_message
- duration_ms

---

## 9. Trade Lifecycle Design

Trade lifecycle is the core analytical abstraction.

### 9.1 Purpose
The lifecycle model merges trade open and close information into one normalized row per trade.

### 9.2 Sources of lifecycle updates
Lifecycle writes should occur from all relevant flows:
- scan/open flow
- sync/exit flow
- end-of-day close flow
- manual close flow when applicable

### 9.3 Trade key strategy
A stable trade key should be derived from:
1. broker parent order id when available
2. broker order id otherwise
3. symbol as fallback only when no better identifier exists

### 9.4 Lifecycle state transitions
- OPEN created when paper order placement is successful
- CLOSED updated when exit is confirmed or forced

### 9.5 Current status
**Partially implemented**
- lifecycle write hooks have been added into main service flows
- runtime validation is still required to confirm production population of `trade_lifecycles`

---

## 10. Risk Management and Position Sizing

### 10.1 Position sizing objective
The system should no longer rely on a fixed `$500` notional per trade as the long-term sizing model.

Target design:
- use **account-aware position sizing**
- enforce **maximum 10 concurrent positions**
- allow deployment of up to **50% of account equity** across open positions
- distribute remaining allocatable capital across remaining open slots
- continue to support stop/target-based trade management and symbol cooldown rules

### 10.2 Allocation model
Definitions:
- `max_positions = 10`
- `max_capital_allocation_pct = 0.50`
- `max_total_allocated_capital = account_equity * max_capital_allocation_pct`
- `remaining_allocatable_capital = max_total_allocated_capital - current_open_exposure`
- `remaining_position_slots = max_positions - current_open_positions`

Base sizing rule:
- `per_trade_notional = remaining_allocatable_capital / remaining_position_slots`

This model ensures:
- capital is distributed across the remaining available slots
- position size scales with account size
- position size becomes smaller as exposure is consumed
- portfolio allocation remains capped at 50% of account equity

### 10.3 Risk interpretation
Important distinction:
- **position notional** is not the same as **risk per trade**
- actual risk per trade depends on the distance between entry price and stop price

Formula:
- `risk_per_trade_dollars = abs(entry_price - stop_price) * shares`

Therefore the architecture should track both:
- notional allocation per trade
- actual stop-based dollar risk per trade

### 10.4 Take-profit interpretation
Take profit should continue to be based on the strategy-generated target price, not a fixed dollar amount.

Formula:
- `take_profit_dollars = abs(target_price - entry_price) * shares`

This means take profit per trade is variable and depends on:
- entry price
- target price
- number of shares

### 10.5 Additional controls
The sizing model should coexist with these controls:
- symbol-level cooldown after successive losses
- optional global daily loss guardrails
- optional max total open exposure guardrail
- skip trades when no remaining slots are available
- skip trades when remaining allocatable capital is not meaningful

### 10.6 Current status
**Defined in architecture, not fully implemented in trading logic yet**
- current system historically used a fixed hard cap approach
- target system is a 50%-allocation / 10-position dynamic sizing model

---

## 11. Scheduler Architecture

Cloud Scheduler triggers operational HTTP endpoints on Cloud Run.

### Current scheduled jobs
- `scheduled-paper-scan-openplus20`
- `scheduled-paper-scan-10min`
- `sync-paper-trades`
- `close-paper-positions-eod`
- `reconcile-paper-trades`
- `analyze-paper-trades`
- `analyze-signals`
- `export-daily-snapshot`

### Scheduler responsibilities
- scan market periodically during session windows
- sync paper trade exits
- close positions near end of day
- reconcile broker vs local records
- produce analysis outputs
- create daily snapshot backup

### Current status
**Implemented**

---

## 12. Analysis and Reporting

Analytics are generated from trade data and signal data.

### Current report families
- trade analysis summary
- trade paired trades output
- signal analysis summary
- signal analysis row output
- Alpaca reconciliation report

### Export destinations
- runtime export staging directories
- GitHub daily snapshot repository

### Current status
**Implemented**, with room for expansion

---

## 13. Dashboard Architecture

The dashboard is a React/Vite frontend backed by Flask API endpoints.

### Dashboard goals
- view summary metrics
- view open trades
- view lifecycle analytics
- view charts
- view insights
- later include logs and reconciliation visibility

### Current UI sections
- summary cards
- open trades table
- trade lifecycle table
- symbol performance chart
- mode performance chart
- hourly performance chart
- equity curve chart
- filters
- insight cards

### Backend dashboard summary returns
- summary
- top symbols
- mode performance
- exit reason breakdown
- hourly performance
- equity curve
- insights

### Current status
**Partially implemented**
- UI structure exists
- data quality depends on lifecycle table population
- reconciliation section not yet implemented
- Alpaca logs section not yet implemented
- hosted static deployment not yet finalized in architecture

---

## 14. Export and Backup Architecture

### Daily snapshot flow
1. export current reports and DB snapshots into a staging directory
2. clone GitHub backup repository into a separate temp repo directory
3. copy staged export files into cloned repository
4. commit and push if there are changes

### Design requirement
The export flow must never delete or truncate live operational CSV sources.
It should only create copies.

### Current status
**Implemented**
- manual endpoint tested successfully
- Cloud Scheduler job exists for daily execution
- operational monitoring still recommended

---

## 15. Reconciliation Architecture

Reconciliation compares local trade data and broker-side order/exit data.

### Main tables
- `reconciliation_runs`
- `reconciliation_details`
- `broker_orders`

### Actual schema notes
- `reconciliation_runs` uses `run_time`
- `reconciliation_details` uses `run_id`
- `broker_orders` does not contain a `broker` column in the current schema

### Current status
**Implemented in storage**, **not fully surfaced in UI**

---

## 16. Observability and Logging

### Logging types
- Flask / gunicorn runtime logs
- trade event logs
- broker order records
- Alpaca API logs in DB
- export failure logs
- reconciliation output

### Required observability goals
- identify failed scheduler runs quickly
- inspect Alpaca API failures and latency
- inspect reconciliation mismatch patterns
- inspect export and GitHub push failures

### Current status
**Partially implemented**
- DB-level Alpaca logging exists
- Cloud Run logs are useful for runtime debugging
- dashboard log views are still missing

---

## 17. Security and Configuration

### Key environment/config values
- `GITHUB_TOKEN`
- `GITHUB_OWNER`
- `GITHUB_REPO`
- `GITHUB_BRANCH`
- `DB_*` or database connection settings
- Alpaca API keys
- export path variables

### Security requirements
- secrets must come from environment or secret manager, never git
- frontend `.env` files should not be committed
- GitHub token must support repository write access

### Current status
**Implemented operationally**, documentation can improve

---

## 18. Implemented vs Missing

### Fully implemented
- Cloud Run backend deployment
- Cloud Build configuration
- Cloud Scheduler jobs for scan, sync, close, reconcile, analysis, and daily snapshot export
- PostgreSQL schema for core operational tables
- Alpaca API DB logging
- daily snapshot export to GitHub
- React dashboard base UI

### Partially implemented
- trade lifecycle persistence (hooks added, runtime proof still needed)
- dashboard analytics (depends on lifecycle population)
- observability UI sections
- reconciliation visibility in dashboard
- static dashboard hosting rollout
- repository documentation and cleanup

### Missing
- root `.gitignore` if not yet created
- finalized static hosting deployment for dashboard UI
- dashboard section for reconciliation runs/details
- dashboard section for Alpaca logs/errors
- dynamic account-aware position sizing using 50% max allocation and 10 max positions
- retention/cleanup policy for growing DB tables and exported artifacts
- architecture-aware README refresh

---

## 19. Repository Hygiene Requirements

### Must be committed
- source code
- schema
- Dockerfile
- cloudbuild config
- package manifests and lockfiles
- frontend source
- documentation files

### Must not be committed
- `__pycache__/`
- `*.pyc`
- `.venv/`
- `dashboard-ui/node_modules/`
- `dashboard-ui/dist/`
- `.DS_Store`
- `.git/`
- local `.env` files
- temporary export folders

---

## 20. Immediate Next Steps

1. implement account-aware dynamic position sizing with 50% max allocation and 10 max positions
2. validate `trade_lifecycles` population during a real market-driven open/close cycle
3. add a root `.gitignore` if missing
4. add dashboard sections for reconciliation and Alpaca logs
5. finalize static hosting deployment for the dashboard UI
6. document runtime environment variables and deployment steps more clearly
7. define retention policy for large operational tables and exported snapshots

---

## 21. Change Management Rule

Before implementing any new requirement:
1. update `ARCHITECTURE.md`
2. classify the change as implemented / partial / missing
3. identify impacted modules
4. implement code changes
5. validate runtime behavior
6. update documentation status

This rule exists so new requirements are captured before code changes and nothing is silently missed.