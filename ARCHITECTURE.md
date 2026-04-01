

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
- **Database:** Neon-hosted PostgreSQL
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
 PostgreSQL (Neon)               Snapshot / GitHub Backup
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
   - Reconciliation, sync, lifecycle, and dashboard state should be derived from PostgreSQL and broker API data, not operational CSV files.
   - Export artifacts should be generated from PostgreSQL-backed data and reports, not from operational CSV logs.

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
- `services/paper_trade_service.py` *(currently placeholder / not materially implemented)*

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

### Repository and analysis placeholders
The following packages/files exist in the repository but are not currently active architectural layers:
- `repositories/*` *(placeholder repository abstraction; current code uses `storage.py` directly)*
- `analysis/*` *(placeholder package; current analysis logic is implemented in root-level modules such as `trade_analysis.py` and `signal_analysis.py`)*
- `export_to_github.py` *(placeholder / not used by the active export flow)*
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

Current code reality:
- `app.py` is still more than a thin composition root and contains significant orchestration, helper logic, and some directly-declared endpoints
- route modules are in use, but responsibility is not yet fully separated from `app.py`

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

Current code reality:
- `services/scan_service.py`, `services/sync_service.py`, and `services/trade_service.py` are active and important
- `services/paper_trade_service.py` is currently a placeholder and should not be treated as an active runtime component yet

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

Current code reality:
- `storage.py` is the effective persistence and query layer for the application
- the `repositories/` package is not yet used as the primary data-access abstraction

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
- broker-side close detection when a separate close order (for example EOD/manual close) fills after the original bracket flow

### 9.3 Trade key strategy
A stable trade key should be derived from:
1. broker parent order id when available
2. broker order id otherwise
3. symbol as fallback only when no better identifier exists

### 9.4 Lifecycle state transitions
- OPEN created when paper order placement is successful
- CLOSED updated when exit is confirmed or forced
- CLOSED must also be updated when Alpaca position flattening is detected through a separate broker-side exit order that is not the original TP/SL child leg

### 9.5 Current status
**Implemented with one remaining edge-case investigation**
- lifecycle write hooks are present in scan, sync, and close-related flows
- historical lifecycle backfill and repair utilities exist
- delayed broker-side close fills are now materially handled through sync, reconciliation, and repair flows
- stale OPEN lifecycle rows caused by delayed Friday EOD/manual closes have been repaired in production data
- auto-heal support now exists for leftover broker positions detected during sync
- one remaining edge case still requires investigation: a newer trade can be locally closed while the corresponding broker-side exit order is not yet uniquely recoverable during reconciliation for that exact parent order

### 9.6 Delayed broker-side close fills and leftover-position recovery

This edge case has been materially improved.

Implemented behavior now includes:
- sync logic detects when broker positions are no longer open even if the original TP/SL leg did not record the close locally
- sync logic can identify separate filled broker-side exit orders that flattened a position outside the original bracket leg flow
- lifecycle rows are updated to CLOSED using the detected exit time, exit price, and an external/manual close style exit reason
- reconciliation treats `MANUAL_CLOSE` and `EXTERNAL_EXIT` as equivalent for comparison purposes
- sync can auto-heal leftover Alpaca positions when the database shows no open paper trades but Alpaca still shows an open position

Remaining investigation:
- if multiple historical trades exist for the same symbol, the system must continue ensuring that a delayed broker-side exit is mapped to the correct parent trade
- one observed AAPL case still remains as a reconciliation mismatch where the database records a local close but reconciliation cannot yet recover a unique broker-side exit order for that exact 1-share parent trade

Primary implementation areas:
- `alpaca_sync.py`
- `services/sync_service.py`
- `alpaca_reconcile.py`

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
- symbol-level cooldown based on consecutive losses
- adaptive position sizing based on recent performance

### 10.6 Current status
**Partially implemented in trading logic**
- the system historically used a fixed hard cap approach
- the codebase now includes a dynamic 50%-allocation / 10-position sizing model
- current scan flow also includes symbol cooldown logic and adaptive sizing hooks
- advanced guardrails such as daily loss cutoff, ATR-based sizing, and calibration analytics remain pending

### 10.7 Adaptive Risk and Cooldown Model

To improve capital preservation and strategy robustness, the system should implement a dynamic risk adjustment model based on recent trade outcomes.

#### 10.7.1 Symbol-level cooldown

Rules:
- track consecutive losing trades per symbol
- if consecutive losses exceed a threshold (e.g., 2 losses):
  - skip new trades for that symbol for a cooldown period

Cooldown parameters:
- `cooldown_loss_threshold = 2`
- `cooldown_period_minutes = 60` (configurable)

Expected behavior:
- prevents repeated losses on the same symbol during adverse conditions
- reduces overtrading on weak setups

#### 10.7.2 Adaptive position sizing

Position size should be adjusted based on recent performance.

Suggested model:
- base position size = dynamic per_trade_notional
- apply multiplier based on performance:

Example multipliers:
- after 2 consecutive losses → reduce size to 50%
- after 3+ consecutive losses → reduce size to 25%
- after recovery (winning trade) → reset to 100%

Formula:
- `adjusted_notional = per_trade_notional * performance_multiplier`

#### 10.7.3 Data requirements

The following data is required:
- trade history grouped by symbol
- last N trades per symbol
- consecutive win/loss streak tracking

Primary source:
- `trade_lifecycles` table

#### 10.7.4 Integration points

This logic should be applied in:
- `services/scan_service.py` (candidate evaluation and sizing)

Optional enforcement layer:
- `paper_alpaca.py` (final validation before order placement)

#### 10.7.5 Current status

**Partially implemented**
- symbol-level cooldown and adaptive sizing hooks have been added in scan orchestration
- current implementation should still be validated during live/runtime trading cycles and refined over time

---

### 10.8 Advanced Risk Controls (Future Enhancements)

The following enhancements are planned to further improve risk management and strategy robustness.

#### 10.8.1 Daily loss cutoff

Rules:
- stop trading for the day if account drawdown exceeds a threshold

Suggested thresholds:
- `daily_loss_cutoff_pct = 0.02` (2%)
- optional aggressive mode: `0.03` (3%)

Behavior:
- no new trades should be placed after threshold breach
- existing trades continue to be managed normally
- trading resumes next trading day

Data requirements:
- starting equity of the day
- current realized + unrealized PnL

Integration points:
- `scan_service.py` (pre-trade guardrail)
- optional enforcement in `app.py` before scan execution

Status:
Partially implemented
- UI guardrail exists
- Backend enforcement in scan_service exists
- Needs runtime validation + refinement

---

#### 10.8.2 Volatility-based sizing (ATR)

Objective:
- adjust stop distance and position sizing based on market volatility

Approach:
- calculate ATR (Average True Range) per symbol
- define stop distance as a multiple of ATR:
  - `stop_distance = ATR * multiplier`

Example:
- low volatility → tighter stop → larger position size
- high volatility → wider stop → smaller position size

Benefits:
- normalizes risk across instruments
- prevents oversized risk in volatile stocks

Integration points:
- `scan_service.py` (candidate sizing logic)
- optional pre-computed ATR cache layer

Status:
**Not implemented**

---

#### 10.8.3 Confidence calibration (ML-style)

Objective:
- align strategy confidence scores with actual historical performance

Approach:
- track:
  - confidence at entry
  - actual outcome (win/loss, pnl)
- evaluate:
  - win rate vs confidence buckets (e.g., 70–80, 80–90, 90+)

Usage:
- adjust position sizing:
  - higher confidence → slightly higher allocation
  - lower confidence → reduced allocation

Example:
- `confidence_multiplier = f(confidence_score)`

Data requirements:
- historical trade outcomes from `trade_lifecycles`
- stored confidence score per trade

Integration points:
- `scan_service.py` (final sizing adjustment)
- analytics layer for calibration reporting

Status:
**Not implemented**

---

## 11. Scheduler Architecture

Cloud Scheduler triggers operational HTTP endpoints on Cloud Run.

### Current scheduled jobs
- `market-ops`
- `daily-post-close`
- `maintenance`

### Current consolidated job design

#### `market-ops`
- single intraday scheduler endpoint: `POST /scheduler/market-ops`
- current cron: `5,15,25,35,45,55 9-15 * * 1-5 (America/New_York)`
- intentionally no-ops on the early `9:05`, `9:15`, and `9:25` ticks
- from `9:35` through `15:45`, runs intraday sync and scheduled scans on a 10-minute offset cadence
- at `15:55`, runs end-of-day close only

#### `daily-post-close`
- post-close scheduler endpoint: `POST /scheduler/daily-post-close`
- current cron: `30 16 * * 1-5 (America/New_York)`
- runs:
  1. sync
  2. reconciliation
  3. trade analysis
  4. signal analysis
  5. daily snapshot export

#### `maintenance`
- housekeeping scheduler endpoint: `POST /scheduler/maintenance`
- current cron: `0 18 * * * (America/New_York)`
- currently used for log pruning and future low-priority maintenance tasks

### Scheduler responsibilities
- scan market during session windows
- sync paper trade state
- close positions near end of day before market close
- reconcile broker vs local records after market close
- produce analysis outputs
- create daily snapshot backup
- perform periodic housekeeping without adding more Scheduler jobs

### Current status
**Implemented and consolidated**
- the production design now uses three Scheduler jobs total in order to stay within the Cloud Scheduler free tier
- orchestration logic lives in backend scheduler endpoints instead of many separate Scheduler jobs
- end-of-day flow is explicitly split into:
  - pre-close forced close in `market-ops`
  - post-close sync, reconciliation, analysis, and export in `daily-post-close`

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
**Substantially implemented, with a smaller set of remaining gaps**
- core dashboard analytics and monitoring UI are in place
- reconciliation summary, mismatch severity, detail table, and history table are implemented in UI structure
- system health controls such as manual refresh and manual reconciliation trigger are implemented
- risk / exposure / adaptive sizing visibility has been materially improved in backend and UI support
- data quality is now materially improved after lifecycle, sync, and reconciliation fixes
- Alpaca logs/errors section is now implemented
- focused views, drilldowns, refresh-state visibility, execution insights, and reconciliation/admin views are implemented
- hosted static deployment is active via Vercel, while the backend remains on Cloud Run
### 13.1 Implemented UI capabilities

The dashboard UI currently includes:
- summary cards for core trading metrics
- equity curve visualization
- open trades table
- trade lifecycle table
- system health section
- Alpaca vs DB open-position mismatch display
- mismatch severity label driven by backend reconciliation output
- reconciliation summary section
- reconciliation breakdown section
- manual refresh button
- manual re-run reconciliation button
- manual sync trigger in the trades view
- last updated timestamp
- last reconciliation status badge
- last reconciliation timestamp
- view-specific auto-refresh gated to U.S. market hours
- focused navigation views for:
  - overview
  - trades
  - reconciliation
  - broker logs
  - analytics
- deep-linkable focused views using URL query state
- `paper_trade_attempts` analytics for execution/skip diagnostics

### 13.2 Remaining UI work

Pending dashboard UI enhancements:
- improved non-blocking toast component instead of inline message banner where still applicable
- per-widget loading and error states in every remaining dense section
- richer status indicators for backend jobs and sync health
- final verification that all reconciliation and risk widgets are fed by production-complete backend data during live sessions

### 13.3 UI next implementation order

Recommended next UI implementation order:
1. improve execution and scheduler health visibility
2. improve backend job / sync health indicators
3. add per-widget loading and error states where still missing
4. replace remaining inline status banners with non-blocking toast behavior
---

## 14. Export and Backup Architecture

### Daily snapshot flow
1. export current reports and DB snapshots into a staging directory
2. clone GitHub backup repository into a separate temp repo directory
3. copy staged export files into cloned repository
4. commit and push if there are changes

### Design requirement
The export flow must never mutate live operational data.
It should only create DB-derived snapshots and generated report copies.

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
**Implemented across storage, API, and UI, with one remaining operational edge case**
- reconciliation runs and mismatch detail APIs exist
- reconciliation summary, detail, and history views exist in the dashboard UI
- reconciliation is now database-first and no longer depends on operational CSV pairing for source-of-truth trade matching
- external broker-side exits are materially recovered and compared correctly
- `MANUAL_CLOSE` and `EXTERNAL_EXIT` are normalized as equivalent reconciliation outcomes
- one remaining mismatch pattern has been observed where a local close exists but reconciliation cannot recover a unique broker-side exit order for the exact parent order

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
**Partially implemented, but improved operationally**
- DB-level Alpaca logging exists
- Cloud Run logs are useful for runtime debugging
- reconciliation observability in the dashboard now exists
- sync and auto-heal behavior can now be inspected through endpoint responses and runtime logs
- dashboard views for Alpaca API logs/errors now exist, but deeper runtime alerting and correlation can still improve

---

## 17. Security and Configuration

### Key environment/config values
- `GITHUB_TOKEN`
- `GITHUB_OWNER`
- `GITHUB_REPO`
- `GITHUB_BRANCH`
- `DATABASE_URL`
- `DB_SCHEMA`
- Alpaca API keys
- export path variables

### Security requirements
- secrets must come from environment or secret manager, never git
- frontend `.env` files should not be committed
- GitHub token must support repository write access

### Current status
**Implemented operationally**
- Cloud Run now uses Secret Manager-backed secrets for sensitive runtime configuration
- Neon pooled PostgreSQL is the active production database
- legacy Cloud SQL resources may still exist temporarily for rollback or teardown, but are no longer part of the active runtime path

---

## 18. Implemented vs Missing

### Fully implemented
- Cloud Run backend deployment
- Cloud Build configuration
- consolidated Cloud Scheduler architecture with three production jobs
- PostgreSQL schema for core operational tables
- Alpaca API DB logging
- daily snapshot export to GitHub
- React dashboard multi-view UI
- reconciliation summary/detail/history UI structure
- reconciliation run and mismatch APIs
- Neon production database migration
- Secret Manager-backed production secrets
- `paper_trade_attempts` persistence and dashboard analytics

### Partially implemented
- trade lifecycle runtime validation across live market flows and remaining edge-case cleanup
- dynamic account-aware position sizing and adaptive sizing controls
- observability UI sections
- scheduler/EOD-close runtime verification and alerting confidence
- repository documentation and cleanup
- separation of concerns between `app.py`, route modules, and service modules
- lifecycle handling for delayed broker-side close fills outside the original bracket order tree

### Missing
- volatility-based sizing (ATR)
- confidence calibration analytics
- retention/cleanup policy for growing DB tables and exported artifacts
- deeper verification/repair path for the rare case where a local close exists but reconciliation cannot recover a unique broker exit order for the exact parent trade

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

1. investigate and fix the final reconciliation edge case where a local close exists but no unique broker-side exit order is recovered for the exact parent trade
2. verify `trade_lifecycles` population during a full real market-driven open/close cycle after the latest sync and reconciliation fixes
3. verify `close-paper-positions-eod` scheduler execution and runtime behavior against delayed fills at the next session open
4. add dashboard sections for Alpaca logs/errors and remaining operational polish
5. finalize static hosting deployment for the dashboard UI
6. document runtime environment variables and deployment steps more clearly
7. define retention policy for large operational tables and exported snapshots
8. continue reducing orchestration and direct endpoint complexity inside `app.py`

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
