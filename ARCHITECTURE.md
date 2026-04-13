

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

## 5. Current Roadmap

Architecture documentation is updated alongside implementation so the document remains a live system reference, not a stale design note.

### 5.1 Do Now

1. **Trading logic observability**
   - expand `paper_trade_attempts` reporting so the system explains by hour:
     - candidates
     - placements
     - non-placement reasons
     - placement-rate trends
   - surface these insights through backend endpoints and the dashboard

2. **Backend structure cleanup**
   - continue reducing orchestration gravity around `app.py`
   - avoid growing `storage.py`; prefer repository imports for new work
   - either implement or remove the placeholder `services/paper_trade_service.py`
   - reduce root-directory clutter by moving maintenance, repair, and backfill utilities into `scripts/`
   - continue the same cleanup for export-related modules by consolidating them under `exports/`
   - continue the same cleanup for trading analysis and scan logic by consolidating them under `analytics/`
   - fold the remaining top-level Alpaca wrappers into the existing `alpaca/` package so broker integration code lives in one place
   - consolidate scan, paper-trade, scheduler, and app-level orchestration helpers under `orchestration/`
   - remove dead root-level files such as unused export utilities
   - consolidate shared foundations such as DB access, structured logging, and trade math under `core/`
   - move the tradable instrument universe into a dedicated registry file instead of embedding it directly inside scan logic

3. **Ops / cloud cleanup**
   - keep Neon as the active production database
   - remove old Cloud SQL and migration leftovers after a short stability window
   - keep deployment and scheduler runbooks aligned with production reality

### 5.2 Next

1. **Performance improvements**
   - cache expensive dashboard read models lightly
   - keep heavy sections lazy-loaded and view-scoped
   - monitor Neon usage against the consolidated scheduler cadence

2. **UI / product improvements**
   - expand overview and analytics visibility for scheduler health, placement rate by hour, and operational alerts
   - keep dense views filterable and easier to scan

3. **Data / repository improvements**
   - add retention/pruning for high-volume operational data
   - introduce read-model helpers or database views where dashboard aggregates justify them

### 5.3 Later

1. **More backend architecture cleanup**
2. **Strategy refinement based on live data**
3. **Advanced analytics and calibration tooling**

### 5.4 Active implementation

Current active work:
- continue expanding intraday quality reporting beyond the newly implemented hourly placement-rate, top non-placement reason reporting, hourly candidate/placed/non-placed attempt analytics, ET-based outcome quality by entry hour, and the merged conversion-vs-quality comparison in the dashboard command view
- keep backend and dashboard analytics aligned as new attempt reporting slices land
- use the architecture document as the running record for completed and pending observability work
- keep paper-trading limits flag-driven so equity allocation and max-position behavior can change via environment/config instead of code edits

Current strategy status:
- completed: `EXTERNAL_EXIT` is reported separately so forced EOD/manual exits no longer distort strategy-only quality views
- completed: post-noon caution is stronger via an additional confidence time penalty from `12:00 PM ET` onward
- completed: short setups now get an additional midday time penalty starting at `11:00 PM ET`, with a stronger bump from `12:00 PM ET`
- completed: short setups now require a higher minimum confidence threshold than longs
- completed: paper-trade sizing is clamped by the hard `$10,000` `ALPACA_MAX_NOTIONAL` cap
- completed: `trade_lifecycles.mode` is preserved across later sync/close/reconcile updates so category analysis stays intact
- pending observation: review whether the `Price is above OR high and above VWAP.` breakout rule is too blunt after a few more live sessions
- pending observation: monitor whether `No meaningful allocatable capital remains.` mostly disappears now that the oversized-order sizing bug is fixed

Market data migration strategy:
- current recommendation: stay on Twelve Data for now while strategy quality and symbol-universe quality continue to improve
- why: live comparisons against recent real trades showed that Twelve Data and Alpaca were close on actual entry-minute prices, so provider mismatch did not look like the main driver of recent losing trades
- why not Alpaca free `IEX`: live comparisons also showed that Alpaca free `IEX` bars are often sparser in the opening-range window, especially on thinner symbols, which can make opening-range and VWAP logic less reliable than the current setup
- future implementation 1: migrate trading/account/order integration to `alpaca-py` first, without changing the scanner's market-data source
- future implementation 2: only evaluate Alpaca as the primary market-data source through a side-by-side validation phase, ideally on paid `SIP` rather than free `IEX`
- upgrade triggers for paid market data: repeated evidence that entry timing is meaningfully late despite otherwise stable strategy behavior, repeated chart-review cases where missed/late entries trace back to candle timing, or enough strategy stability that data quality becomes the next obvious bottleneck
- if migration happens later, the intended order is:
- `1.` keep Twelve Data live while adding Alpaca market-data adapters in parallel
- `2.` compare OR completeness, VWAP, benchmark direction, and entry timing side by side for multiple sessions
- `3.` cut over only after the higher-quality feed clearly improves scan quality

Parallel IBKR evaluation strategy:
- implementation approach: keep the current Alpaca paper-trading path stable while building IBKR support in parallel for comparison only
- branch strategy: do IBKR work on a dedicated branch so broker-abstraction and bridge changes do not destabilize the current production path
- hosting approach: use a GCP VM for the IBKR sidecar stack because Cloud Run is not a good fit for a persistent IB Gateway session
- current implementation status: the first broker-abstraction layer is now in place, with Alpaca wired behind a generic paper-broker adapter and the IBKR side now upgraded from a placeholder to a bridge-based contract that expects a VM-hosted HTTP service
- current implementation status: a minimal `ibkr_bridge/` Flask service scaffold now exists in the repo so the GCP VM side has a concrete starting point and endpoint contract
- current implementation status: VM deployment scaffolding now exists under `ibkr_bridge/systemd/` with a service unit, env template, and bridge runbook for the GCP VM path
- current implementation status: the first real IBKR bridge read-path is now implemented for account, positions, and open-order reads; write-path endpoints remain intentionally deferred until IB Gateway connectivity is verified on the VM
- cost-control plan: the IBKR VM should not run 24/7; it should be started and stopped by dedicated Cloud Scheduler jobs around the trading window, while IB Gateway login remains a manual daily step for now
- operational compromise for now: keep the VM public IP attached while IB Gateway login is still a manual operator task from a laptop or phone; once admin access is moved to IAP or another private-only path, the public IP should be removed to reduce standing cost
- current implementation status: the IBKR bridge service is being decoupled from raw Gateway port readiness so the dashboard can surface `LOGIN_REQUIRED` and `MARKET_DATA_UNAVAILABLE` states instead of only showing the VM as down
- current implementation status: a repeatable VM desktop bootstrap path is now part of the repo so `xrdp`, `xfce`, and headless `Xvfb` support can be applied consistently on the IBKR VM
- current implementation status: the bridge now also supports the first operational write-path actions for cancel-by-symbol and market close-position flows; paper bracket placement remains deferred
- current implementation status: holiday-aware VM control is now implemented in the main Cloud Run app through `POST /scheduler/ibkr-vm-control`, which reuses the NYSE calendar before starting the VM on trading days
- current implementation status: IBKR login-required alerting now exists through `POST /scheduler/ibkr-login-alert`, which can call a webhook-backed Signal relay when the broker session needs operator login
- current implementation status: the persistence model is being extended to tag paper-trading rows by broker so Alpaca and IBKR orders, trade events, lifecycles, and attempts can be compared cleanly from the database
- current implementation status: the scan flow is now being split into two true parallel tracks instead of one shared candidate set, so Alpaca continues to evaluate from Twelve Data while IBKR evaluates from IBKR market data before placing to its own paper account
- target architecture:
- `1.` Cloud Run remains the main app, dashboard, scheduler, and Neon-backed API
- `2.` a GCP VM runs IB Gateway plus a small authenticated IBKR bridge service
- `3.` Cloud Run reaches the VM over an internal path using a Serverless VPC Access connector plus the VM internal IP, rather than a public bridge URL
- `4.` the main app talks to the bridge service rather than directly to IB Gateway
- scan-path split:
- `1.` Alpaca paper flow uses Twelve Data candles plus Alpaca paper placement
- `2.` IBKR paper flow uses IBKR market data plus IBKR paper placement
- `3.` both flows run from the same scheduler invocation and persist separately with broker-tagged records for later comparison
- VM operating window:
- `1.` start the VM at `9:15 AM ET`
- `2.` keep it available through the close and immediate post-close comparison window
- `3.` stop the VM at `5:00 PM ET`
- `4.` keep a manual IB Gateway login fallback for now, but auto-start the VM, IB Gateway process, and bridge so operator intervention is only needed when readiness fails
- `5.` keep a static public VM IP for operator access so phone/laptop RDP does not change across stop/start cycles
- `6.` keep the bridge reachable even when the broker session is not yet ready, so the dashboard can distinguish process health from broker-login readiness
- holiday handling note:
- `1.` plain Cloud Scheduler cron can handle weekdays and timezone shifts but not exchange holidays
- `2.` the current design now uses a calendar-aware Cloud Run controller at `POST /scheduler/ibkr-vm-control`
- `3.` start requests skip cleanly on NYSE holidays and weekends; stop requests remain safe idempotent calls for cost control
- operator flow note:
- `1.` Cloud Run and scheduler automation should bring the VM-side stack up automatically
- `2.` the readiness check is the gate for "usable broker session" rather than simple process uptime
- `3.` only if readiness fails should the operator log into IB Gateway, which can be done from a laptop or phone over RDP
- expected bridge contract:
- `GET /account`
- `GET /positions`
- `GET /orders/open`
- `GET /orders/{order_id}`
- `GET /orders/{order_id}/sync`
- `POST /orders/paper-bracket`
- `POST /orders/cancel-by-symbol`
- `POST /positions/close`
- implementation order:
- `1.` add a broker interface/abstraction layer in the app
- `2.` refactor the existing Alpaca path to implement that interface without changing behavior
- `3.` build the IBKR bridge on the VM for paper trading only
- `4.` add an IBKR adapter in the app for account, orders, positions, cancel, close, sync, and reconcile operations
- `5.` keep Alpaca as the primary paper broker while IBKR runs in shadow mode
- `6.` log broker-side comparisons for market data, order acceptance, fill behavior, and lifecycle outcomes
- comparison goals:
- `1.` compare market-data completeness and opening-range reliability
- `2.` compare paper fill behavior and rejection patterns
- `3.` compare operational overhead, stability, and cost
- `4.` compare broker-tagged DB records directly so dashboard and SQL analysis can distinguish Alpaca vs IBKR behavior without code-specific heuristics
- decision point: only consider changing direction after enough parallel sessions show a clear advantage in data quality or execution quality

### 5.5 Operational checklist

Daily:
- review `Overview` for attention items, execution insights, conversion vs quality, reconciliation status, and recent broker/API failures
- review `Analytics` for hourly attempt outcomes, hourly outcome quality, and whether high-conversion hours are also high-quality hours
- review `Trades` and use manual sync only when local trade state appears stale

Every few trading days:
- review afternoon behavior after removal of the hard late-session block
- review top skip reasons from `paper_trade_attempts`
- review Neon usage against the current scheduler cadence

Cloud cleanup once stable:
- delete old Cloud SQL
- remove migration leftovers
- remove unused DB secrets

Strategy tuning triggers:
- repeated weak realized P&L in specific ET hours
- persistent mismatch between conversion and realized quality
- one skip reason dominating most non-placements

Best next tuning knobs:
- time penalty
- confidence threshold
- sizing aggressiveness
- specific-hour caution

Current paper-trading config defaults:
- `PAPER_TRADE_MAX_CAPITAL_ALLOCATION_PCT=1.0`
- `PAPER_TRADE_ENFORCE_MAX_POSITIONS=false`
- `PAPER_TRADE_MAX_POSITIONS=10` as the dormant future cap when the flag is re-enabled
- instrument watchlists are intentionally split across multiple categories to stay below the per-category Twelve Data symbol ceiling
- these values are now carried through deployment config in `cloudbuild.yaml`, so future changes can be made at deploy-time without editing strategy logic
- placement sizing is additionally clamped by `ALPACA_MAX_NOTIONAL`, so confidence-based sizing cannot expand a paper trade above the broker-side per-trade hard cap
- Alpaca HTTP audit logging is now opt-in for successful requests via `ENABLE_ALPACA_HTTP_AUDIT`; failures are always persisted
- the Cloud Run container now starts Gunicorn with a 300-second worker timeout so the `daily-post-close` scheduler flow has enough time to finish reconciliation and exports without being killed at the default 30-second boundary

---

## 6. Repository Structure

Target direction:
- keep the repository root focused on application entrypoints, infra files, and top-level documentation
- move operational one-off utilities into `scripts/`
- continue moving export, analytics, and integration modules into more explicit domain folders over time

### Backend root
- `app.py` — application wiring and top-level service/route integration
- `storage.py` — thin compatibility facade over repository modules
- `schema.sql` — database schema definition
- root-level files should stay limited to active runtime entrypoints and cross-cutting foundations

### Shared foundations
- `core/`
  - `db.py`
  - `logging_utils.py`
  - `trade_math.py`

### Orchestration helpers
- `orchestration/`
  - `scan_context.py`
  - `paper_trade_context.py`
  - `app_orchestration.py`
  - `scheduler_ops.py`

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
- `brokers/`
  - `base.py`
  - `alpaca_adapter.py`
  - `ibkr_adapter.py`
- `alpaca/`
  - `alpaca_http.py`
  - `alpaca_client.py`
  - `alpaca_orders.py`
  - `alpaca_positions.py`
  - `paper.py`
  - `sync.py`
  - `reconcile.py`

### Analytics and export
- `analytics/`
  - `instruments.py`
  - `instruments.json`
  - `trade_analysis.py`
  - `signal_analysis.py`
  - `trade_scan.py`
- `exports/`
  - `export_reports.py`
  - `export_daily_snapshot.py`
  - `github_export.py`

### Maintenance and repair utilities
- `scripts/`
  - backfills
  - reconciliation/lifecycle repair utilities
  - operational cleanup helpers

Current code reality:
- this cleanup is in progress; repair and backfill scripts are being moved out of the repository root first because they are low-risk and contribute heavily to perceived root-level sprawl
- export-related runtime modules have now been consolidated under `exports/`, which reduces root-level noise without changing hot-path scan/sync behavior
- analytics and scan modules have now been consolidated under `analytics/`, which makes the repository shape clearer without changing the service boundaries used by the runtime
- Alpaca integration wrappers have now been folded into the existing `alpaca/` package, so broker code no longer spills across the repository root
- orchestration and context helpers have now been consolidated under `orchestration/`, which leaves the repository root focused on app entrypoints and shared foundations
- the unused `export_to_github.py` stub has been removed from the root
- shared foundations have now been consolidated under `core/`, so DB access, structured logging, and trade math are no longer scattered at the repository root
- the tradable instrument universe now lives in `analytics/instruments.json` and is loaded through `analytics/instruments.py`, which keeps watchlist growth and duplicate validation out of the scan code
- instrument groups are now split into `primary` through `sixth` plus `core_one` through `core_three`, keeping the original categories trimmed while allowing watchlist growth without breaching the per-category limit
- paper-trading allocation and max-position behavior are now env-driven through `core/paper_trade_config.py`, so paper-mode risk posture can be changed without editing strategy code

### Dashboard defaults
- operational tables now sort newest-first in the UI by default so the latest entries are visible first even if backend ordering changes
- risk exposure now displays unlimited open-position capacity when the max-position cap is disabled

### Repository modules
- `repositories/scans_repo.py`
- `repositories/trades_repo.py`
- `repositories/broker_repo.py`
- `repositories/reconcile_repo.py`
- `repositories/ops_repo.py`

Current code reality:
- repository modules now hold most domain-specific persistence logic
- `storage.py` remains as a compatibility surface so existing imports do not need to change all at once
- `analysis/*` is still not the active runtime analysis package; analysis logic remains in root-level modules such as `trade_analysis.py` and `signal_analysis.py`
- `export_to_github.py` remains unused by the active export flow
### Frontend
- `dashboard-ui/`
  - `src/pages/DashboardPage.jsx`
  - `src/components/*`
  - `src/api/*`

---

## 7. Core Runtime Components

### 7.1 Flask API Layer

The Flask app is the runtime entry point deployed on Cloud Run.

Responsibilities:
- expose operational endpoints
- expose scheduled endpoints
- expose analytics endpoints
- expose dashboard endpoints
- delegate implementation to service and storage layers

Current code reality:
- `app.py` is materially slimmer than the original monolith and now focuses on wiring, handler composition, and route registration
- significant helper/orchestration logic has been extracted into `scan_context.py`, `paper_trade_context.py`, and `app_orchestration.py`
- route modules are in active use, though some integration responsibility still lives in `app.py`

Examples of endpoint groups:
- scan endpoints
- sync endpoints
- trade endpoints
- reconciliation endpoints
- analysis endpoints
- export endpoints
- dashboard endpoints
- health endpoints

### 7.2 Service Layer

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

## 8. External Integrations

### 8.1 Alpaca

Used for:
- paper order placement
- position reads
- position closes
- order status lookup
- order synchronization

All Alpaca requests should be logged into `alpaca_api_logs` where practical.

### 8.2 GitHub

Used for:
- daily export snapshot backup
- long-term export retention outside Cloud Run runtime filesystem

The GitHub export implementation should:
- clone existing repository history
- copy snapshot files into the cloned working tree
- commit only when there are changes
- push safely without force

---

## 9. Database Architecture

Primary database: PostgreSQL.

Current code reality:
- repository modules are now the primary home for most persistence logic
- `storage.py` remains as a thin facade for backwards compatibility and import stability

### 9.1 Primary tables

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

#### `signal_logs`
Stores per-scan summary rows and signal-level logging used for analysis, diagnostics, and signal matching support.

#### `paper_trade_attempts`
Stores one row per candidate/attempt outcome so placement, skip, rejection, and execution decisions can be analyzed directly from the database.

Current code reality:
- `paper_trade_attempts` is now the primary diagnostic source for understanding why trades were not placed
- dashboard and operational reporting now summarize attempt outcomes by session hour and dominant non-placement reason
- the analytics view now includes an hourly execution-attempt outcome chart so candidate volume, placements, and non-placements can be reviewed side by side
- the dashboard summary API now also exposes ET-based realized outcome quality by entry hour from `trade_lifecycles`
- the overview execution section now compares conversion strength against realized quality so misleading “high activity but weak outcomes” hours are easier to spot quickly
- the health API exposes dedicated attempt analytics endpoints including:
  - `/paper-trade-attempts/recent`
  - `/paper-trade-attempts/rejections`
  - `/paper-trade-attempts/daily-summary`
  - `/paper-trade-attempts/hourly-summary`

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

## 10. Trade Lifecycle Design

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

## 11. Risk Management and Position Sizing

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
- from `9:35` through `15:45`, including every `:55` tick, runs intraday sync and scheduled scans on a 10-minute offset cadence
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
- reconciliation detail/history tables
- Alpaca API log views
- execution insight panels for `paper_trade_attempts`

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
- view-specific dashboard polling gated to the operational window
- dashboard polling cadence reduced to 30 minutes to avoid unnecessary Neon activity while a browser tab is open
- dashboard polling window aligned to current operations flow: weekdays 9:35 AM to 4:30 PM ET
- initial dashboard load now prioritizes the active view only instead of fetching every heavy section up front
- reconciliation overview data loads on the overview, while reconciliation detail/history stay deferred to the reconciliation workspace
- dashboard summary, risk exposure, and ops summary endpoints now use short in-process TTL caching to reduce repeated Neon reads
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
2. compare staged files against the current GitHub repository contents through the GitHub Contents API
3. upload only changed files with the configured `GITHUB_TOKEN`
4. create/update snapshot files directly on the configured branch without requiring a local `git` binary

### Design requirement
The export flow must never mutate live operational data.
It should only create DB-derived snapshots and generated report copies.

### Current status
**Implemented**
- manual endpoint tested successfully
- `daily-post-close` Cloud Scheduler job exists for daily execution
- operational monitoring still recommended

### Trade lifecycle persistence note
- `trade_lifecycles.mode` must be preserved across later sync, reconciliation, and close updates.
- Repository updates now keep the existing mode when a downstream caller sends a blank mode, which prevents intraday category metadata from being wiped during close/sync flows.

### Strategy reporting note
- dashboard hourly outcome quality now has a strategy-only view that excludes `EXTERNAL_EXIT` rows
- `EXTERNAL_EXIT` remains stored and reported separately so forced EOD/manual closes do not distort core strategy reads
- late-session trade evaluation now applies an extra confidence penalty from `12:00 PM ET` onward instead of reintroducing a hard afternoon block

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
- Alpaca HTTP failures are always stored, while successful request auditing is now controlled by `ENABLE_ALPACA_HTTP_AUDIT`
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
- legacy Cloud SQL resources have been removed; Neon is the only active production database

---

## 18. Implemented vs Missing

### Fully implemented
- Cloud Run backend deployment
- Cloud Build configuration
- push-triggered Cloud Build deployment flow, so normal code pushes should not require a second manual build trigger in routine operation
- consolidated Cloud Scheduler architecture with three production jobs
- PostgreSQL schema for core operational tables
- repository-based persistence split with `storage.py` compatibility layer
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
- Artifact Registry retention hardening; a repository cleanup policy file now exists in `scripts/artifact_registry_cleanup_policy.json`, but production cleanup policy application and validation should be treated as an operational follow-up item

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
2. monitor `trade_lifecycles`, `paper_trade_attempts`, and scheduler behavior through several real market sessions after the latest scheduler and late-session trading changes
3. verify the consolidated `market-ops` and `daily-post-close` scheduler flow against delayed fills, close timing, and post-close sync/reconciliation ordering
4. define and implement retention policy for large operational tables and exported snapshots, especially `alpaca_api_logs`
5. finalize teardown of legacy Cloud SQL / migration resources once Neon has been stable long enough
6. improve dashboard operational visibility further, especially placement/skip reasons by hour and scheduler/job health indicators
7. document runtime environment variables, scheduler jobs, and deployment steps more clearly
8. continue reducing remaining integration/orchestration complexity inside `app.py`

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
