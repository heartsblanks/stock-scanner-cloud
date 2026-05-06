# Stock Scanner Cloud

Cloud-native trading workflow system for scanning markets, executing IBKR paper trades, syncing state, analyzing performance, and visualizing results via a dashboard.

👉 See **ARCHITECTURE.md** for the full system design and current implementation status.

---

## 🚀 What this project does

- Schedules market scans and generates trade candidates
- Places and manages **IBKR paper trades**
- Syncs open/closed positions and detects exits
- Stores data in **PostgreSQL (Neon)**
- Stores detailed scan signal rows in **PostgreSQL**
- Builds **trade lifecycle analytics**
- Runs reconciliation between local and broker data
- Serves a **React dashboard** for monitoring and insights

---

## 🏗️ Tech Stack

- **Backend:** Python (Flask)
- **Frontend:** React + Vite
- **Database:** PostgreSQL (Neon)
- **Compute:** Google Cloud Run
- **Scheduling:** Google Cloud Scheduler
- **CI/CD:** Google Cloud Build
- **Broker:** IBKR (paper trading)

---

## 📂 Repository Structure (simplified)

```
├── app.py
├── db.py
├── storage.py
├── schema.sql
├── routes/
├── services/
├── repositories/
├── ibkr/
├── analysis/
├── exports/
├── dashboard-ui/
├── Dockerfile
├── cloudbuild.yaml
├── README.md
├── ARCHITECTURE.md
```

---

## ⚙️ Setup (local)

### 1. Clone
```bash
git clone https://github.com/heartsblanks/stock-scanner-cloud.git
cd stock-scanner-cloud
```

### 2. Python setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Run backend
```bash
python app.py
```

API will be available at:
```
http://localhost:8080
```

---

## 🐳 Docker

```bash
docker build -t stock-scanner-cloud .
docker run -p 8080:8080 stock-scanner-cloud
```

---

## ☁️ Cloud Deployment

Deployed via **Google Cloud Build → Cloud Run** with a hosted PostgreSQL database on Neon.

The runtime container uses Gunicorn with an extended worker timeout so the post-close scheduler flow can complete longer reconciliation/export work without hitting the default 30-second worker abort.

```bash
gcloud builds submit --tag gcr.io/<PROJECT_ID>/stock-scanner

gcloud run deploy stock-scanner \
  --image gcr.io/<PROJECT_ID>/stock-scanner \
  --region europe-west1
```

---

## 🔐 Runtime Environment

Core backend variables:

- `DATABASE_URL` or the socket-based `DB_*` variables
- `APCA_API_KEY_ID`
- `APCA_API_SECRET_KEY`
- `APCA_BASE_URL` for IBKR paper/live selection
- `ADMIN_API_TOKEN` for protected operational endpoints such as `/admin/test-alert`

Paper-trading and scan controls:

- `PAPER_TRADE_MIN_CONFIDENCE`
- `IBKR_PAPER_TRADE_MIN_CONFIDENCE`
- `PAPER_MAX_NOTIONAL`
- `MIN_NOTIONAL_TO_PLACE`
- `SCHEDULED_PAPER_ACCOUNT_SIZE`
- `ENABLE_FRACTIONAL_SHARES`
- `FRACTIONAL_SHARE_DECIMALS`
- `PAPER_STOP_COOLDOWN_MINUTES`
- `PAPER_TARGET_COOLDOWN_MINUTES`
- `PAPER_MANUAL_CLOSE_COOLDOWN_MINUTES`
- `PAPER_SYMBOL_GATING_ENABLED`
- `PAPER_SYMBOL_GATING_LOOKBACK`
- `PAPER_SYMBOL_GATING_MIN_TRADES`
- `PAPER_SYMBOL_GATING_MAX_AVG_PNL_PCT`
- `PAPER_CONSECUTIVE_LOSS_COOLDOWN_THRESHOLD`
- `PAPER_CONSECUTIVE_LOSS_COOLDOWN_MINUTES`
- `LOW_PRICE_NOTIONAL_CAP_ENABLED`
- `LOW_PRICE_THRESHOLD`
- `LOW_PRICE_MAX_NOTIONAL`
- `SYMBOL_ELIGIBILITY_MAX_SYMBOLS_PER_MODE`
- `SYMBOL_RANKING_WINDOW_DAYS`
- `SYMBOL_RANKING_BROKER`
- `IBKR_SYMBOL_RANKING_WINDOW_DAYS`
- `IBKR_SYMBOL_RANKING_MIN_CLOSED_TRADES`
- `ENABLE_LATE_SESSION_HARD_BLOCK`

IBKR / VM orchestration variables:

- `IBKR_BRIDGE_BASE_URL`
- `IBKR_BRIDGE_TOKEN`
- `IBKR_READINESS_SYMBOL`
- `IBKR_BRIDGE_HEALTH_TIMEOUT_SECONDS`
- `IBKR_BRIDGE_ACCOUNT_TIMEOUT_SECONDS`
- `IBKR_BRIDGE_POSITIONS_TIMEOUT_SECONDS`
- `IBKR_BRIDGE_STATUS_MARKET_DATA_TIMEOUT_SECONDS`
- `IBKR_BRIDGE_MARKET_DATA_TIMEOUT_SECONDS`
- `IBKR_SHADOW_ACCOUNT_SIZE_FALLBACK`
- `IBKR_VM_PROJECT`
- `IBKR_VM_ZONE`
- `IBKR_VM_INSTANCE_NAME`

Reporting / export variables:

- `LOG_BUCKET`
- `RECONCILIATION_BUCKET`
- `RECONCILIATION_OBJECT`
- `TRADE_ANALYSIS_BUCKET`
- `TRADE_ANALYSIS_SUMMARY_OBJECT`
- `TRADE_ANALYSIS_PAIRED_OBJECT`
- `SIGNAL_ANALYSIS_BUCKET`
- `SIGNAL_ANALYSIS_SUMMARY_OBJECT`
- `SIGNAL_ANALYSIS_ROWS_OBJECT`
- `LOG_LEVEL`

---

## ⏱️ Scheduler Jobs

Cloud Scheduler triggers:

- market scans
- trade sync
- end-of-day close
- reconciliation
- analytics generation

Recommended consolidated scheduler setup:

- `market-ops`
  - `*/5 9-15 * * 1-5 (America/New_York)`
  - Calls `POST /scheduler/market-ops`
  - Internally starts sync at `9:30`, keeps sync on a low-call `30`-minute cadence (`:00/:30`), starts scheduled scans at `9:45`, keeps scans on the 10-minute offset cadence through `15:45`, disables periodic health probes during the day, reserves `15:50` for pre-close sync plus readiness prep, and keeps `15:55` for end-of-day close only
- `daily-post-close`
  - `30 16 * * 1-5 (America/New_York)`
  - Calls `POST /scheduler/daily-post-close`
  - Internally runs sync first, repairs stale IBKR closes, refreshes symbol rankings, refreshes next-session symbol eligibility, then runs reconciliation, trade analysis, signal analysis, and mode ranking refresh
- `maintenance`
  - `0 18 * * * (America/New_York)`
  - Calls `POST /scheduler/maintenance`
  - Prunes `broker_api_logs` plus older operational rows from `signal_logs`, `scan_runs`, `paper_trade_attempts`, `broker_orders`, `reconciliation_details`, and `reconciliation_runs`
- `ibkr-login-alert`
  - `*/10 9-16 * * 1-5 (America/New_York)`
  - Calls `POST /scheduler/ibkr-login-alert`
  - Applies a low-call check gate (default `30` minutes) and only sends an alert when the IBKR bridge reports `LOGIN_REQUIRED` and Telegram alert credentials are configured
- `test-day-cycle` (manual on-demand endpoint)
  - Calls `POST /scheduler/test-day-cycle`
  - Requires `X-Admin-Token: $ADMIN_API_TOKEN`
  - Runs a compressed E2E cycle in one request: scan rounds (by mode), sync, optional EOD close, and optional post-close reconciliation/analysis ops
  - Useful for repeatable QA drills from Cloud Run (for example, replaying a "full day" in ~1 hour)

Example on-demand US test compressed cycle:

```bash
curl -sS -X POST "$RUN_BASE_URL/scheduler/test-day-cycle" \
  -H "Content-Type: application/json" \
  -H "X-Admin-Token: $ADMIN_API_TOKEN" \
  -d '{
    "modes": ["us_test"],
    "scan_rounds": 6,
    "scan_interval_seconds": 600,
    "paper_trade": true,
    "ignore_market_hours": true,
    "run_initial_sync": true,
    "sync_after_each_scan": true,
    "run_eod_close": true,
    "run_post_close": true
  }'
```

Low-call mode toggle:

- Set `IBKR_LOW_CALL_MODE=true` in Cloud Run to enable reduced IBKR polling behavior.
- Optional overrides:
  - `IBKR_LOGIN_ALERT_CHECK_INTERVAL_MINUTES` (default `30` in low-call mode)
  - `IBKR_ACCOUNT_EQUITY_CACHE_TTL_SECONDS` (default `1800`)

---

## 🔔 IBKR Login Alerts

The app can emit an IBKR login-required alert directly through a Telegram bot.

Required environment variables:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `TELEGRAM_ALERT_DEDUP_MINUTES` (optional, defaults to `30`)

Important note:

- You need to create a Telegram bot once through `@BotFather`.
- You need to send at least one message to that bot from the destination chat before alerts can be delivered there.

Operational flow:

- Cloud Scheduler calls `POST /scheduler/ibkr-login-alert`
- Cloud Run checks `/ibkr-status`
- If the bridge reports `LOGIN_REQUIRED`, the backend sends a deduplicated Telegram alert
- The dashboard can also trigger a manual alert test through the Vercel proxy route `dashboard-ui/api/admin-test-alert.js`

## 📊 Dashboard

## 🧹 Retention Policy

The maintenance scheduler currently applies this first-pass retention policy:

- `broker_api_logs`: 30 days
- `signal_logs`: 45 days
- `scan_runs`: 45 days
- `paper_trade_attempts`: 120 days
- `broker_orders`: 120 days
- `reconciliation_details`: 120 days
- `reconciliation_runs`: 120 days

Trade history tables such as `trade_events` and `trade_lifecycles` are intentionally not pruned by the automated maintenance job yet.

Located in:
```
dashboard-ui/
```

Run locally:
```bash
cd dashboard-ui
npm install
npm run dev
```

Features:
- summary metrics
- open trades
- lifecycle analytics
- charts (equity, symbol, mode, hourly)
- insights

---

## 💾 Database

Main tables:
- `signal_logs`
- `trade_events`
- `trade_lifecycles`
- `broker_orders`
- `reconciliation_runs`
- `reconciliation_details`
- `broker_api_logs`
- `instrument_catalog`
- `symbol_session_eligibility`
- `symbol_rankings`

Schema defined in:
```
schema.sql
```

Production symbols are loaded from the DB-backed `instrument_catalog`. The code-level defaults are only a seed/sync helper; live scheduled scans use `symbol_session_eligibility`, capped by price eligibility and `symbol_rankings`.

---

## ⚠️ Important Notes

- Paper trading only (IBKR paper API)
- Not financial advice
- Do not use for real capital without validation

---

## 🧹 What NOT to commit

Do **not** commit:

- `__pycache__/`
- `*.pyc`
- `.venv/`
- `dashboard-ui/node_modules/`
- `dashboard-ui/dist/`
- `.env`
- `.DS_Store`

---

## 📌 Current Status

See **ARCHITECTURE.md → Implemented vs Missing** for:
- completed features
- partial implementations
- remaining work

---

## 🤝 Contributing

Before making changes:
1. update `ARCHITECTURE.md`
2. define requirement clearly
3. implement
4. validate

---

## 📄 License

Add license here.
