# Stock Scanner Cloud

Cloud-native trading workflow system for scanning markets, executing Alpaca paper trades, syncing state, analyzing performance, and visualizing results via a dashboard.

👉 See **ARCHITECTURE.md** for the full system design and current implementation status.

---

## 🚀 What this project does

- Schedules market scans and generates trade candidates
- Places and manages **Alpaca paper trades**
- Syncs open/closed positions and detects exits
- Stores data in **PostgreSQL (Neon)**
- Stores detailed scan signal rows in **PostgreSQL**
- Builds **trade lifecycle analytics**
- Runs reconciliation between local and broker data
- Exports daily snapshots and backs them up to **GitHub**
- Serves a **React dashboard** for monitoring and insights

---

## 🏗️ Tech Stack

- **Backend:** Python (Flask)
- **Frontend:** React + Vite
- **Database:** PostgreSQL (Neon)
- **Compute:** Google Cloud Run
- **Scheduling:** Google Cloud Scheduler
- **CI/CD:** Google Cloud Build
- **Broker:** Alpaca (paper trading)
- **Backup:** GitHub (daily snapshots)

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
├── alpaca/
├── analysis/
├── export_daily_snapshot.py
├── github_export.py
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

```bash
gcloud builds submit --tag gcr.io/<PROJECT_ID>/stock-scanner

gcloud run deploy stock-scanner \
  --image gcr.io/<PROJECT_ID>/stock-scanner \
  --region europe-west1
```

---

## ⏱️ Scheduler Jobs

Cloud Scheduler triggers:

- market scans
- trade sync
- end-of-day close
- reconciliation
- analytics generation
- daily GitHub snapshot export

---

## 📊 Dashboard

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
- `alpaca_api_logs`

Schema defined in:
```
schema.sql
```

---

## 🔁 Daily Snapshot Backup

Endpoint:
```
POST /export-daily-snapshot
```

Flow:
1. export reports + DB snapshots
2. clone GitHub repo
3. copy snapshot files
4. commit + push

Snapshots are DB-derived and no longer depend on operational `signals.csv` or `trades.csv` log files.

---

## ⚠️ Important Notes

- Paper trading only (Alpaca paper API)
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
