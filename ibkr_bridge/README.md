# IBKR Bridge

This service is intended to run on a GCP VM next to IB Gateway.

Purpose:
- keep IB Gateway off Cloud Run
- expose a small authenticated HTTP API that the main app can call
- support parallel paper-trading evaluation against the current Alpaca setup

Current status:
- read-path implementation started
- authenticated endpoints now support real IBKR reads for account, positions, and open orders
- write-path endpoints are still scaffolded until IB Gateway connectivity is verified on the VM

Recommended VM layout:
- VM OS: Ubuntu on GCP Compute Engine
- repo checkout: `/opt/stock-scanner-cloud`
- runtime user: `ibkr`
- bridge env file: `/etc/ibkr-bridge.env`
- bridge port: `8090` by default
- IB Gateway should stay local to the VM and should not be exposed publicly

Expected runtime env:
- `IBKR_BRIDGE_TOKEN`
- `PORT` (optional, defaults to `8090`)
- `IBKR_HOST` (defaults to `127.0.0.1`)
- `IBKR_PORT` (defaults to paper `4002`)
- `IBKR_CLIENT_ID` (defaults to `101`)
- `IBKR_ACCOUNT_ID` (optional; pin a specific paper account if needed)

Current endpoint contract:
- `GET /health`
- `GET /account`
- `GET /positions`
- `GET /orders/open`
- `GET /orders/{order_id}`
- `GET /orders/{order_id}/sync`
- `POST /orders/paper-bracket`
- `POST /orders/cancel-by-symbol`
- `POST /positions/close`

Planned next work:
- connect the running bridge to IB Gateway / TWS on the VM
- implement paper bracket-order placement
- implement order sync / reconciliation helpers
- add a small systemd-friendly deployment/runbook for the GCP VM

Cost control recommendation:
- keep the VM scheduled only for the trading window instead of running 24/7
- recommended scheduler window:
  - start VM at `9:15 AM ET`
  - stop VM at `5:00 PM ET`
- keep in mind that IB Gateway login is still manual right now, so the VM can auto-start but IB Gateway still needs a fresh daily paper-session login until that is automated

## GCP VM Runbook

### 1. Create the VM
- create an Ubuntu VM in the same region family you prefer for operations
- allow:
  - SSH from your IP
  - the bridge API port only from trusted IPs or through a reverse proxy
- do not expose IB Gateway's local API port publicly

### 2. Install runtime dependencies
```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip openjdk-17-jre git
```

### 3. Create the runtime user
```bash
sudo useradd --system --create-home --shell /bin/bash ibkr
```

### 4. Clone the repo on the VM
```bash
sudo mkdir -p /opt
cd /opt
sudo git clone https://github.com/heartsblanks/stock-scanner-cloud.git
sudo chown -R ibkr:ibkr /opt/stock-scanner-cloud
```

### 5. Install Python dependencies
```bash
cd /opt/stock-scanner-cloud
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

If you use the venv path in production, update the `ExecStart` path in the systemd unit to:
```bash
/opt/stock-scanner-cloud/.venv/bin/python -m ibkr_bridge.app
```

### 6. Install the bridge env file
```bash
sudo cp ibkr_bridge/systemd/ibkr-bridge.env.example /etc/ibkr-bridge.env
sudo chmod 600 /etc/ibkr-bridge.env
```

Then edit `/etc/ibkr-bridge.env` and set:
- `IBKR_BRIDGE_TOKEN`
- later, when implemented:
  - `IBKR_HOST`
  - `IBKR_PORT`
  - `IBKR_CLIENT_ID`

### 7. Install the systemd unit
```bash
sudo cp ibkr_bridge/systemd/ibkr-bridge.service /etc/systemd/system/ibkr-bridge.service
sudo systemctl daemon-reload
sudo systemctl enable ibkr-bridge
sudo systemctl start ibkr-bridge
```

### 8. Verify the bridge
Health should work without auth:
```bash
curl http://127.0.0.1:8090/health
```

Protected endpoints should require the bearer token:
```bash
curl -H "Authorization: Bearer YOUR_TOKEN" http://127.0.0.1:8090/account
```

### 9. Point the main app at the VM bridge later
When the VM bridge is reachable and implemented, configure the main app with:
- `PAPER_BROKER=ibkr`
- `IBKR_BRIDGE_BASE_URL=https://...` or internal VM URL
- `IBKR_BRIDGE_TOKEN=...`

For parallel evaluation, keep:
- `PAPER_BROKER=alpaca`
- and use shadow/compare flags separately until IBKR is proven stable

## VM Start/Stop Automation

Use the helper script in [scripts/setup_ibkr_vm_scheduler.sh](/Users/viniththomas/stock-scanner-cloud/scripts/setup_ibkr_vm_scheduler.sh) to create two Cloud Scheduler jobs:
- `ibkr-vm-start`
- `ibkr-vm-stop`

Default schedule:
- start: `15 9 * * 1-5` in `America/New_York`
- stop: `0 17 * * 1-5` in `America/New_York`

That window leaves time before the open and enough room after the close for post-close comparison work.
