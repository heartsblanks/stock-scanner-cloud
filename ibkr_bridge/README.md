# IBKR Bridge

This service is intended to run on a GCP VM next to IB Gateway.

Purpose:
- keep IB Gateway off Cloud Run
- expose a small authenticated HTTP API that the main app can call
- support parallel paper-trading evaluation against the current Alpaca setup

Current status:
- read-path implementation started
- authenticated endpoints now support real IBKR reads for account, positions, and open orders
- the first operational write-path endpoints are now available for:
  - `POST /orders/cancel-by-symbol`
  - `POST /positions/close`
- paper bracket-order placement is now available and manually verified through the bridge

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
- expand order sync / reconciliation helpers
- improve broker-session durability and startup automation
- reduce or eliminate the remaining daily manual IB Gateway login steps

Cost control recommendation:
- keep the VM scheduled only for the trading window instead of running 24/7
- recommended scheduler window:
  - start VM at `9:15 AM ET`
  - stop VM at `5:00 PM ET`
- keep in mind that IB Gateway login is still manual right now, so the VM can auto-start but IB Gateway may still need a fresh daily paper-session login until that is automated
- plain Cloud Scheduler cron cannot skip market holidays by itself
- if you want holiday-aware VM control, the clean path is:
  - Scheduler calls a tiny controller endpoint or script on weekdays
  - that controller checks the NYSE calendar
  - only then starts or stops the VM
- the current repo now includes that controller on the main Cloud Run app at:
  - `POST /scheduler/ibkr-vm-control`
- the scheduler setup script now points the weekday jobs at that controller instead of calling Compute Engine directly

Current manual-access convenience:
- the VM now has a static public IP for manual RDP access:
  - `34.77.242.163:3389`
- that means your phone/laptop login address should stay stable even though the VM is stopped and started on a schedule
- Cloud Run still reaches the bridge over the VM's internal IP through the VPC connector, so the static public IP is only for operator access

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

### 7a. Optional: install IB Gateway as a managed service
If you want the VM to start IB Gateway automatically when it boots, add the optional service files:

```bash
sudo cp ibkr_bridge/systemd/ibkr-gateway.env.example /etc/ibkr-gateway.env
sudo cp ibkr_bridge/systemd/ibkr-gateway.service /etc/systemd/system/ibkr-gateway.service
sudo chmod 600 /etc/ibkr-gateway.env
sudo systemctl daemon-reload
sudo systemctl enable ibkr-gateway
sudo systemctl start ibkr-gateway
```

Notes:
- you still need to verify the launcher path in `/etc/ibkr-gateway.env`
- the bridge service can now start even before the Gateway API port is ready, which keeps `/health` available for dashboard status checks
- Gateway can auto-start headlessly through `Xvfb` when no `DISPLAY` is present
- keep the bridge separate from Gateway so the API service can be restarted independently

### 7b. Add a readiness check
Once both services are installed, you can verify whether automation actually resulted in a usable broker session:

```bash
sudo bash -lc 'set -a; source /etc/ibkr-bridge.env; source /etc/ibkr-gateway.env; python3 /opt/stock-scanner-cloud/ibkr_bridge/scripts/check_ibkr_bridge_ready.py'
```

This is intentionally stricter than a simple port check. It verifies:
- bridge health
- account access
- one small intraday market-data fetch

If that command fails, the stack is up but the IBKR session still needs manual attention, usually a login or re-login.

### 7c. Daily operator flow
Recommended daily flow now that VM startup is automated:

1. let the weekday scheduler start the VM
2. let `ibkr-gateway.service` and `ibkr-bridge.service` start automatically
3. run the readiness check
4. only log into IB Gateway if readiness fails

That keeps the manual step as a fallback instead of a required daily ritual.

### 7d. Make the VM desktop repeatable
If you want a visible remote desktop session for IB Gateway logins, use the helper script from your laptop:

```bash
bash scripts/setup_ibkr_vm_desktop.sh
```

That script installs:
- `xrdp`
- `xfce4`
- `dbus-x11`
- `xvfb`

It also configures the `ibkr` user to start an XFCE session and enables the `xrdp` service. After that, your normal daily flow can be:

1. let the VM auto-start
2. let Gateway auto-start
3. check the dashboard
4. if the dashboard says `LOGIN_REQUIRED`, connect by RDP and complete the IBKR login
5. let the bridge continue serving account and market-data checks once the session is ready

Phone-friendly fallback:
- you can use your phone for the IBKR approval/IB Key step if IBKR prompts for it
- you can also use a phone RDP client to connect to the VM at `34.77.242.163:3389` if the session needs manual attention
- the goal is:
  - automation handles VM + Gateway + bridge startup
  - you only intervene when the readiness check says the session is not usable

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

## Why VM Deployment Is Separate From `git push`

Cloud Run is being deployed through `cloudbuild.yaml`, so pushing code and triggering builds updates the managed service automatically.

The IBKR VM is different:
- it is a stateful machine, not a managed serverless deploy target
- it runs local software next to IB Gateway
- we intentionally have not wired a VM auto-deploy hook yet, because an automatic restart during market hours could interrupt the broker session

So today the VM deploy step is explicit:

```bash
bash scripts/deploy_ibkr_vm.sh
```

That script:
- SSHes into the VM
- fast-forwards the `ibkr-parallel-eval` checkout
- refreshes the repo-local Python virtualenv and installs `requirements.txt`
- refreshes the `ibkr-gateway` and `ibkr-bridge` systemd unit files from the repo
- reloads systemd
- restarts both services
- restarts `ibkr-bridge`

This is safer than silently redeploying the VM on every push while the broker session is live.

## Recommended Next Sequence

The best order from here is:
- auto-start IB Gateway on boot
- keep the bridge updated with `scripts/deploy_ibkr_vm.sh`
- run the readiness check after boot or after deployment
- use manual phone/laptop login only when readiness fails
- verify live dual placement during market hours
- then decide whether to automate the remaining login/session step further

## VM Start/Stop Automation

Use the helper script in [scripts/setup_ibkr_vm_scheduler.sh](/Users/viniththomas/stock-scanner-cloud/scripts/setup_ibkr_vm_scheduler.sh) to create two Cloud Scheduler jobs:
- `ibkr-vm-start`
- `ibkr-vm-stop`

Default schedule:
- start: `15 9 * * 1-5` in `America/New_York`
- stop: `0 17 * * 1-5` in `America/New_York`

That window leaves time before the open and enough room after the close for post-close comparison work.
