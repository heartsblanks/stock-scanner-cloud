#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-stock-scanner-490821}"
ZONE="${ZONE:-europe-west1-b}"
INSTANCE_NAME="${INSTANCE_NAME:-ibkr-bridge-vm}"
BRANCH="${BRANCH:-ibkr-parallel-eval}"
VM_USER="${VM_USER:-ibkr}"

gcloud compute ssh "$INSTANCE_NAME" \
  --project="$PROJECT_ID" \
  --zone="$ZONE" \
  --command="sudo -u $VM_USER bash -lc 'cd /opt/stock-scanner-cloud && git fetch origin && git checkout $BRANCH && git pull --ff-only origin $BRANCH && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt' \
&& sudo cp /opt/stock-scanner-cloud/ibkr_bridge/systemd/ibkr-gateway.service /etc/systemd/system/ibkr-gateway.service \
&& sudo cp /opt/stock-scanner-cloud/ibkr_bridge/systemd/ibkr-bridge.service /etc/systemd/system/ibkr-bridge.service \
&& sudo usermod -a -G systemd-journal $VM_USER \
&& sudo systemctl daemon-reload \
&& sudo systemctl disable --now ibkr-gateway || true \
&& sudo systemctl restart ibkr-bridge \
&& sleep 3 \
&& sudo systemctl is-active ibkr-bridge \
&& curl -sf http://127.0.0.1:8090/health || true"
