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
  --command="sudo -u $VM_USER bash -lc 'cd /opt/stock-scanner-cloud && git fetch origin && git checkout $BRANCH && git pull --ff-only origin $BRANCH' && sudo systemctl restart ibkr-bridge && sudo systemctl is-active ibkr-bridge"
