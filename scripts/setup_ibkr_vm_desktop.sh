#!/usr/bin/env bash

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-stock-scanner-490821}"
ZONE="${ZONE:-europe-west1-b}"
INSTANCE_NAME="${INSTANCE_NAME:-ibkr-bridge-vm}"
VM_USER="${VM_USER:-ibkr}"

REMOTE_SCRIPT=$(cat <<'EOF'
set -euo pipefail

sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  openbox \
  xrdp \
  xorgxrdp \
  dbus-x11 \
  xvfb

cat <<'EOS' | sudo tee /home/__VM_USER__/.xsession >/dev/null
export LIBGL_ALWAYS_SOFTWARE=1
export MESA_LOADER_DRIVER_OVERRIDE=llvmpipe
openbox-session
EOS
sudo chown __VM_USER__:__VM_USER__ /home/__VM_USER__/.xsession
sudo chmod 644 /home/__VM_USER__/.xsession

sudo adduser xrdp ssl-cert >/dev/null 2>&1 || true
sudo systemctl enable xrdp
sudo systemctl restart xrdp
sudo systemctl enable ibkr-gateway
sudo systemctl enable ibkr-bridge

sudo systemctl status xrdp --no-pager -l | tail -n 20
EOF
)

REMOTE_SCRIPT="${REMOTE_SCRIPT//__VM_USER__/${VM_USER}}"

gcloud compute ssh "$INSTANCE_NAME" \
  --project="$PROJECT_ID" \
  --zone="$ZONE" \
  --command="$REMOTE_SCRIPT"
