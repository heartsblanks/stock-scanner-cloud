#!/usr/bin/env bash
set -euo pipefail

IBKR_GATEWAY_HOME="${IBKR_GATEWAY_HOME:-/home/ibkr/Jts}"
IBKR_GATEWAY_LAUNCHER="${IBKR_GATEWAY_LAUNCHER:-$IBKR_GATEWAY_HOME/ibgateway/ibgateway}"
IBKR_GATEWAY_USER="${IBKR_GATEWAY_USER:-ibkr}"
IBKR_GATEWAY_ARGS="${IBKR_GATEWAY_ARGS:-}"

if [[ ! -x "$IBKR_GATEWAY_LAUNCHER" ]]; then
  echo "IB Gateway launcher not found or not executable: $IBKR_GATEWAY_LAUNCHER" >&2
  exit 1
fi

if [[ "$(id -un)" != "$IBKR_GATEWAY_USER" ]]; then
  echo "This launcher should run as $IBKR_GATEWAY_USER" >&2
  exit 1
fi

cd "$IBKR_GATEWAY_HOME"
exec "$IBKR_GATEWAY_LAUNCHER" $IBKR_GATEWAY_ARGS
