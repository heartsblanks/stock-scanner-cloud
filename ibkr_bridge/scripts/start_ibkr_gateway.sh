#!/usr/bin/env bash
set -euo pipefail

IBKR_GATEWAY_HOME="${IBKR_GATEWAY_HOME:-/home/ibkr/Jts}"
IBKR_GATEWAY_LAUNCHER="${IBKR_GATEWAY_LAUNCHER:-$IBKR_GATEWAY_HOME/ibgateway/ibgateway}"
IBKR_GATEWAY_USER="${IBKR_GATEWAY_USER:-ibkr}"
IBKR_GATEWAY_ARGS="${IBKR_GATEWAY_ARGS:-}"
IBKR_GATEWAY_DISPLAY="${IBKR_GATEWAY_DISPLAY:-:99}"
IBKR_GATEWAY_XVFB_CMD="${IBKR_GATEWAY_XVFB_CMD:-Xvfb}"
IBKR_GATEWAY_XVFB_SCREEN="${IBKR_GATEWAY_XVFB_SCREEN:-1024x768x24}"
IBKR_GATEWAY_XVFB_PID_FILE="${IBKR_GATEWAY_XVFB_PID_FILE:-/tmp/ibkr-gateway-xvfb.pid}"

if [[ ! -x "$IBKR_GATEWAY_LAUNCHER" ]]; then
  echo "IB Gateway launcher not found or not executable: $IBKR_GATEWAY_LAUNCHER" >&2
  exit 1
fi

if [[ "$(id -un)" != "$IBKR_GATEWAY_USER" ]]; then
  echo "This launcher should run as $IBKR_GATEWAY_USER" >&2
  exit 1
fi

start_xvfb_if_needed() {
  if [[ -n "${DISPLAY:-}" ]]; then
    return 0
  fi

  if ! command -v "$IBKR_GATEWAY_XVFB_CMD" >/dev/null 2>&1; then
    echo "DISPLAY is not set and $IBKR_GATEWAY_XVFB_CMD is not installed" >&2
    exit 1
  fi

  export DISPLAY="$IBKR_GATEWAY_DISPLAY"

  if pgrep -u "$IBKR_GATEWAY_USER" -f "^$IBKR_GATEWAY_XVFB_CMD $DISPLAY " >/dev/null 2>&1; then
    return 0
  fi

  "$IBKR_GATEWAY_XVFB_CMD" "$DISPLAY" -screen 0 "$IBKR_GATEWAY_XVFB_SCREEN" \
    >"${IBKR_GATEWAY_XVFB_PID_FILE}.log" 2>&1 &
  echo $! >"$IBKR_GATEWAY_XVFB_PID_FILE"

  # Give the virtual display a moment to come up before launching the GUI app.
  sleep 2
}

start_xvfb_if_needed

cd "$IBKR_GATEWAY_HOME"
exec "$IBKR_GATEWAY_LAUNCHER" $IBKR_GATEWAY_ARGS
