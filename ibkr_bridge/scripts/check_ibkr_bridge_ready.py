#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request


def _request_json(base_url: str, path: str, *, token: str, timeout: float) -> object:
    req = urllib.request.Request(urllib.parse.urljoin(base_url, path))
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=timeout) as response:
        raw = response.read().decode().strip()
        return json.loads(raw) if raw else None


def main() -> int:
    base_url = str(os.getenv("IBKR_BRIDGE_READY_BASE_URL", "http://127.0.0.1:8090")).strip().rstrip("/") + "/"
    token = str(os.getenv("IBKR_BRIDGE_TOKEN", "")).strip()
    symbol = str(os.getenv("IBKR_READINESS_SYMBOL", "SPY")).strip().upper() or "SPY"
    timeout = float(os.getenv("IBKR_BRIDGE_READY_TIMEOUT_SECONDS", "15"))

    try:
        health_payload = _request_json(base_url, "health", token=token, timeout=timeout) or {}
        if not isinstance(health_payload, dict) or not health_payload.get("ok"):
            raise RuntimeError("bridge health endpoint did not return ok=true")

        account_payload = _request_json(base_url, "account", token=token, timeout=timeout) or {}
        if not isinstance(account_payload, dict) or not account_payload.get("account_id"):
            raise RuntimeError("bridge account endpoint did not return an account_id")

        market_path = f"market-data/intraday?symbol={urllib.parse.quote(symbol)}&interval=1min"
        market_payload = _request_json(base_url, market_path, token=token, timeout=timeout) or []
        if not isinstance(market_payload, list):
            raise RuntimeError("bridge market-data endpoint did not return a list")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, RuntimeError, ValueError) as exc:
        sys.stderr.write(f"IBKR bridge readiness failed: {exc}\n")
        return 1

    sys.stdout.write(
        f"IBKR bridge ready: account_id={account_payload.get('account_id')} "
        f"readiness_symbol={symbol} bars={len(market_payload)}\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
