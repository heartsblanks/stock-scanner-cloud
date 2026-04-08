from __future__ import annotations

import os
from functools import wraps
from typing import Any, Callable

from flask import Flask, jsonify, request
from ibkr_bridge.connector import get_ibkr_client


app = Flask(__name__)


def _bridge_token() -> str:
    return str(os.getenv("IBKR_BRIDGE_TOKEN", "")).strip()


def _authorized(req) -> bool:
    expected = _bridge_token()
    if not expected:
        return True

    auth_header = str(req.headers.get("Authorization", "")).strip()
    return auth_header == f"Bearer {expected}"


def require_auth(fn: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(fn)
    def wrapper(*args: Any, **kwargs: Any):
        if not _authorized(request):
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        return fn(*args, **kwargs)

    return wrapper


def not_implemented(operation: str):
    return (
        jsonify(
            {
                "ok": False,
                "error": "not_implemented",
                "operation": operation,
                "message": "IBKR bridge endpoint scaffold exists but broker connectivity is not implemented yet.",
            }
        ),
        501,
    )


def service_unavailable(message: str, *, operation: str, status_code: int = 503):
    return (
        jsonify(
            {
                "ok": False,
                "error": "service_unavailable",
                "operation": operation,
                "message": message,
            }
        ),
        status_code,
    )


@app.get("/health")
def health():
    client = get_ibkr_client()
    return jsonify({"ok": True, "service": "ibkr-bridge", "ibkr": client.health_snapshot()})


def _run_bridge_operation(operation: str, fn: Callable[[], Any]):
    try:
        return jsonify(fn())
    except RuntimeError as exc:
        return service_unavailable(str(exc), operation=operation)
    except Exception as exc:
        return service_unavailable(str(exc), operation=operation)


@app.get("/account")
@require_auth
def get_account():
    return _run_bridge_operation("get_account", lambda: get_ibkr_client().get_account())


@app.get("/positions")
@require_auth
def get_positions():
    return _run_bridge_operation("get_positions", lambda: get_ibkr_client().get_positions())


@app.get("/market-data/intraday")
@require_auth
def get_intraday_market_data():
    def fetch_intraday():
        symbol = str(request.args.get("symbol", "")).strip().upper()
        interval = str(request.args.get("interval", "1min")).strip().lower() or "1min"
        outputsize_raw = request.args.get("outputsize", "0")
        try:
            outputsize = int(outputsize_raw) if str(outputsize_raw).strip() else None
        except Exception:
            outputsize = None
        return get_ibkr_client().get_intraday_candles(symbol, interval=interval, outputsize=outputsize)

    return _run_bridge_operation("get_intraday_market_data", fetch_intraday)


@app.get("/orders/open")
@require_auth
def get_open_orders():
    return _run_bridge_operation("get_open_orders", lambda: get_ibkr_client().get_open_orders())


@app.get("/orders/<order_id>")
@require_auth
def get_order(order_id: str):
    def build_order():
        order = get_ibkr_client().get_order(order_id)
        if not order:
            raise RuntimeError(f"Order '{order_id}' was not found in open IBKR trades.")
        return order

    return _run_bridge_operation(f"get_order:{order_id}", build_order)


@app.get("/orders/<order_id>/sync")
@require_auth
def sync_order(order_id: str):
    def sync_open_order():
        order = get_ibkr_client().get_order(order_id)
        if order:
            return order
        return {
            "id": str(order_id).strip(),
            "status": "unknown",
            "message": "Order was not found in current open IBKR trades.",
        }

    return _run_bridge_operation(f"sync_order:{order_id}", sync_open_order)


@app.post("/orders/paper-bracket")
@require_auth
def place_paper_bracket():
    def place_bracket():
        payload = request.get_json(silent=True) or {}
        trade = payload.get("trade") or {}
        max_notional_raw = payload.get("max_notional")
        max_notional = None if max_notional_raw in (None, "") else float(max_notional_raw)
        return get_ibkr_client().place_paper_bracket_order(trade, max_notional=max_notional)

    return _run_bridge_operation("place_paper_bracket", place_bracket)


@app.post("/orders/cancel-by-symbol")
@require_auth
def cancel_orders_by_symbol():
    def cancel_for_symbol():
        payload = request.get_json(silent=True) or {}
        symbol = str(payload.get("symbol", "")).strip().upper()
        canceled_order_ids = get_ibkr_client().cancel_orders_by_symbol(symbol)
        return {
            "ok": True,
            "symbol": symbol,
            "canceled_order_ids": canceled_order_ids,
        }

    return _run_bridge_operation("cancel_orders_by_symbol", cancel_for_symbol)


@app.post("/positions/close")
@require_auth
def close_position():
    def close_for_symbol():
        payload = request.get_json(silent=True) or {}
        symbol = str(payload.get("symbol", "")).strip().upper()
        return get_ibkr_client().close_position(symbol)

    return _run_bridge_operation("close_position", close_for_symbol)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8090")), debug=False)
