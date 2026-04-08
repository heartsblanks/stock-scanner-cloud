from __future__ import annotations

import os
from functools import wraps
from typing import Any, Callable

from flask import Flask, jsonify, request


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


@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "ibkr-bridge"})


@app.get("/account")
@require_auth
def get_account():
    return not_implemented("get_account")


@app.get("/positions")
@require_auth
def get_positions():
    return not_implemented("get_positions")


@app.get("/orders/open")
@require_auth
def get_open_orders():
    return not_implemented("get_open_orders")


@app.get("/orders/<order_id>")
@require_auth
def get_order(order_id: str):
    return not_implemented(f"get_order:{order_id}")


@app.get("/orders/<order_id>/sync")
@require_auth
def sync_order(order_id: str):
    return not_implemented(f"sync_order:{order_id}")


@app.post("/orders/paper-bracket")
@require_auth
def place_paper_bracket():
    return not_implemented("place_paper_bracket")


@app.post("/orders/cancel-by-symbol")
@require_auth
def cancel_orders_by_symbol():
    return not_implemented("cancel_orders_by_symbol")


@app.post("/positions/close")
@require_auth
def close_position():
    return not_implemented("close_position")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8090")), debug=False)

