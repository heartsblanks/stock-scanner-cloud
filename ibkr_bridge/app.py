from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable

from flask import Flask, jsonify, request
from core.logging_utils import log_exception, log_info
from ibkr_bridge.connector import get_ibkr_client


app = Flask(__name__)


def _normalize_journalctl_timestamp(raw_value: str) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return value
    candidate = value
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except Exception:
        return value
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def _truthy_env(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, str(default))).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _success_audit_enabled() -> bool:
    return _truthy_env("ENABLE_IBKR_BRIDGE_SUCCESS_AUDIT", True)


def _full_payload_audit_enabled() -> bool:
    return _truthy_env("ENABLE_IBKR_BRIDGE_FULL_PAYLOAD_AUDIT", False)


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


def _audit_success(message: str, *, operation: str, payload: Any, summary: dict[str, Any] | None = None) -> None:
    if not _success_audit_enabled():
        return

    fields: dict[str, Any] = {
        "component": "ibkr_bridge",
        "operation": operation,
    }
    if summary:
        fields.update(summary)
    if _full_payload_audit_enabled():
        fields["payload"] = payload
    log_info(message, **fields)


@app.get("/health")
def health():
    client = get_ibkr_client()
    return jsonify({"ok": True, "service": "ibkr-bridge", "ibkr": client.health_snapshot()})


def _run_bridge_operation(operation: str, fn: Callable[[], Any]):
    try:
        return jsonify(fn())
    except RuntimeError as exc:
        log_exception(
            "IBKR bridge runtime error",
            exc,
            component="ibkr_bridge",
            operation=operation,
            method=request.method,
            path=request.path,
            remote_addr=request.remote_addr,
        )
        return service_unavailable(str(exc), operation=operation)
    except Exception as exc:
        log_exception(
            "IBKR bridge unexpected error",
            exc,
            component="ibkr_bridge",
            operation=operation,
            method=request.method,
            path=request.path,
            remote_addr=request.remote_addr,
        )
        return service_unavailable(str(exc), operation=operation)


@app.get("/account")
@require_auth
def get_account():
    def fetch_account():
        payload = get_ibkr_client().get_account()
        _audit_success(
            "IBKR bridge account fetched",
            operation="get_account",
            payload=payload,
            summary={
                "account_id": payload.get("account_id"),
                "equity": payload.get("equity"),
                "buying_power": payload.get("buying_power"),
                "status": payload.get("status"),
            },
        )
        return payload

    return _run_bridge_operation("get_account", fetch_account)


@app.get("/positions")
@require_auth
def get_positions():
    def fetch_positions():
        payload = get_ibkr_client().get_positions()
        _audit_success(
            "IBKR bridge positions fetched",
            operation="get_positions",
            payload=payload,
            summary={"count": len(payload or [])},
        )
        return payload

    return _run_bridge_operation("get_positions", fetch_positions)


@app.get("/open-state")
@require_auth
def get_open_state():
    def fetch_open_state():
        payload = get_ibkr_client().get_open_state()
        _audit_success(
            "IBKR bridge open state fetched",
            operation="get_open_state",
            payload=payload,
            summary={
                "position_count": len(payload.get("positions") or []),
                "order_count": len(payload.get("orders") or []),
            },
        )
        return payload

    return _run_bridge_operation("get_open_state", fetch_open_state)


@app.get("/market-data/intraday")
@require_auth
def get_intraday_market_data():
    def fetch_intraday():
        symbol = str(request.args.get("symbol", "")).strip().upper()
        interval = str(request.args.get("interval", "1min")).strip().lower() or "1min"
        exchange = str(request.args.get("exchange", "")).strip().upper() or None
        primary_exchange = str(request.args.get("primary_exchange", "")).strip().upper() or None
        currency = str(request.args.get("currency", "")).strip().upper() or None
        outputsize_raw = request.args.get("outputsize", "0")
        try:
            outputsize = int(outputsize_raw) if str(outputsize_raw).strip() else None
        except Exception:
            outputsize = None
        payload = get_ibkr_client().get_intraday_candles(
            symbol,
            interval=interval,
            outputsize=outputsize,
            exchange=exchange,
            primary_exchange=primary_exchange,
            currency=currency,
        )
        _audit_success(
            "IBKR bridge intraday candles fetched",
            operation="get_intraday_market_data",
            payload=payload,
            summary={
                "symbol": symbol,
                "interval": interval,
                "outputsize": outputsize,
                "exchange": exchange,
                "primary_exchange": primary_exchange,
                "currency": currency,
                "count": len(payload or []),
            },
        )
        return payload

    return _run_bridge_operation("get_intraday_market_data", fetch_intraday)


@app.get("/orders/open")
@require_auth
def get_open_orders():
    def fetch_open_orders():
        payload = get_ibkr_client().get_open_orders()
        _audit_success(
            "IBKR bridge open orders fetched",
            operation="get_open_orders",
            payload=payload,
            summary={"count": len(payload or [])},
        )
        return payload

    return _run_bridge_operation("get_open_orders", fetch_open_orders)


@app.get("/journal")
@require_auth
def get_bridge_journal():
    def fetch_journal():
        since = _normalize_journalctl_timestamp(str(request.args.get("since", "")).strip())
        until = _normalize_journalctl_timestamp(str(request.args.get("until", "")).strip())
        unit = str(request.args.get("unit", "ibkr-bridge")).strip() or "ibkr-bridge"
        if not since or not until:
            raise RuntimeError("since and until are required")

        result = subprocess.run(
            ["journalctl", "-u", unit, "--since", since, "--until", until, "--no-pager"],
            check=False,
            capture_output=True,
            text=True,
        )
        stdout_lines = result.stdout.splitlines()
        no_entries = (
            result.returncode == 1
            and len(stdout_lines) == 1
            and stdout_lines[0].strip().lower().startswith("-- no entries")
        )
        if result.returncode not in (0, 1) or (result.returncode == 1 and not no_entries):
            raise RuntimeError(
                "journalctl failed "
                f"(code={result.returncode}) "
                f"stderr={result.stderr.strip()!r} stdout={result.stdout.strip()!r}"
            )
        lines = [] if no_entries else stdout_lines
        payload = {
            "ok": True,
            "unit": unit,
            "since": since,
            "until": until,
            "line_count": len(lines),
            "lines": lines,
        }
        _audit_success(
            "IBKR bridge journal fetched",
            operation="get_bridge_journal",
            payload=payload,
            summary={"unit": unit, "since": since, "until": until, "line_count": len(lines)},
        )
        return payload

    return _run_bridge_operation("get_bridge_journal", fetch_journal)


@app.get("/orders/<order_id>")
@require_auth
def get_order(order_id: str):
    def build_order():
        order = get_ibkr_client().get_order(order_id)
        if not order:
            raise RuntimeError(f"Order '{order_id}' was not found in open IBKR trades.")
        _audit_success(
            "IBKR bridge order fetched",
            operation=f"get_order:{order_id}",
            payload=order,
            summary={
                "order_id": order.get("id"),
                "symbol": order.get("symbol"),
                "status": order.get("status"),
            },
        )
        return order

    return _run_bridge_operation(f"get_order:{order_id}", build_order)


@app.get("/orders/<order_id>/sync")
@require_auth
def sync_order(order_id: str):
    def sync_open_order():
        order = get_ibkr_client().sync_order(order_id)
        if order and str(order.get("status", "")).strip().lower() != "unknown":
            _audit_success(
                "IBKR bridge order synced",
                operation=f"sync_order:{order_id}",
                payload=order,
                summary={
                    "order_id": order.get("id"),
                    "symbol": order.get("symbol"),
                    "status": order.get("status"),
                },
            )
            return order
        payload = {
            "id": str(order_id).strip(),
            "status": "unknown",
            "message": "Order was not found in current open IBKR trades.",
        }
        _audit_success(
            "IBKR bridge order sync returned unknown",
            operation=f"sync_order:{order_id}",
            payload=payload,
            summary={"order_id": payload["id"], "status": payload["status"]},
        )
        return payload

    return _run_bridge_operation(f"sync_order:{order_id}", sync_open_order)


@app.post("/orders/sync-batch")
@require_auth
def sync_orders_batch():
    def sync_open_orders_batch():
        payload = request.get_json(silent=True) or {}
        raw_order_ids = payload.get("order_ids")
        if not isinstance(raw_order_ids, list):
            raise RuntimeError("order_ids must be provided as a list")

        sync_payload = get_ibkr_client().sync_orders(raw_order_ids)
        _audit_success(
            "IBKR bridge orders synced in batch",
            operation="sync_orders_batch",
            payload=sync_payload,
            summary={
                "requested_count": sync_payload.get("requested_count", 0),
                "synced_count": sync_payload.get("synced_count", 0),
                "unknown_count": sync_payload.get("unknown_count", 0),
                "duration_ms": ((sync_payload.get("durations_ms") or {}).get("total", 0)),
            },
        )
        return sync_payload

    return _run_bridge_operation("sync_orders_batch", sync_open_orders_batch)


@app.post("/orders/paper-bracket")
@require_auth
def place_paper_bracket():
    def place_bracket():
        payload = request.get_json(silent=True) or {}
        trade = payload.get("trade") or {}
        max_notional_raw = payload.get("max_notional")
        max_notional = None if max_notional_raw in (None, "") else float(max_notional_raw)
        result = get_ibkr_client().place_paper_bracket_order(trade, max_notional=max_notional)
        _audit_success(
            "IBKR bridge paper bracket processed",
            operation="place_paper_bracket",
            payload=result,
            summary={
                "symbol": result.get("symbol"),
                "placed": result.get("placed"),
                "reason": result.get("reason"),
                "broker_order_id": result.get("broker_order_id") or result.get("order_id"),
            },
        )
        return result

    return _run_bridge_operation("place_paper_bracket", place_bracket)


@app.post("/orders/cancel-by-symbol")
@require_auth
def cancel_orders_by_symbol():
    def cancel_for_symbol():
        payload = request.get_json(silent=True) or {}
        symbol = str(payload.get("symbol", "")).strip().upper()
        canceled_order_ids = get_ibkr_client().cancel_orders_by_symbol(symbol)
        result = {
            "ok": True,
            "symbol": symbol,
            "canceled_order_ids": canceled_order_ids,
        }
        _audit_success(
            "IBKR bridge cancel-by-symbol processed",
            operation="cancel_orders_by_symbol",
            payload=result,
            summary={"symbol": symbol, "canceled_count": len(canceled_order_ids)},
        )
        return result

    return _run_bridge_operation("cancel_orders_by_symbol", cancel_for_symbol)


@app.post("/positions/close")
@require_auth
def close_position():
    def close_for_symbol():
        payload = request.get_json(silent=True) or {}
        symbol = str(payload.get("symbol", "")).strip().upper()
        result = get_ibkr_client().close_position(symbol)
        _audit_success(
            "IBKR bridge close-position processed",
            operation="close_position",
            payload=result,
            summary={
                "symbol": symbol,
                "placed": result.get("placed"),
                "reason": result.get("reason"),
                "order_id": result.get("order_id"),
            },
        )
        return result

    return _run_bridge_operation("close_position", close_for_symbol)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8090")), debug=False)
