from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from core.db import execute, fetch_all, fetch_one


def insert_broker_order(
    order_id: str,
    symbol: Optional[str] = None,
    side: Optional[str] = None,
    order_type: Optional[str] = None,
    status: Optional[str] = None,
    qty: Optional[float] = None,
    filled_qty: Optional[float] = None,
    avg_fill_price: Optional[float] = None,
    submitted_at: Optional[datetime] = None,
    filled_at: Optional[datetime] = None,
) -> None:
    existing = fetch_one(
        """
        SELECT id FROM broker_orders WHERE order_id = %(order_id)s ORDER BY created_at DESC, id DESC LIMIT 1
        """,
        {"order_id": order_id},
    )
    if existing:
        execute(
            """
            UPDATE broker_orders
            SET symbol = %(symbol)s, side = %(side)s, order_type = %(order_type)s, status = %(status)s,
                qty = %(qty)s, filled_qty = %(filled_qty)s, avg_fill_price = %(avg_fill_price)s,
                submitted_at = %(submitted_at)s, filled_at = %(filled_at)s
            WHERE id = %(id)s
            """,
            {
                "id": existing["id"],
                "symbol": symbol,
                "side": side,
                "order_type": order_type,
                "status": status,
                "qty": qty,
                "filled_qty": filled_qty,
                "avg_fill_price": avg_fill_price,
                "submitted_at": submitted_at,
                "filled_at": filled_at,
            },
        )
        return
    execute(
        """
        INSERT INTO broker_orders (
            order_id, symbol, side, order_type, status, qty, filled_qty, avg_fill_price, submitted_at, filled_at
        ) VALUES (
            %(order_id)s, %(symbol)s, %(side)s, %(order_type)s, %(status)s, %(qty)s, %(filled_qty)s, %(avg_fill_price)s, %(submitted_at)s, %(filled_at)s
        )
        """,
        {
            "order_id": order_id,
            "symbol": symbol,
            "side": side,
            "order_type": order_type,
            "status": status,
            "qty": qty,
            "filled_qty": filled_qty,
            "avg_fill_price": avg_fill_price,
            "submitted_at": submitted_at,
            "filled_at": filled_at,
        },
    )


def get_broker_order(order_id: str) -> Optional[dict]:
    return fetch_one("SELECT * FROM broker_orders WHERE order_id = %(order_id)s ORDER BY created_at DESC, id DESC LIMIT 1", {"order_id": order_id})


def get_recent_broker_orders(limit: int = 100) -> list[dict]:
    return fetch_all(
        "SELECT * FROM broker_orders ORDER BY COALESCE(submitted_at, created_at) DESC, id DESC LIMIT %(limit)s",
        {"limit": limit},
    )


def get_broker_order_status_counts() -> list[dict]:
    return fetch_all(
        """
        SELECT COALESCE(status, '') AS status, COUNT(*)::INT AS count
        FROM broker_orders
        GROUP BY COALESCE(status, '')
        ORDER BY count DESC, status ASC
        """,
        {},
    )


def insert_alpaca_api_log(
    logged_at: datetime,
    method: str,
    url: str,
    params: Optional[dict[str, Any]] = None,
    request_body: Optional[dict[str, Any]] = None,
    status_code: Optional[int] = None,
    response_body: Optional[str] = None,
    success: Optional[bool] = None,
    error_message: Optional[str] = None,
    duration_ms: Optional[int] = None,
) -> None:
    execute(
        """
        INSERT INTO alpaca_api_logs (
            logged_at, method, url, params_json, request_body_json, status_code, response_body, success, error_message, duration_ms
        ) VALUES (
            %(logged_at)s, %(method)s, %(url)s, %(params_json)s, %(request_body_json)s, %(status_code)s, %(response_body)s, %(success)s, %(error_message)s, %(duration_ms)s
        )
        """,
        {
            "logged_at": logged_at,
            "method": method,
            "url": url,
            "params_json": json.dumps(params, sort_keys=True) if params is not None else None,
            "request_body_json": json.dumps(request_body, sort_keys=True) if request_body is not None else None,
            "status_code": status_code,
            "response_body": response_body,
            "success": success,
            "error_message": error_message,
            "duration_ms": duration_ms,
        },
    )


def get_recent_alpaca_api_logs(limit: int = 100) -> list[dict]:
    return fetch_all("SELECT * FROM alpaca_api_logs ORDER BY logged_at DESC, id DESC LIMIT %(limit)s", {"limit": limit})


def get_recent_alpaca_api_errors(limit: int = 100) -> list[dict]:
    return fetch_all(
        """
        SELECT *
        FROM alpaca_api_logs
        WHERE COALESCE(success, FALSE) = FALSE
           OR COALESCE(status_code, 0) >= 400
           OR COALESCE(error_message, '') <> ''
        ORDER BY logged_at DESC, id DESC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )


def prune_alpaca_api_logs(retention_days: int = 30) -> int:
    row = fetch_one(
        """
        WITH deleted AS (
            DELETE FROM alpaca_api_logs
            WHERE logged_at < NOW() - (%(retention_days)s::text || ' days')::interval
            RETURNING 1
        )
        SELECT COUNT(*)::INT AS deleted_count FROM deleted
        """,
        {"retention_days": max(1, int(retention_days))},
    )
    return int(row["deleted_count"]) if row and row.get("deleted_count") is not None else 0
