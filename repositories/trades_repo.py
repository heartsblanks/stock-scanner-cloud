from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from core.db import execute, fetch_all, fetch_one
from repositories.common import normalize_text, to_optional_float


def insert_trade_event(
    event_time: datetime,
    event_type: str,
    symbol: str,
    side: Optional[str] = None,
    shares: Optional[float] = None,
    price: Optional[float] = None,
    mode: Optional[str] = None,
    broker: Optional[str] = None,
    order_id: Optional[str] = None,
    parent_order_id: Optional[str] = None,
    status: Optional[str] = None,
) -> None:
    existing = fetch_one(
        """
        SELECT id
        FROM trade_events
        WHERE event_time = %(event_time)s
          AND event_type = %(event_type)s
          AND symbol = %(symbol)s
          AND COALESCE(side, '') = %(side)s
          AND COALESCE(shares, -1) = %(shares_match)s
          AND COALESCE(price, -1) = %(price_match)s
          AND COALESCE(mode, '') = %(mode)s
          AND COALESCE(broker, '') = %(broker)s
          AND COALESCE(order_id, '') = %(order_id)s
          AND COALESCE(parent_order_id, '') = %(parent_order_id)s
          AND COALESCE(status, '') = %(status)s
        LIMIT 1
        """,
        {
            "event_time": event_time,
            "event_type": event_type,
            "symbol": symbol,
            "side": normalize_text(side),
            "shares_match": shares if shares is not None else -1,
            "price_match": price if price is not None else -1,
            "mode": normalize_text(mode),
            "broker": normalize_text(broker),
            "order_id": normalize_text(order_id),
            "parent_order_id": normalize_text(parent_order_id),
            "status": normalize_text(status),
        },
    )
    if existing:
        return
    execute(
        """
        INSERT INTO trade_events (
            event_time, event_type, symbol, side, shares, price, mode, broker, order_id, parent_order_id, status
        ) VALUES (
            %(event_time)s, %(event_type)s, %(symbol)s, %(side)s, %(shares)s, %(price)s, %(mode)s, %(broker)s, %(order_id)s, %(parent_order_id)s, %(status)s
        )
        """,
        {
            "event_time": event_time,
            "event_type": event_type,
            "symbol": symbol,
            "side": side,
            "shares": shares,
            "price": price,
            "mode": mode,
            "broker": broker,
            "order_id": order_id,
            "parent_order_id": parent_order_id,
            "status": status,
        },
    )


def get_recent_trade_events(limit: int = 100) -> list[dict]:
    return fetch_all("SELECT * FROM trade_events ORDER BY event_time DESC, id DESC LIMIT %(limit)s", {"limit": limit})


def get_trade_event_by_order_id(order_id: str) -> Optional[dict]:
    return fetch_one("SELECT * FROM trade_events WHERE order_id = %(order_id)s ORDER BY event_time DESC, id DESC LIMIT 1", {"order_id": order_id})


def get_recent_trade_event_rows(limit: int = 100, broker: Optional[str] = None) -> list[dict]:
    normalized_broker = normalize_text(broker).upper() if broker else ""
    if normalized_broker:
        return fetch_all(
            """
            SELECT *
            FROM trade_events
            WHERE UPPER(COALESCE(broker, '')) = %(broker)s
            ORDER BY event_time DESC, id DESC
            LIMIT %(limit)s
            """,
            {"limit": limit, "broker": normalized_broker},
        )
    return fetch_all("SELECT * FROM trade_events ORDER BY event_time DESC, id DESC LIMIT %(limit)s", {"limit": limit})


def get_trade_event_rows_for_date(target_date: str, limit: int = 1000) -> list[dict]:
    return fetch_all(
        """
        SELECT
            event_time AS timestamp_utc, event_type, symbol, mode, COALESCE(side, '') AS side,
            COALESCE(broker, '') AS broker,
            COALESCE(shares::text, '') AS shares, COALESCE(price::text, '') AS price,
            COALESCE(order_id, '') AS broker_order_id, COALESCE(parent_order_id, '') AS broker_parent_order_id,
            COALESCE(status, '') AS status
        FROM trade_events
        WHERE event_time::date = %(target_date)s::date
        ORDER BY event_time ASC, id ASC
        LIMIT %(limit)s
        """,
        {"target_date": target_date, "limit": limit},
    )


def get_trade_event_counts_by_type() -> list[dict]:
    return fetch_all(
        "SELECT event_type, COUNT(*)::INT AS count FROM trade_events GROUP BY event_type ORDER BY count DESC, event_type ASC",
        {},
    )


def get_open_trade_events(limit: int = 100, broker: Optional[str] = None) -> list[dict]:
    broker_filter = ""
    params: dict[str, Any] = {"limit": limit}
    if broker:
        broker_filter = "AND UPPER(COALESCE(te.broker, '')) = %(broker)s"
        params["broker"] = normalize_text(broker).upper()
    return fetch_all(
        f"""
        WITH ranked AS (
            SELECT te.*, ROW_NUMBER() OVER (
                PARTITION BY COALESCE(NULLIF(parent_order_id, ''), NULLIF(order_id, ''), symbol)
                ORDER BY event_time DESC, id DESC
            ) AS rn
            FROM trade_events te
            WHERE 1 = 1
              {broker_filter}
        )
        SELECT * FROM ranked
        WHERE rn = 1 AND UPPER(COALESCE(status, '')) = 'OPEN'
        ORDER BY event_time DESC, id DESC
        LIMIT %(limit)s
        """,
        params,
    )


def get_closed_trade_events(limit: int = 100, broker: Optional[str] = None) -> list[dict]:
    normalized_broker = normalize_text(broker).upper() if broker else ""
    if normalized_broker:
        return fetch_all(
            """
            SELECT *
            FROM trade_events
            WHERE UPPER(COALESCE(status, '')) = 'CLOSED'
              AND UPPER(COALESCE(broker, '')) = %(broker)s
            ORDER BY event_time DESC, id DESC
            LIMIT %(limit)s
            """,
            {"limit": limit, "broker": normalized_broker},
        )
    return fetch_all(
        "SELECT * FROM trade_events WHERE UPPER(COALESCE(status, '')) = 'CLOSED' ORDER BY event_time DESC, id DESC LIMIT %(limit)s",
        {"limit": limit},
    )


def upsert_trade_lifecycle(
    trade_key: str,
    symbol: Optional[str] = None,
    mode: Optional[str] = None,
    side: Optional[str] = None,
    direction: Optional[str] = None,
    status: Optional[str] = None,
    entry_time: Optional[datetime] = None,
    entry_price: Optional[float] = None,
    exit_time: Optional[datetime] = None,
    exit_price: Optional[float] = None,
    stop_price: Optional[float] = None,
    target_price: Optional[float] = None,
    exit_reason: Optional[str] = None,
    shares: Optional[float] = None,
    realized_pnl: Optional[float] = None,
    realized_pnl_percent: Optional[float] = None,
    duration_minutes: Optional[float] = None,
    signal_timestamp: Optional[datetime] = None,
    signal_entry: Optional[float] = None,
    signal_stop: Optional[float] = None,
    signal_target: Optional[float] = None,
    signal_confidence: Optional[float] = None,
    broker: Optional[str] = None,
    order_id: Optional[str] = None,
    parent_order_id: Optional[str] = None,
    exit_order_id: Optional[str] = None,
) -> None:
    normalized_trade_key = normalize_text(trade_key)
    if not normalized_trade_key:
        raise ValueError("trade_key is required")
    existing = fetch_one("SELECT id FROM trade_lifecycles WHERE trade_key = %(trade_key)s LIMIT 1", {"trade_key": normalized_trade_key})
    params = {
        "trade_key": normalized_trade_key,
        "symbol": symbol,
        "mode": mode,
        "side": side,
        "direction": direction,
        "status": status,
        "entry_time": entry_time,
        "entry_price": entry_price,
        "exit_time": exit_time,
        "exit_price": exit_price,
        "stop_price": stop_price,
        "target_price": target_price,
        "exit_reason": exit_reason,
        "shares": shares,
        "realized_pnl": realized_pnl,
        "realized_pnl_percent": realized_pnl_percent,
        "duration_minutes": duration_minutes,
        "signal_timestamp": signal_timestamp,
        "signal_entry": signal_entry,
        "signal_stop": signal_stop,
        "signal_target": signal_target,
        "signal_confidence": signal_confidence,
        "broker": broker,
        "order_id": order_id,
        "parent_order_id": parent_order_id,
        "exit_order_id": exit_order_id,
    }
    if existing:
        params["id"] = existing["id"]
        execute(
            """
            UPDATE trade_lifecycles
            SET symbol = %(symbol)s, mode = COALESCE(NULLIF(%(mode)s, ''), mode), side = %(side)s, direction = %(direction)s, status = %(status)s,
                entry_time = %(entry_time)s, entry_price = %(entry_price)s, exit_time = %(exit_time)s, exit_price = %(exit_price)s,
                stop_price = %(stop_price)s, target_price = %(target_price)s, exit_reason = %(exit_reason)s, shares = %(shares)s,
                realized_pnl = %(realized_pnl)s, realized_pnl_percent = %(realized_pnl_percent)s, duration_minutes = %(duration_minutes)s,
                signal_timestamp = %(signal_timestamp)s, signal_entry = %(signal_entry)s, signal_stop = %(signal_stop)s,
                signal_target = %(signal_target)s, signal_confidence = %(signal_confidence)s, broker = COALESCE(NULLIF(%(broker)s, ''), broker), order_id = %(order_id)s,
                parent_order_id = %(parent_order_id)s, exit_order_id = %(exit_order_id)s, updated_at = NOW()
            WHERE id = %(id)s
            """,
            params,
        )
        return
    execute(
        """
        INSERT INTO trade_lifecycles (
            trade_key, symbol, mode, side, direction, status, entry_time, entry_price, exit_time, exit_price,
            stop_price, target_price, exit_reason, shares, realized_pnl, realized_pnl_percent, duration_minutes,
            signal_timestamp, signal_entry, signal_stop, signal_target, signal_confidence, broker, order_id, parent_order_id, exit_order_id
        ) VALUES (
            %(trade_key)s, %(symbol)s, %(mode)s, %(side)s, %(direction)s, %(status)s, %(entry_time)s, %(entry_price)s, %(exit_time)s, %(exit_price)s,
            %(stop_price)s, %(target_price)s, %(exit_reason)s, %(shares)s, %(realized_pnl)s, %(realized_pnl_percent)s, %(duration_minutes)s,
            %(signal_timestamp)s, %(signal_entry)s, %(signal_stop)s, %(signal_target)s, %(signal_confidence)s, %(broker)s, %(order_id)s, %(parent_order_id)s, %(exit_order_id)s
        )
        """,
        params,
    )


def get_trade_lifecycles(limit: int = 100, status: Optional[str] = None, broker: Optional[str] = None) -> list[dict]:
    normalized_broker = normalize_text(broker).upper() if broker else ""
    if status and normalized_broker:
        return fetch_all(
            """
            SELECT *
            FROM trade_lifecycles
            WHERE UPPER(COALESCE(status, '')) = %(status)s
              AND UPPER(COALESCE(broker, '')) = %(broker)s
            ORDER BY COALESCE(entry_time, created_at) DESC, id DESC
            LIMIT %(limit)s
            """,
            {"status": normalize_text(status).upper(), "broker": normalized_broker, "limit": limit},
        )
    if status:
        return fetch_all(
            """
            SELECT *
            FROM trade_lifecycles
            WHERE UPPER(COALESCE(status, '')) = %(status)s
            ORDER BY COALESCE(entry_time, created_at) DESC, id DESC
            LIMIT %(limit)s
            """,
            {"status": normalize_text(status).upper(), "limit": limit},
        )
    if normalized_broker:
        return fetch_all(
            """
            SELECT *
            FROM trade_lifecycles
            WHERE UPPER(COALESCE(broker, '')) = %(broker)s
            ORDER BY COALESCE(entry_time, created_at) DESC, id DESC
            LIMIT %(limit)s
            """,
            {"broker": normalized_broker, "limit": limit},
        )
    return fetch_all(
        "SELECT * FROM trade_lifecycles ORDER BY COALESCE(entry_time, created_at) DESC, id DESC LIMIT %(limit)s",
        {"limit": limit},
    )


def get_recent_closed_trade_lifecycle_for_symbol(symbol: str) -> Optional[dict]:
    normalized_symbol = normalize_text(symbol).upper()
    if not normalized_symbol:
        return None
    return fetch_one(
        """
        SELECT *
        FROM trade_lifecycles
        WHERE UPPER(symbol) = %(symbol)s AND UPPER(COALESCE(status, '')) = 'CLOSED'
        ORDER BY COALESCE(exit_time, updated_at, created_at) DESC, id DESC
        LIMIT 1
        """,
        {"symbol": normalized_symbol},
    )


def get_latest_open_trade_lifecycle(symbol: str, *, parent_order_id: Optional[str] = None) -> Optional[dict]:
    normalized_symbol = normalize_text(symbol).upper()
    normalized_parent_order_id = normalize_text(parent_order_id)
    if not normalized_symbol:
        return None
    if normalized_parent_order_id:
        row = fetch_one(
            """
            SELECT *
            FROM trade_lifecycles
            WHERE UPPER(symbol) = %(symbol)s
              AND UPPER(COALESCE(status, '')) = 'OPEN'
              AND COALESCE(parent_order_id, '') = %(parent_order_id)s
            ORDER BY COALESCE(entry_time, updated_at, created_at) DESC, id DESC
            LIMIT 1
            """,
            {"symbol": normalized_symbol, "parent_order_id": normalized_parent_order_id},
        )
        if row:
            return row
    return fetch_one(
        """
        SELECT *
        FROM trade_lifecycles
        WHERE UPPER(symbol) = %(symbol)s AND UPPER(COALESCE(status, '')) = 'OPEN'
        ORDER BY COALESCE(entry_time, updated_at, created_at) DESC, id DESC
        LIMIT 1
        """,
        {"symbol": normalized_symbol},
    )


def get_trade_lifecycle_summary_from_table(limit: int = 1000, broker: Optional[str] = None) -> dict:
    rows = get_trade_lifecycles(limit=limit, broker=broker)
    closed_rows = [row for row in rows if str(row.get("status", "")).strip().upper() == "CLOSED"]
    pnl_values = [to_optional_float(row.get("realized_pnl")) for row in closed_rows if to_optional_float(row.get("realized_pnl")) is not None]
    wins = [value for value in pnl_values if value > 0]
    losses = [value for value in pnl_values if value < 0]
    flats = [value for value in pnl_values if value == 0]
    return {
        "trade_count": len(rows),
        "closed_trade_count": len(closed_rows),
        "open_trade_count": len([row for row in rows if str(row.get("status", "")).strip().upper() == "OPEN"]),
        "realized_pnl_total": round(sum(pnl_values), 6) if pnl_values else 0.0,
        "winning_trade_count": len(wins),
        "losing_trade_count": len(losses),
        "flat_trade_count": len(flats),
        "average_realized_pnl": round(sum(pnl_values) / len(pnl_values), 6) if pnl_values else None,
        "best_trade_pnl": max(pnl_values) if pnl_values else None,
        "worst_trade_pnl": min(pnl_values) if pnl_values else None,
    }


def get_daily_realized_pnl(target_date: str) -> float:
    row = fetch_one(
        """
        SELECT ROUND(COALESCE(SUM(realized_pnl), 0)::numeric, 6) AS realized_pnl_total
        FROM trade_lifecycles
        WHERE UPPER(COALESCE(status, '')) = 'CLOSED'
          AND exit_time IS NOT NULL
          AND exit_time::date = %(target_date)s::date
        """,
        {"target_date": target_date},
    )
    if not row:
        return 0.0
    value = row.get("realized_pnl_total")
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def get_trade_lifecycles_for_date(target_date: str, limit: int = 1000) -> list[dict]:
    return fetch_all(
        """
        SELECT *
        FROM trade_lifecycles
        WHERE (entry_time::date = %(target_date)s::date OR exit_time::date = %(target_date)s::date)
        ORDER BY COALESCE(entry_time, created_at) DESC, id DESC
        LIMIT %(limit)s
        """,
        {"target_date": target_date, "limit": limit},
    )


def get_trade_lifecycle_summary_for_date(target_date: str, limit: int = 5000) -> dict:
    rows = get_trade_lifecycles_for_date(target_date=target_date, limit=limit)
    closed_rows = [row for row in rows if str(row.get("status", "")).strip().upper() == "CLOSED"]
    open_rows = [row for row in rows if str(row.get("status", "")).strip().upper() == "OPEN"]
    pnl_values = [to_optional_float(row.get("realized_pnl")) for row in closed_rows if to_optional_float(row.get("realized_pnl")) is not None]
    duration_values = [to_optional_float(row.get("duration_minutes")) for row in closed_rows if to_optional_float(row.get("duration_minutes")) is not None]
    wins = [value for value in pnl_values if value > 0]
    losses = [value for value in pnl_values if value < 0]
    flats = [value for value in pnl_values if value == 0]
    return {
        "date": target_date,
        "trade_count": len(rows),
        "closed_trade_count": len(closed_rows),
        "open_trade_count": len(open_rows),
        "realized_pnl_total": round(sum(pnl_values), 6) if pnl_values else 0.0,
        "winning_trade_count": len(wins),
        "losing_trade_count": len(losses),
        "flat_trade_count": len(flats),
        "win_rate_percent": round((len(wins) / len(closed_rows)) * 100.0, 6) if closed_rows else None,
        "average_realized_pnl": round(sum(pnl_values) / len(pnl_values), 6) if pnl_values else None,
        "best_trade_pnl": max(pnl_values) if pnl_values else None,
        "worst_trade_pnl": min(pnl_values) if pnl_values else None,
        "average_duration_minutes": round(sum(duration_values) / len(duration_values), 6) if duration_values else None,
    }


def get_symbol_performance(limit: int = 100) -> list[dict]:
    return fetch_all(
        """
        SELECT symbol, COUNT(*)::INT AS trade_count,
               COUNT(*) FILTER (WHERE UPPER(COALESCE(status, '')) = 'CLOSED')::INT AS closed_trade_count,
               COUNT(*) FILTER (WHERE COALESCE(realized_pnl, 0) > 0)::INT AS winning_trade_count,
               COUNT(*) FILTER (WHERE COALESCE(realized_pnl, 0) < 0)::INT AS losing_trade_count,
               ROUND(COALESCE(SUM(realized_pnl), 0)::numeric, 6) AS realized_pnl_total,
               ROUND(AVG(realized_pnl)::numeric, 6) AS average_realized_pnl,
               ROUND(AVG(duration_minutes)::numeric, 6) AS average_duration_minutes
        FROM trade_lifecycles
        GROUP BY symbol
        ORDER BY realized_pnl_total DESC, symbol ASC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )


def get_mode_performance(limit: int = 100) -> list[dict]:
    return fetch_all(
        """
        SELECT COALESCE(mode, '') AS mode, COUNT(*)::INT AS trade_count,
               COUNT(*) FILTER (WHERE UPPER(COALESCE(status, '')) = 'CLOSED')::INT AS closed_trade_count,
               COUNT(*) FILTER (WHERE COALESCE(realized_pnl, 0) > 0)::INT AS winning_trade_count,
               COUNT(*) FILTER (WHERE COALESCE(realized_pnl, 0) < 0)::INT AS losing_trade_count,
               ROUND(COALESCE(SUM(realized_pnl), 0)::numeric, 6) AS realized_pnl_total,
               ROUND(AVG(realized_pnl)::numeric, 6) AS average_realized_pnl,
               ROUND(AVG(duration_minutes)::numeric, 6) AS average_duration_minutes
        FROM trade_lifecycles
        GROUP BY COALESCE(mode, '')
        ORDER BY realized_pnl_total DESC, mode ASC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )


def get_exit_reason_breakdown(limit: int = 100) -> list[dict]:
    return fetch_all(
        """
        SELECT COALESCE(exit_reason, '') AS exit_reason,
               COUNT(*)::INT AS trade_count,
               ROUND(COALESCE(SUM(realized_pnl), 0)::numeric, 6) AS realized_pnl_total,
               ROUND(AVG(realized_pnl)::numeric, 6) AS average_realized_pnl
        FROM trade_lifecycles
        WHERE UPPER(COALESCE(status, '')) = 'CLOSED'
        GROUP BY COALESCE(exit_reason, '')
        ORDER BY trade_count DESC, exit_reason ASC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )


def get_external_exit_summary() -> dict | None:
    row = fetch_one(
        """
        SELECT COUNT(*)::INT AS trade_count,
               ROUND(COALESCE(SUM(realized_pnl), 0)::numeric, 6) AS realized_pnl_total,
               ROUND(AVG(realized_pnl)::numeric, 6) AS average_realized_pnl
        FROM trade_lifecycles
        WHERE UPPER(COALESCE(status, '')) = 'CLOSED'
          AND UPPER(COALESCE(exit_reason, '')) = 'EXTERNAL_EXIT'
        """,
        {},
    )
    if not row or not row.get("trade_count"):
        return None
    return row


def get_hourly_performance(limit: int = 100) -> list[dict]:
    return fetch_all(
        """
        SELECT EXTRACT(HOUR FROM entry_time)::INT AS entry_hour_utc,
               COUNT(*)::INT AS trade_count,
               COUNT(*) FILTER (WHERE UPPER(COALESCE(status, '')) = 'CLOSED')::INT AS closed_trade_count,
               ROUND(COALESCE(SUM(realized_pnl), 0)::numeric, 6) AS realized_pnl_total,
               ROUND(AVG(realized_pnl)::numeric, 6) AS average_realized_pnl,
               ROUND(AVG(duration_minutes)::numeric, 6) AS average_duration_minutes
        FROM trade_lifecycles
        WHERE entry_time IS NOT NULL
        GROUP BY EXTRACT(HOUR FROM entry_time)
        ORDER BY entry_hour_utc ASC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )


def get_hourly_outcome_quality(limit: int = 24, *, exclude_external_exit: bool = False) -> list[dict]:
    external_exit_filter = ""
    if exclude_external_exit:
        external_exit_filter = "AND UPPER(COALESCE(exit_reason, '')) <> 'EXTERNAL_EXIT'"
    return fetch_all(
        f"""
        SELECT EXTRACT(HOUR FROM (entry_time AT TIME ZONE 'America/New_York'))::INT AS entry_hour_ny,
               COUNT(*)::INT AS trade_count,
               COUNT(*) FILTER (WHERE UPPER(COALESCE(status, '')) = 'CLOSED')::INT AS closed_trade_count,
               COUNT(*) FILTER (WHERE COALESCE(realized_pnl, 0) > 0)::INT AS winning_trade_count,
               COUNT(*) FILTER (WHERE COALESCE(realized_pnl, 0) < 0)::INT AS losing_trade_count,
               ROUND(COALESCE(SUM(realized_pnl), 0)::numeric, 6) AS realized_pnl_total,
               ROUND(AVG(realized_pnl)::numeric, 6) AS average_realized_pnl,
               CASE
                   WHEN COUNT(*) FILTER (WHERE UPPER(COALESCE(status, '')) = 'CLOSED') > 0
                   THEN ROUND(
                       (
                           COUNT(*) FILTER (WHERE COALESCE(realized_pnl, 0) > 0)::NUMERIC
                           / COUNT(*) FILTER (WHERE UPPER(COALESCE(status, '')) = 'CLOSED')::NUMERIC
                       ) * 100,
                       1
                   )
                   ELSE NULL
               END AS win_rate
        FROM trade_lifecycles
        WHERE entry_time IS NOT NULL
          {external_exit_filter}
        GROUP BY EXTRACT(HOUR FROM (entry_time AT TIME ZONE 'America/New_York'))
        ORDER BY entry_hour_ny ASC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )


def get_equity_curve(limit: int = 5000) -> list[dict]:
    rows = fetch_all(
        """
        SELECT COALESCE(exit_time, entry_time, created_at) AS timestamp, COALESCE(realized_pnl, 0) AS realized_pnl
        FROM trade_lifecycles
        ORDER BY COALESCE(exit_time, entry_time, created_at) ASC, id ASC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )
    cumulative = 0.0
    curve = []
    for row in rows:
        realized_pnl = to_optional_float(row.get("realized_pnl")) or 0.0
        cumulative += realized_pnl
        timestamp = row.get("timestamp")
        curve.append({
            "timestamp": timestamp.isoformat() if hasattr(timestamp, "isoformat") else str(timestamp),
            "realized_pnl": round(realized_pnl, 6),
            "cumulative_pnl": round(cumulative, 6),
        })
    return curve


def get_dashboard_summary(target_date: Optional[str] = None) -> dict:
    base_summary = get_trade_lifecycle_summary_for_date(target_date=target_date, limit=5000) if target_date else get_trade_lifecycle_summary_from_table(limit=5000)
    symbol_performance = get_symbol_performance(limit=10)
    mode_performance = get_mode_performance(limit=10)
    exit_reason_breakdown = get_exit_reason_breakdown(limit=20)
    external_exit_summary = get_external_exit_summary()
    hourly_performance = get_hourly_performance(limit=24)
    hourly_outcome_quality = get_hourly_outcome_quality(limit=24)
    strategy_hourly_outcome_quality = get_hourly_outcome_quality(limit=24, exclude_external_exit=True)
    equity_curve = get_equity_curve(limit=5000)
    return {
        "summary": base_summary,
        "top_symbols": symbol_performance,
        "mode_performance": mode_performance,
        "exit_reason_breakdown": exit_reason_breakdown,
        "external_exit_summary": external_exit_summary,
        "hourly_performance": hourly_performance,
        "hourly_outcome_quality": hourly_outcome_quality,
        "strategy_hourly_outcome_quality": strategy_hourly_outcome_quality,
        "equity_curve": equity_curve,
        "insights": {
            "best_symbol": (symbol_performance[0] if symbol_performance else None),
            "best_mode": (mode_performance[0] if mode_performance else None),
            "most_common_exit": (exit_reason_breakdown[0] if exit_reason_breakdown else None),
            "best_hour": max(hourly_performance, key=lambda x: x.get("realized_pnl_total", 0)) if hourly_performance else None,
            "external_exit_summary": external_exit_summary,
        },
    }
