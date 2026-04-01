from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import requests

from logging_utils import log_exception
from alpaca.paper import get_open_orders, get_open_positions
from scan_context import NY_TZ, parse_iso_utc, to_float_or_none
from storage import (
    get_daily_realized_pnl,
    get_latest_open_trade_lifecycle,
    get_recent_closed_trade_lifecycle_for_symbol,
    get_recent_trade_event_rows,
    get_signal_log_rows,
    get_trade_event_rows_for_date,
    get_trade_lifecycles,
)


PAPER_STOP_COOLDOWN_MINUTES = int(os.getenv("PAPER_STOP_COOLDOWN_MINUTES", "30"))
PAPER_TARGET_COOLDOWN_MINUTES = int(os.getenv("PAPER_TARGET_COOLDOWN_MINUTES", "0"))
PAPER_MANUAL_CLOSE_COOLDOWN_MINUTES = int(os.getenv("PAPER_MANUAL_CLOSE_COOLDOWN_MINUTES", "0"))
TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY")
TWELVEDATA_BASE_URL = "https://api.twelvedata.com/time_series"


def read_trade_rows_for_date(target_date: str) -> list[dict]:
    db_rows = get_trade_event_rows_for_date(target_date=target_date, limit=5000)
    normalized_rows: list[dict] = []

    for row in db_rows:
        event_type = str(row.get("event_type", "")).strip().upper()
        price = str(row.get("price", "") or "").strip()
        broker_order_id = str(row.get("broker_order_id", "") or "").strip()
        broker_parent_order_id = str(row.get("broker_parent_order_id", "") or "").strip()

        normalized_rows.append({
            "timestamp_utc": row.get("timestamp_utc", ""),
            "event_type": event_type,
            "symbol": row.get("symbol", ""),
            "mode": row.get("mode", ""),
            "trade_source": "ALPACA_PAPER" if (broker_order_id or broker_parent_order_id) else "MANUAL",
            "shares": row.get("shares", ""),
            "entry_price": price if event_type == "OPEN" else "",
            "exit_price": price if event_type != "OPEN" else "",
            "notes": "",
            "status": row.get("status", ""),
            "broker_order_id": broker_order_id,
            "broker_parent_order_id": broker_parent_order_id,
        })

    return normalized_rows


def paper_trade_exit_already_logged(parent_order_id: str, exit_event: str) -> bool:
    normalized_parent_order_id = str(parent_order_id).strip()
    normalized_exit_event = str(exit_event).strip().upper()
    if not normalized_parent_order_id or not normalized_exit_event:
        return False

    try:
        rows = get_recent_trade_event_rows(limit=1000)
    except Exception as exc:
        log_exception("Failed to read trade events from DB", exc, component="paper_trade_context", operation="paper_trade_exit_already_logged")
        return False

    for row in rows or []:
        try:
            row_parent_order_id = str(row.get("parent_order_id") or "").strip()
            if row_parent_order_id != normalized_parent_order_id:
                continue

            status = str(row.get("status", "")).strip().upper()
            event_type = str(row.get("event_type", "")).strip().upper()

            if status == "CLOSED" or event_type in {"STOP_HIT", "TARGET_HIT", "MANUAL_CLOSE", "EOD_CLOSE"}:
                return True
        except Exception:
            continue

    return False


def get_open_paper_trades() -> list[dict]:
    try:
        rows = get_trade_lifecycles(limit=1000, status="OPEN")
    except Exception as exc:
        log_exception("Failed to read open trade lifecycles from DB", exc, component="paper_trade_context", operation="get_open_paper_trades")
        return []

    normalized_rows: list[dict] = []
    for row in rows or []:
        try:
            symbol = str(row.get("symbol", "")).strip().upper()
            if not symbol:
                continue

            parent_order_id = str(
                row.get("parent_order_id")
                or row.get("broker_parent_order_id")
                or row.get("order_id")
                or row.get("broker_order_id")
                or ""
            ).strip()
            if not parent_order_id:
                continue

            order_id = str(row.get("order_id") or row.get("broker_order_id") or parent_order_id).strip()

            normalized_rows.append({
                "timestamp_utc": row.get("entry_time") or row.get("timestamp_utc") or "",
                "event_type": "OPEN",
                "symbol": symbol,
                "name": row.get("name") or "",
                "side": row.get("side") or "",
                "direction": row.get("direction") or "",
                "shares": row.get("shares", ""),
                "entry_price": row.get("entry_price", ""),
                "stop_price": row.get("stop_price", ""),
                "target_price": row.get("target_price", ""),
                "status": row.get("status") or "OPEN",
                "exit_reason": row.get("exit_reason") or "",
                "trade_source": "ALPACA_PAPER",
                "broker_order_id": order_id,
                "broker_parent_order_id": parent_order_id,
                "linked_signal_timestamp_utc": row.get("signal_timestamp") or "",
                "linked_signal_entry": row.get("signal_entry") or "",
                "linked_signal_stop": row.get("signal_stop") or "",
                "linked_signal_target": row.get("signal_target") or "",
                "linked_signal_confidence": row.get("signal_confidence") or "",
            })
        except Exception:
            continue

    return normalized_rows


def get_managed_open_paper_trades_for_eod_close() -> list[dict]:
    open_rows = get_open_paper_trades()

    try:
        positions = get_open_positions()
        open_orders = get_open_orders()
    except Exception as exc:
        log_exception("Broker validation for open paper trades failed", exc, component="paper_trade_context", operation="get_managed_open_paper_trades_for_eod_close")
        return open_rows

    open_position_symbols = {
        str(position.get("symbol", "")).strip().upper()
        for position in positions
        if str(position.get("symbol", "")).strip()
    }
    open_order_ids = {
        str(order.get("id", "")).strip()
        for order in open_orders
        if str(order.get("id", "")).strip()
    }

    for order in open_orders:
        for leg in order.get("legs") or []:
            leg_id = str(leg.get("id", "")).strip()
            if leg_id:
                open_order_ids.add(leg_id)

    validated_open_rows = []
    for row in open_rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        parent_order_id = str(row.get("broker_parent_order_id", "")).strip()
        broker_order_id = str(row.get("broker_order_id", "")).strip()
        if symbol in open_position_symbols or parent_order_id in open_order_ids or broker_order_id in open_order_ids:
            validated_open_rows.append(row)

    return validated_open_rows


def get_current_open_position_state() -> tuple[int, float]:
    try:
        positions = get_open_positions()
    except Exception as exc:
        log_exception("Failed to read current open positions for sizing context", exc, component="paper_trade_context", operation="get_current_open_position_state")
        return 0, 0.0

    current_open_positions = 0
    current_open_exposure = 0.0

    for position in positions:
        symbol = str(position.get("symbol", "")).strip().upper()
        if not symbol:
            continue

        current_open_positions += 1
        market_value = to_float_or_none(position.get("market_value"))
        if market_value is not None:
            current_open_exposure += abs(market_value)
            continue

        qty = to_float_or_none(position.get("qty"))
        current_price = to_float_or_none(position.get("current_price"))
        if qty is not None and current_price is not None:
            current_open_exposure += abs(qty * current_price)

    return current_open_positions, current_open_exposure


def get_risk_exposure_summary() -> dict:
    current_open_positions, current_open_exposure = get_current_open_position_state()

    try:
        positions = get_open_positions()
    except Exception as exc:
        log_exception("Failed to read open positions for risk summary", exc, component="paper_trade_context", operation="get_risk_exposure_summary")
        positions = []

    try:
        today_utc = datetime.now(timezone.utc).date().isoformat()
        daily_realized_pnl = get_daily_realized_pnl(today_utc)
    except Exception as exc:
        log_exception("Failed to read daily realized PnL", exc, component="paper_trade_context", operation="get_risk_exposure_summary")
        daily_realized_pnl = 0.0

    daily_unrealized_pnl = 0.0
    for position in positions:
        unrealized_pl = to_float_or_none(position.get("unrealized_pl"))
        if unrealized_pl is not None:
            daily_unrealized_pnl += unrealized_pl

    max_positions = 10
    max_capital_allocation_pct = 0.50
    account_size = 0.0
    try:
        from services.scan_service import _get_live_alpaca_account_equity

        account_size = float(_get_live_alpaca_account_equity({}))
    except Exception as exc:
        log_exception("Failed to resolve live account equity for risk summary", exc, component="paper_trade_context", operation="get_risk_exposure_summary")

    max_total_allocated_capital = account_size * max_capital_allocation_pct
    allocation_used_pct = ((current_open_exposure / max_total_allocated_capital) * 100.0) if max_total_allocated_capital > 0 else 0.0

    return {
        "total_open_exposure": round(current_open_exposure, 2),
        "open_position_count": int(current_open_positions),
        "daily_realized_pnl": round(daily_realized_pnl, 2),
        "daily_unrealized_pnl": round(daily_unrealized_pnl, 2),
        "allocation_used_pct": round(allocation_used_pct, 2),
        "max_positions": max_positions,
        "max_total_allocated_capital": round(max_total_allocated_capital, 2),
        "max_capital_allocation_pct": max_capital_allocation_pct,
        "account_size": round(account_size, 2),
    }


def read_all_signal_rows() -> list[dict]:
    rows = get_signal_log_rows(limit=10000)
    normalized_rows: list[dict] = []

    for row in rows:
        normalized_rows.append({
            "timestamp_utc": row.get("timestamp_utc", ""),
            "scan_id": row.get("scan_id", ""),
            "scan_source": row.get("scan_source", ""),
            "market_phase": row.get("market_phase", ""),
            "scan_execution_time_ms": row.get("scan_execution_time_ms", ""),
            "mode": row.get("mode", ""),
            "account_size": row.get("account_size", ""),
            "current_open_positions": row.get("current_open_positions", ""),
            "current_open_exposure": row.get("current_open_exposure", ""),
            "timing_ok": row.get("timing_ok", ""),
            "source": row.get("source", ""),
            "trade_count": row.get("trade_count", ""),
            "top_name": row.get("top_name", ""),
            "top_symbol": row.get("top_symbol", ""),
            "current_price": row.get("current_price", ""),
            "entry": row.get("entry", ""),
            "stop": row.get("stop", ""),
            "target": row.get("target", ""),
            "shares": row.get("shares", ""),
            "confidence": row.get("confidence", ""),
            "reason": row.get("reason", ""),
            "benchmark_sp500": row.get("benchmark_sp500", ""),
            "benchmark_nasdaq": row.get("benchmark_nasdaq", ""),
            "paper_trade_enabled": row.get("paper_trade_enabled", ""),
            "paper_trade_candidate_count": row.get("paper_trade_candidate_count", ""),
            "paper_trade_long_candidate_count": row.get("paper_trade_long_candidate_count", ""),
            "paper_trade_short_candidate_count": row.get("paper_trade_short_candidate_count", ""),
            "paper_trade_placed_count": row.get("paper_trade_placed_count", ""),
            "paper_trade_placed_long_count": row.get("paper_trade_placed_long_count", ""),
            "paper_trade_placed_short_count": row.get("paper_trade_placed_short_count", ""),
            "paper_candidate_symbols": row.get("paper_candidate_symbols", ""),
            "paper_candidate_confidences": row.get("paper_candidate_confidences", ""),
            "paper_skipped_symbols": row.get("paper_skipped_symbols", ""),
            "paper_skip_reasons": row.get("paper_skip_reasons", ""),
            "paper_placed_symbols": row.get("paper_placed_symbols", ""),
            "paper_trade_ids": row.get("paper_trade_ids", ""),
        })

    return normalized_rows


def get_latest_open_paper_trade_for_symbol(symbol: str) -> dict | None:
    normalized_symbol = str(symbol).strip().upper()
    if not normalized_symbol:
        return None

    matching_rows = [row for row in get_open_paper_trades() if str(row.get("symbol", "")).strip().upper() == normalized_symbol]
    if not matching_rows:
        return None

    matching_rows.sort(key=lambda row: (str(row.get("timestamp_utc", "")).strip(), str(row.get("broker_parent_order_id", "")).strip()))
    return matching_rows[-1]


def get_latest_paper_close_event_for_symbol(symbol: str) -> dict | None:
    normalized_symbol = str(symbol).strip().upper()
    if not normalized_symbol:
        return None

    row = get_recent_closed_trade_lifecycle_for_symbol(normalized_symbol)
    if not row:
        return None

    return {
        "timestamp_utc": row.get("exit_time"),
        "symbol": row.get("symbol", ""),
        "status": row.get("status", ""),
        "exit_reason": row.get("exit_reason", ""),
        "order_id": row.get("order_id", ""),
        "parent_order_id": row.get("parent_order_id", ""),
    }


def is_symbol_in_paper_cooldown(symbol: str, now_utc: str) -> tuple[bool, str]:
    latest_close = get_latest_paper_close_event_for_symbol(symbol)
    if not latest_close:
        return False, ""

    exit_reason = str(latest_close.get("exit_reason", "")).strip().upper()
    cooldown_minutes = 0
    cooldown_label = ""
    if exit_reason == "STOP_HIT":
        cooldown_minutes = PAPER_STOP_COOLDOWN_MINUTES
        cooldown_label = "stop"
    elif exit_reason == "TARGET_HIT":
        cooldown_minutes = PAPER_TARGET_COOLDOWN_MINUTES
        cooldown_label = "target"
    elif exit_reason in {"MANUAL_CLOSE", "EOD_CLOSE"}:
        cooldown_minutes = PAPER_MANUAL_CLOSE_COOLDOWN_MINUTES
        cooldown_label = "manual_close"
    else:
        return False, ""

    if cooldown_minutes <= 0:
        return False, ""

    latest_ts = str(latest_close.get("timestamp_utc", "")).strip()
    if not latest_ts:
        return False, ""

    try:
        now_dt = parse_iso_utc(now_utc)
        latest_dt = parse_iso_utc(latest_ts)
    except Exception:
        return False, ""

    cooldown_until = latest_dt + timedelta(minutes=cooldown_minutes)
    if now_dt < cooldown_until:
        return True, f"{cooldown_label}_cooldown_until_{cooldown_until.isoformat()}"
    return False, ""


def find_best_signal_match(symbol: str, actual_entry_price: float | None, open_timestamp_utc: str) -> dict | None:
    rows = read_all_signal_rows()
    normalized_symbol = symbol.strip().upper()
    open_dt = parse_iso_utc(open_timestamp_utc)
    candidates = []

    for row in rows:
        row_symbol = str(row.get("top_symbol", "")).strip().upper()
        if row_symbol != normalized_symbol:
            continue

        row_ts = str(row.get("timestamp_utc", "")).strip()
        if not row_ts:
            continue

        try:
            row_dt = parse_iso_utc(row_ts)
        except Exception:
            continue

        if row_dt > open_dt:
            continue

        entry_val = to_float_or_none(row.get("entry", ""))
        price_diff = float("inf") if actual_entry_price is None or entry_val is None else abs(entry_val - actual_entry_price)
        time_diff_seconds = (open_dt - row_dt).total_seconds()
        candidates.append((price_diff, time_diff_seconds, row_dt, row))

    if not candidates:
        return None

    candidates.sort(key=lambda item: (item[0], item[1], -item[2].timestamp()))
    return candidates[0][3]


def find_latest_open_trade(symbol: str, trade_source: str | None = None, broker_parent_order_id: str | None = None) -> dict | None:
    normalized_symbol = str(symbol or "").strip().upper()
    normalized_trade_source = str(trade_source or "").strip().upper()
    normalized_parent_order_id = str(broker_parent_order_id or "").strip()

    if not normalized_symbol:
        return None

    if normalized_trade_source == "ALPACA_PAPER":
        open_rows = get_open_paper_trades()
        if normalized_parent_order_id:
            for row in reversed(open_rows):
                if str(row.get("broker_parent_order_id", "")).strip() == normalized_parent_order_id:
                    return row
        for row in reversed(open_rows):
            if str(row.get("symbol", "")).strip().upper() == normalized_symbol:
                return row
        return None

    row = get_latest_open_trade_lifecycle(normalized_symbol, parent_order_id=normalized_parent_order_id or None)
    if not row:
        return None

    return {
        "timestamp_utc": row.get("entry_time"),
        "symbol": row.get("symbol", ""),
        "mode": row.get("mode", ""),
        "shares": row.get("shares", ""),
        "entry_price": row.get("entry_price", ""),
        "stop_price": row.get("stop_price", ""),
        "target_price": row.get("target_price", ""),
        "status": row.get("status", ""),
        "broker_order_id": row.get("order_id", ""),
        "broker_parent_order_id": row.get("parent_order_id", ""),
        "linked_signal_timestamp_utc": row.get("signal_timestamp", ""),
        "linked_signal_entry": row.get("signal_entry", ""),
        "linked_signal_stop": row.get("signal_stop", ""),
        "linked_signal_target": row.get("signal_target", ""),
        "linked_signal_confidence": row.get("signal_confidence", ""),
    }


def fetch_candles_between(symbol: str, start_utc: str, end_utc: str, interval: str = "5min") -> list[dict]:
    if not TWELVEDATA_API_KEY:
        raise RuntimeError("Missing TWELVEDATA_API_KEY in environment.")

    start_dt_ny = parse_iso_utc(start_utc).astimezone(NY_TZ)
    end_dt_ny = parse_iso_utc(end_utc).astimezone(NY_TZ)
    elapsed_minutes = max(1, int((end_dt_ny - start_dt_ny).total_seconds() / 60))
    outputsize = min(5000, max(100, (elapsed_minutes // 5) + 20)) if interval == "5min" else min(5000, max(200, elapsed_minutes + 30))

    params = {
        "symbol": symbol,
        "interval": interval,
        "start_date": start_dt_ny.strftime("%Y-%m-%d %H:%M:%S"),
        "end_date": end_dt_ny.strftime("%Y-%m-%d %H:%M:%S"),
        "outputsize": outputsize,
        "apikey": TWELVEDATA_API_KEY,
        "format": "JSON",
        "order": "asc",
    }

    response = requests.get(TWELVEDATA_BASE_URL, params=params, timeout=20)
    response.raise_for_status()
    data = response.json()
    if data.get("status") == "error":
        raise ValueError(data.get("message", "Unknown Twelve Data error"))

    candles = []
    for row in data.get("values") or []:
        try:
            candles.append({
                "datetime": row["datetime"],
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
            })
        except Exception:
            continue

    return candles


def infer_first_level_hit(open_row: dict, close_timestamp_utc: str) -> dict:
    stop_raw = open_row.get("stop_price", "")
    target_raw = open_row.get("target_price", "")
    open_ts = open_row.get("timestamp_utc", "")
    symbol = str(open_row.get("symbol", "")).strip().upper()

    if not open_ts or stop_raw in ("", None) or target_raw in ("", None):
        return {
            "inferred_stop_hit": "",
            "inferred_target_hit": "",
            "inferred_first_level_hit": "",
            "inferred_analysis_start_utc": open_ts,
            "inferred_analysis_end_utc": close_timestamp_utc,
        }

    stop_price = float(stop_raw)
    target_price = float(target_raw)
    candles = fetch_candles_between(symbol, open_ts, close_timestamp_utc, interval="5min")

    stop_index = None
    target_index = None
    for index, candle in enumerate(candles):
        if stop_index is None and candle["low"] <= stop_price:
            stop_index = index
        if target_index is None and candle["high"] >= target_price:
            target_index = index

    inferred_stop_hit = "YES" if stop_index is not None else "NO"
    inferred_target_hit = "YES" if target_index is not None else "NO"

    if stop_index is None and target_index is None:
        first_hit = "NEITHER"
    elif stop_index is not None and target_index is None:
        first_hit = "STOP_FIRST"
    elif stop_index is None and target_index is not None:
        first_hit = "TARGET_FIRST"
    elif stop_index < target_index:
        first_hit = "STOP_FIRST"
    elif target_index < stop_index:
        first_hit = "TARGET_FIRST"
    else:
        first_hit = "BOTH_SAME_CANDLE"

    return {
        "inferred_stop_hit": inferred_stop_hit,
        "inferred_target_hit": inferred_target_hit,
        "inferred_first_level_hit": first_hit,
        "inferred_analysis_start_utc": open_ts,
        "inferred_analysis_end_utc": close_timestamp_utc,
    }
