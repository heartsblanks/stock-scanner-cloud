import os
import io
import csv
from datetime import datetime, timezone

import requests
from zoneinfo import ZoneInfo

from flask import Flask, request, jsonify
from google.cloud import storage
from paper_alpaca import place_paper_bracket_order_from_trade, get_open_positions, get_open_orders, close_position, cancel_open_orders_for_symbol
from alpaca_sync import sync_order_by_id, get_order_by_id

from trade_scan import (
    run_scan,
    market_time_check,
    MIN_CONFIDENCE,
    PRIMARY_INSTRUMENTS,
    SECONDARY_INSTRUMENTS,
    THIRD_INSTRUMENTS,
    FOURTH_INSTRUMENTS,
)
PAPER_TRADE_MIN_CONFIDENCE = 70

app = Flask(__name__)

INSTRUMENT_GROUPS = {
    "primary": PRIMARY_INSTRUMENTS,
    "secondary": SECONDARY_INSTRUMENTS,
    "third": THIRD_INSTRUMENTS,
    "fourth": FOURTH_INSTRUMENTS,
}


def find_instrument_by_symbol(symbol: str) -> tuple[str, str] | tuple[None, None]:
    symbol = symbol.strip().upper()
    for mode_name, instruments in INSTRUMENT_GROUPS.items():
        for display_name, info in instruments.items():
            if info.get("symbol", "").upper() == symbol:
                return display_name, mode_name
    return None, None

LOG_BUCKET = os.getenv("LOG_BUCKET", "stock-scanner-490821-logs")
SIGNALS_CSV_PATH = os.getenv("SIGNALS_CSV_PATH", "signals/signals.csv")
TRADES_CSV_PATH = os.getenv("TRADES_CSV_PATH", "trades/trades.csv")
TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY")
TWELVEDATA_BASE_URL = "https://api.twelvedata.com/time_series"
NY_TZ = ZoneInfo("America/New_York")


def trade_to_dict(eval_result: dict) -> dict:
    m = eval_result["metrics"]
    return {
        "name": eval_result["name"],
        "symbol": m["symbol"],
        "confidence": m["final_confidence"],
        "direction": m["direction"],
        "manual_eligible": m.get("manual_eligible", m["direction"] == "BUY"),
        "paper_eligible": m.get("paper_eligible", False),
        "current_price": round(m["price"], 4),
        "entry": round(m["entry"], 4),
        "stop": round(m["stop"], 4),
        "target": round(m["target"], 4),
        "shares": m["shares"],
        "position_cost": round(m["actual_position_cost"], 2),
        "risk_per_share": round(m["risk_per_share"], 4),
        "actual_risk": round(m["actual_risk"], 2),
        "max_allowed_risk": round(m["risk_amount"], 2),
        "or_high": round(m["or_high"], 4),
        "or_low": round(m["or_low"], 4),
        "vwap": round(m["vwap"], 4),
        "benchmark_key": m.get("benchmark_key"),
        "benchmark_direction": m.get("benchmark_direction"),
        "reason": eval_result["final_reason"],
    }



def debug_to_dict(eval_result: dict) -> dict:
    return {
        "name": eval_result["name"],
        "decision": eval_result["decision"],
        "final_reason": eval_result["final_reason"],
        "checks": eval_result.get("checks", {}),
        "metrics": eval_result.get("metrics", {}),
    }


# --- Helper function for paper trade candidate selection ---
def paper_candidate_from_evaluation(eval_result: dict) -> dict | None:
    metrics = eval_result.get("metrics") or {}
    direction = str(metrics.get("direction", "")).strip().upper()

    if direction not in {"BUY", "SELL"}:
        return None

    entry = to_float_or_none(metrics.get("entry"))
    price = to_float_or_none(metrics.get("price"))
    stop = to_float_or_none(metrics.get("stop"))
    target = to_float_or_none(metrics.get("target"))

    entry_value = entry if entry is not None else price
    if entry_value is None or stop is None or target is None:
        return None

    confidence = metrics.get("final_confidence")
    confidence_value = None
    try:
        confidence_value = float(confidence)
    except (TypeError, ValueError):
        priority = to_float_or_none(metrics.get("priority"))
        if priority is not None:
            confidence_value = float(priority) * 10.0

    if confidence_value is None or confidence_value < PAPER_TRADE_MIN_CONFIDENCE:
        return None

    shares = metrics.get("shares")
    if shares in (None, ""):
        shares = ""

    risk_per_share = to_float_or_none(metrics.get("risk_per_share"))
    if risk_per_share is None:
        risk_per_share = abs(entry_value - stop)

    share_count_for_calc = to_float_or_none(shares)
    if share_count_for_calc is None or share_count_for_calc <= 0:
        share_count_for_calc = 0.0

    actual_position_cost = to_float_or_none(metrics.get("actual_position_cost"))
    if actual_position_cost is None:
        actual_position_cost = entry_value * share_count_for_calc

    actual_risk = to_float_or_none(metrics.get("actual_risk"))
    if actual_risk is None:
        actual_risk = risk_per_share * share_count_for_calc

    risk_amount = to_float_or_none(metrics.get("risk_amount"))
    if risk_amount is None:
        risk_amount = actual_risk

    normalized_metrics = {
        **metrics,
        "entry": entry_value,
        "price": price if price is not None else entry_value,
        "stop": stop,
        "target": target,
        "shares": int(float(shares)) if shares not in (None, "") else "",
        "actual_position_cost": actual_position_cost,
        "risk_per_share": risk_per_share,
        "actual_risk": actual_risk,
        "risk_amount": risk_amount,
        "final_confidence": confidence_value,
        "manual_eligible": direction == "BUY" and str(eval_result.get("decision", "")).strip().upper() == "VALID",
        "paper_eligible": True,
    }

    return {
        "name": eval_result.get("name", ""),
        "final_reason": eval_result.get("final_reason", ""),
        "decision": "PAPER_CANDIDATE",
        "checks": eval_result.get("checks", {}),
        "metrics": normalized_metrics,
    }


def append_csv_row(path: str, headers: list[str], row: dict) -> None:
    client = storage.Client()
    bucket = client.bucket(LOG_BUCKET)
    blob = bucket.blob(path)

    existing_rows = []
    if blob.exists():
        content = blob.download_as_text()
        if content.strip():
            reader = csv.DictReader(io.StringIO(content))
            existing_rows = list(reader)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers)
    writer.writeheader()

    for existing_row in existing_rows:
        writer.writerow({h: existing_row.get(h, "") for h in headers})

    writer.writerow({h: row.get(h, "") for h in headers})
    blob.upload_from_string(output.getvalue(), content_type="text/csv")


def append_signal_log(row: dict) -> None:
    headers = [
        "timestamp_utc",
        "mode",
        "account_size",
        "timing_ok",
        "source",
        "trade_count",
        "top_name",
        "top_symbol",
        "current_price",
        "entry",
        "stop",
        "target",
        "shares",
        "confidence",
        "reason",
        "benchmark_sp500",
        "benchmark_nasdaq",
        "paper_trade_enabled",
        "paper_trade_candidate_count",
        "paper_trade_long_candidate_count",
        "paper_trade_short_candidate_count",
        "paper_trade_placed_count",
        "paper_trade_placed_long_count",
        "paper_trade_placed_short_count",
        "paper_candidate_symbols",
        "paper_placed_symbols",
    ]
    append_csv_row(SIGNALS_CSV_PATH, headers, row)


def append_trade_log(row: dict) -> None:
    headers = [
        "timestamp_utc",
        "event_type",
        "symbol",
        "name",
        "mode",
        "trade_source",
        "broker",
        "broker_order_id",
        "broker_parent_order_id",
        "broker_status",
        "broker_filled_qty",
        "broker_filled_avg_price",
        "broker_exit_order_id",
        "shares",
        "entry_price",
        "stop_price",
        "target_price",
        "exit_price",
        "exit_reason",
        "status",
        "notes",
        "linked_signal_timestamp_utc",
        "linked_signal_entry",
        "linked_signal_stop",
        "linked_signal_target",
        "linked_signal_confidence",
        "inferred_stop_hit",
        "inferred_target_hit",
        "inferred_first_level_hit",
        "inferred_analysis_start_utc",
        "inferred_analysis_end_utc",
    ]
    append_csv_row(TRADES_CSV_PATH, headers, row)

def read_trade_rows_for_date(target_date: str) -> list[dict]:
    client = storage.Client()
    bucket = client.bucket(LOG_BUCKET)
    blob = bucket.blob(TRADES_CSV_PATH)

    if not blob.exists():
        return []

    content = blob.download_as_text()
    if not content.strip():
        return []

    reader = csv.DictReader(io.StringIO(content))
    rows = []

    for row in reader:
        ts = str(row.get("timestamp_utc", ""))
        if ts.startswith(target_date):
            rows.append(row)

    return rows


def read_all_trade_rows() -> list[dict]:
    client = storage.Client()
    bucket = client.bucket(LOG_BUCKET)
    blob = bucket.blob(TRADES_CSV_PATH)

    if not blob.exists():
        return []

    content = blob.download_as_text()
    if not content.strip():
        return []

    reader = csv.DictReader(io.StringIO(content))
    return list(reader)


# --- Helper functions for paper trade syncing ---

def paper_trade_exit_already_logged(parent_order_id: str, exit_event: str) -> bool:
    parent_order_id = str(parent_order_id).strip()
    exit_event = str(exit_event).strip().upper()
    if not parent_order_id or not exit_event:
        return False

    rows = read_all_trade_rows()
    for row in rows:
        if str(row.get("trade_source", "")).strip().upper() != "ALPACA_PAPER":
            continue
        if str(row.get("broker_parent_order_id", "")).strip() != parent_order_id:
            continue
        if str(row.get("event_type", "")).strip().upper() == exit_event:
            return True
    return False


def get_open_paper_trades() -> list[dict]:
    rows = read_all_trade_rows()
    latest_by_parent_order_id: dict[str, dict] = {}

    for row in rows:
        if str(row.get("trade_source", "")).strip().upper() != "ALPACA_PAPER":
            continue

        parent_order_id = str(row.get("broker_parent_order_id", "")).strip()
        if not parent_order_id:
            continue

        latest_by_parent_order_id[parent_order_id] = row

    open_rows = []
    for row in latest_by_parent_order_id.values():
        if str(row.get("status", "")).strip().upper() == "OPEN":
            open_rows.append(row)

    try:
        positions = get_open_positions()
        open_orders = get_open_orders()
    except Exception as e:
        print(f"Broker validation for open paper trades failed: {e}", flush=True)
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

        if (
            symbol in open_position_symbols
            or parent_order_id in open_order_ids
            or broker_order_id in open_order_ids
        ):
            validated_open_rows.append(row)

    return validated_open_rows


def read_all_signal_rows() -> list[dict]:
    client = storage.Client()
    bucket = client.bucket(LOG_BUCKET)
    blob = bucket.blob(SIGNALS_CSV_PATH)

    if not blob.exists():
        return []

    content = blob.download_as_text()
    if not content.strip():
        return []

    reader = csv.DictReader(io.StringIO(content))
    return list(reader)


def parse_iso_utc(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def to_float_or_none(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def find_best_signal_match(symbol: str, actual_entry_price: float | None, open_timestamp_utc: str) -> dict | None:
    rows = read_all_signal_rows()
    symbol = symbol.strip().upper()
    open_dt = parse_iso_utc(open_timestamp_utc)

    candidates = []
    for row in rows:
        row_symbol = str(row.get("top_symbol", "")).strip().upper()
        if row_symbol != symbol:
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
        if actual_entry_price is None or entry_val is None:
            price_diff = float("inf")
        else:
            price_diff = abs(entry_val - actual_entry_price)

        time_diff_seconds = (open_dt - row_dt).total_seconds()
        candidates.append((price_diff, time_diff_seconds, row_dt, row))

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x[0], x[1], -x[2].timestamp()))
    return candidates[0][3]


def find_latest_open_trade(symbol: str, trade_source: str | None = None, broker_parent_order_id: str | None = None) -> dict | None:
    rows = read_all_trade_rows()
    current_open = None

    normalized_symbol = symbol.strip().upper()
    normalized_trade_source = str(trade_source or "").strip().upper()
    normalized_parent_order_id = str(broker_parent_order_id or "").strip()

    for row in rows:
        row_symbol = str(row.get("symbol", "")).strip().upper()
        if row_symbol != normalized_symbol:
            continue

        if normalized_trade_source:
            row_trade_source = str(row.get("trade_source", "")).strip().upper()
            if row_trade_source != normalized_trade_source:
                continue

        if normalized_parent_order_id:
            row_parent_order_id = str(row.get("broker_parent_order_id", "")).strip()
            if row_parent_order_id != normalized_parent_order_id:
                continue

        event_type = str(row.get("event_type", "")).strip().upper()
        status = str(row.get("status", "")).strip().upper()

        if event_type == "OPEN":
            current_open = row
        elif status == "CLOSED":
            current_open = None

    return current_open


def fetch_candles_between(symbol: str, start_utc: str, end_utc: str, interval: str = "5min") -> list[dict]:
    if not TWELVEDATA_API_KEY:
        raise RuntimeError("Missing TWELVEDATA_API_KEY in environment.")

    start_dt_ny = parse_iso_utc(start_utc).astimezone(NY_TZ)
    end_dt_ny = parse_iso_utc(end_utc).astimezone(NY_TZ)

    elapsed_minutes = max(1, int((end_dt_ny - start_dt_ny).total_seconds() / 60))
    if interval == "5min":
        outputsize = min(5000, max(100, (elapsed_minutes // 5) + 20))
    else:
        outputsize = min(5000, max(200, elapsed_minutes + 30))

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

    values = data.get("values") or []
    candles = []

    for row in values:
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

    for i, candle in enumerate(candles):
        if stop_index is None and candle["low"] <= stop_price:
            stop_index = i
        if target_index is None and candle["high"] >= target_price:
            target_index = i

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

@app.get("/")
def home():
    return jsonify({
        "ok": True,
        "service": "stock-scanner",
        "endpoints": ["/scan", "/log-trade", "/sync-paper-trades", "/close-paper-positions", "/read-trades-by-date"]
    })

# --- Sync paper trades endpoint ---

@app.post("/sync-paper-trades")
def sync_paper_trades():
    try:
        open_rows = get_open_paper_trades()
    except Exception as e:
        print(f"Open paper trade read failed: {e}", flush=True)
        return jsonify({"ok": False, "error": f"open paper trade read failed: {e}"}), 500

    results = []
    synced_count = 0
    skipped_count = 0

    for open_row in open_rows:
        parent_order_id = str(open_row.get("broker_parent_order_id", "")).strip()
        symbol = str(open_row.get("symbol", "")).strip().upper()

        if not parent_order_id:
            results.append({
                "symbol": symbol,
                "synced": False,
                "reason": "missing_parent_order_id",
            })
            skipped_count += 1
            continue

        try:
            sync_result = sync_order_by_id(parent_order_id)
        except Exception as e:
            print(f"Paper trade sync failed for {symbol} / {parent_order_id}: {e}", flush=True)
            results.append({
                "symbol": symbol,
                "parent_order_id": parent_order_id,
                "synced": False,
                "reason": "sync_exception",
                "details": str(e),
            })
            skipped_count += 1
            continue

        exit_event = str(sync_result.get("exit_event", "")).strip().upper()
        if not exit_event:
            results.append({
                "symbol": symbol,
                "parent_order_id": parent_order_id,
                "synced": False,
                "reason": "still_open",
                "parent_status": sync_result.get("parent_status", ""),
                "take_profit_status": sync_result.get("take_profit_status", ""),
                "stop_loss_status": sync_result.get("stop_loss_status", ""),
            })
            skipped_count += 1
            continue

        if paper_trade_exit_already_logged(parent_order_id, exit_event):
            results.append({
                "symbol": symbol,
                "parent_order_id": parent_order_id,
                "synced": False,
                "reason": "exit_already_logged",
                "exit_event": exit_event,
            })
            skipped_count += 1
            continue

        timestamp_utc = datetime.now(timezone.utc).isoformat()

        try:
            append_trade_log({
                "timestamp_utc": timestamp_utc,
                "event_type": exit_event,
                "symbol": symbol,
                "name": open_row.get("name", ""),
                "mode": open_row.get("mode", ""),
                "trade_source": "ALPACA_PAPER",
                "broker": "ALPACA",
                "broker_order_id": parent_order_id,
                "broker_parent_order_id": parent_order_id,
                "broker_status": sync_result.get("parent_status", ""),
                "broker_filled_qty": sync_result.get("entry_filled_qty", ""),
                "broker_filled_avg_price": sync_result.get("entry_filled_avg_price", ""),
                "broker_exit_order_id": sync_result.get("exit_order_id", ""),
                "shares": open_row.get("shares", ""),
                "entry_price": open_row.get("entry_price", ""),
                "stop_price": open_row.get("stop_price", ""),
                "target_price": open_row.get("target_price", ""),
                "exit_price": sync_result.get("exit_price", ""),
                "exit_reason": sync_result.get("exit_reason", exit_event),
                "status": "CLOSED",
                "notes": f"Paper trade exit synced from Alpaca. exit_event={exit_event}",
                "linked_signal_timestamp_utc": open_row.get("linked_signal_timestamp_utc", ""),
                "linked_signal_entry": open_row.get("linked_signal_entry", ""),
                "linked_signal_stop": open_row.get("linked_signal_stop", ""),
                "linked_signal_target": open_row.get("linked_signal_target", ""),
                "linked_signal_confidence": open_row.get("linked_signal_confidence", ""),
                "inferred_stop_hit": "",
                "inferred_target_hit": "",
                "inferred_first_level_hit": "",
                "inferred_analysis_start_utc": "",
                "inferred_analysis_end_utc": "",
            })
        except Exception as e:
            print(f"Paper trade exit log write failed for {symbol} / {parent_order_id}: {e}", flush=True)
            results.append({
                "symbol": symbol,
                "parent_order_id": parent_order_id,
                "synced": False,
                "reason": "log_write_failed",
                "details": str(e),
            })
            skipped_count += 1
            continue

        synced_count += 1
        results.append({
            "symbol": symbol,
            "parent_order_id": parent_order_id,
            "synced": True,
            "exit_event": exit_event,
            "exit_price": sync_result.get("exit_price", ""),
            "exit_order_id": sync_result.get("exit_order_id", ""),
            "parent_status": sync_result.get("parent_status", ""),
        })

    return jsonify({
        "ok": True,
        "open_paper_trade_count": len(open_rows),
        "synced_count": synced_count,
        "skipped_count": skipped_count,
        "results": results,
    })


@app.post("/scan")
def scan():
    payload = request.get_json(silent=True) or {}

    account_size = payload.get("account_size")
    mode = str(payload.get("mode", "primary")).lower()
    debug_raw = payload.get("debug", False)
    if isinstance(debug_raw, bool):
        debug = debug_raw
    elif isinstance(debug_raw, str):
        debug = debug_raw.strip().lower() in {"true", "1", "yes", "y", "on"}
    else:
        debug = bool(debug_raw)

    paper_trade_raw = payload.get("paper_trade", False)
    if isinstance(paper_trade_raw, bool):
        paper_trade = paper_trade_raw
    elif isinstance(paper_trade_raw, str):
        paper_trade = paper_trade_raw.strip().lower() in {"true", "1", "yes", "y", "on"}
    else:
        paper_trade = bool(paper_trade_raw)

    if account_size is None:
        return jsonify({"ok": False, "error": "account_size is required"}), 400

    try:
        account_size = float(account_size)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "account_size must be numeric"}), 400

    if mode not in {"primary", "secondary", "third", "fourth"}:
        return jsonify({"ok": False, "error": "mode must be primary, secondary, third, or fourth"}), 400

    ok, timing_msg = market_time_check()
    timestamp_utc = datetime.now(timezone.utc).isoformat()

    if not ok:
        try:
            append_signal_log({
                "timestamp_utc": timestamp_utc,
                "mode": mode,
                "account_size": account_size,
                "timing_ok": False,
                "source": mode.upper(),
                "trade_count": 0,
                "top_name": "",
                "top_symbol": "",
                "current_price": "",
                "entry": "",
                "stop": "",
                "target": "",
                "shares": "",
                "confidence": "",
                "reason": timing_msg.replace("\n", " "),
                "benchmark_sp500": "",
                "benchmark_nasdaq": "",
                "paper_trade_enabled": paper_trade,
                "paper_trade_candidate_count": 0,
                "paper_trade_long_candidate_count": 0,
                "paper_trade_short_candidate_count": 0,
                "paper_trade_placed_count": 0,
                "paper_trade_placed_long_count": 0,
                "paper_trade_placed_short_count": 0,
                "paper_candidate_symbols": "",
                "paper_placed_symbols": "",
            })
        except Exception as e:
            print(f"Signal log write failed: {e}", flush=True)

        return jsonify({
            "ok": True,
            "timing_ok": False,
            "message": timing_msg,
            "source": mode.upper(),
            "trades": [],
            "min_confidence": MIN_CONFIDENCE,
            "paper_trade_enabled": paper_trade,
        })

    all_trades, evaluations, _fetch_ok, _fetch_fail, benchmark_directions, source = run_scan(account_size, mode)
    trades = [t for t in all_trades if t["metrics"].get("direction") == "BUY"]
    paper_trades = []
    for ev in evaluations:
        candidate = paper_candidate_from_evaluation(ev)
        if candidate is not None:
            paper_trades.append(candidate)
    paper_long_candidates = [t for t in paper_trades if t["metrics"].get("direction") == "BUY"]
    paper_short_candidates = [t for t in paper_trades if t["metrics"].get("direction") == "SELL"]

    paper_candidate_symbols = ",".join(
        t["metrics"].get("symbol", "") for t in paper_trades if t["metrics"].get("symbol", "")
    )

    response = {
        "ok": True,
        "timing_ok": True,
        "message": timing_msg,
        "source": source,
        "benchmark_directions": benchmark_directions,
        "min_confidence": MIN_CONFIDENCE,
        "trade_count": len(trades),
        "paper_trade_candidate_count": len(paper_trades),
        "paper_trade_long_candidate_count": len(paper_long_candidates),
        "paper_trade_short_candidate_count": len(paper_short_candidates),
        "trades": [trade_to_dict(t) for t in trades],
        "paper_trade_enabled": paper_trade,
    }

    if debug:
        response["evaluations"] = [debug_to_dict(ev) for ev in evaluations]

    top_trade = trades[0] if trades else None

    if paper_trade:
        if not paper_trades:
            response["paper_trade_result"] = {
                "attempted": False,
                "placed": False,
                "reason": "no_paper_trade_candidates_at_or_above_threshold",
                "candidate_count": 0,
                "long_candidate_count": 0,
                "short_candidate_count": 0,
                "placed_long_count": 0,
                "placed_short_count": 0,
            }
        else:
            paper_results = []
            placed_count = 0
            placed_long_count = 0
            placed_short_count = 0

            for paper_trade_candidate in paper_trades:
                try:
                    paper_trade_result = place_paper_bracket_order_from_trade(paper_trade_candidate)
                    paper_results.append(paper_trade_result)

                    if paper_trade_result.get("placed"):
                        placed_count += 1
                        if paper_trade_candidate["metrics"].get("direction") == "BUY":
                            placed_long_count += 1
                        elif paper_trade_candidate["metrics"].get("direction") == "SELL":
                            placed_short_count += 1
                        paper_metrics = paper_trade_candidate["metrics"]
                        append_trade_log({
                            "timestamp_utc": timestamp_utc,
                            "event_type": "OPEN",
                            "symbol": paper_metrics.get("symbol", ""),
                            "name": paper_trade_candidate.get("name", ""),
                            "mode": mode,
                            "trade_source": "ALPACA_PAPER",
                            "broker": "ALPACA",
                            "broker_order_id": paper_trade_result.get("alpaca_order_id", ""),
                            "broker_parent_order_id": paper_trade_result.get("alpaca_order_id", ""),
                            "broker_status": paper_trade_result.get("alpaca_order_status", ""),
                            "broker_filled_qty": "",
                            "broker_filled_avg_price": "",
                            "broker_exit_order_id": "",
                            "shares": paper_trade_result.get("shares", ""),
                            "entry_price": paper_metrics.get("entry", ""),
                            "stop_price": paper_metrics.get("stop", ""),
                            "target_price": paper_metrics.get("target", ""),
                            "exit_price": "",
                            "exit_reason": "",
                            "status": "OPEN",
                            "notes": f"Paper {paper_metrics.get('direction', '')} bracket order submitted. client_order_id={paper_trade_result.get('client_order_id', '')}",
                            "linked_signal_timestamp_utc": timestamp_utc,
                            "linked_signal_entry": paper_metrics.get("entry", ""),
                            "linked_signal_stop": paper_metrics.get("stop", ""),
                            "linked_signal_target": paper_metrics.get("target", ""),
                            "linked_signal_confidence": paper_metrics.get("final_confidence", ""),
                            "inferred_stop_hit": "",
                            "inferred_target_hit": "",
                            "inferred_first_level_hit": "",
                            "inferred_analysis_start_utc": "",
                            "inferred_analysis_end_utc": "",
                        })
                except Exception as e:
                    print(f"Paper trade placement failed: {e}", flush=True)
                    paper_results.append({
                        "attempted": True,
                        "placed": False,
                        "reason": "paper_trade_exception",
                        "details": str(e),
                        "symbol": paper_trade_candidate.get("metrics", {}).get("symbol", ""),
                    })

            response["paper_trade_result"] = {
                "attempted": True,
                "placed": placed_count > 0,
                "candidate_count": len(paper_trades),
                "long_candidate_count": len(paper_long_candidates),
                "short_candidate_count": len(paper_short_candidates),
                "placed_count": placed_count,
                "placed_long_count": placed_long_count,
                "placed_short_count": placed_short_count,
                "results": paper_results,
            }

    paper_trade_result = response.get("paper_trade_result", {}) if paper_trade else {}
    paper_trade_placed_count = paper_trade_result.get("placed_count", 0) if isinstance(paper_trade_result, dict) else 0
    paper_trade_placed_long_count = paper_trade_result.get("placed_long_count", 0) if isinstance(paper_trade_result, dict) else 0
    paper_trade_placed_short_count = paper_trade_result.get("placed_short_count", 0) if isinstance(paper_trade_result, dict) else 0
    paper_placed_symbols = ""
    if isinstance(paper_trade_result, dict):
        paper_result_items = paper_trade_result.get("results", []) or []
        paper_placed_symbols = ",".join(
            str(item.get("symbol", "")).strip().upper()
            for item in paper_result_items
            if isinstance(item, dict) and item.get("placed") and str(item.get("symbol", "")).strip()
        )

    try:
        top_metrics = top_trade["metrics"] if top_trade else {}

        append_signal_log({
            "timestamp_utc": timestamp_utc,
            "mode": mode,
            "account_size": account_size,
            "timing_ok": True,
            "source": source,
            "trade_count": len(trades),
            "top_name": top_trade["name"] if top_trade else "",
            "top_symbol": top_metrics.get("symbol", ""),
            "current_price": top_metrics.get("price", ""),
            "entry": top_metrics.get("entry", ""),
            "stop": top_metrics.get("stop", ""),
            "target": top_metrics.get("target", ""),
            "shares": top_metrics.get("shares", ""),
            "confidence": top_metrics.get("final_confidence", ""),
            "reason": top_trade["final_reason"] if top_trade else "No trade today",
            "benchmark_sp500": benchmark_directions.get("SP500", ""),
            "benchmark_nasdaq": benchmark_directions.get("NASDAQ", ""),
            "paper_trade_enabled": paper_trade,
            "paper_trade_candidate_count": len(paper_trades),
            "paper_trade_long_candidate_count": len(paper_long_candidates),
            "paper_trade_short_candidate_count": len(paper_short_candidates),
            "paper_trade_placed_count": paper_trade_placed_count,
            "paper_trade_placed_long_count": paper_trade_placed_long_count,
            "paper_trade_placed_short_count": paper_trade_placed_short_count,
            "paper_candidate_symbols": paper_candidate_symbols,
            "paper_placed_symbols": paper_placed_symbols,
        })
    except Exception as e:
        print(f"Signal log write failed: {e}", flush=True)

    return jsonify(response)


@app.post("/log-trade")
def log_trade():
    payload = request.get_json(silent=True) or {}

    event_type = str(payload.get("event_type", "")).strip().upper()
    symbol = str(payload.get("symbol", "")).strip().upper()
    trade_source = str(payload.get("trade_source", "MANUAL")).strip().upper() or "MANUAL"
    broker = str(payload.get("broker", "")).strip().upper()
    broker_order_id = str(payload.get("broker_order_id", "")).strip()
    broker_parent_order_id = str(payload.get("broker_parent_order_id", "")).strip()
    broker_status = str(payload.get("broker_status", "")).strip().upper()
    broker_filled_qty = payload.get("broker_filled_qty", "")
    broker_filled_avg_price = payload.get("broker_filled_avg_price", "")
    broker_exit_order_id = str(payload.get("broker_exit_order_id", "")).strip()
    notes = str(payload.get("notes", "")).strip()

    if event_type not in {"OPEN", "STOP_HIT", "TARGET_HIT", "MANUAL_CLOSE"}:
        return jsonify({
            "ok": False,
            "error": "event_type must be OPEN, STOP_HIT, TARGET_HIT, or MANUAL_CLOSE"
        }), 400
    if trade_source not in {"MANUAL", "ALPACA_PAPER"}:
        return jsonify({
            "ok": False,
            "error": "trade_source must be MANUAL or ALPACA_PAPER"
        }), 400

    if not symbol:
        return jsonify({"ok": False, "error": "symbol is required"}), 400

    inferred_name, inferred_mode = find_instrument_by_symbol(symbol)
    if inferred_name is None or inferred_mode is None:
        return jsonify({"ok": False, "error": "symbol not found in configured watchlists"}), 400

    price = payload.get("price", "")
    shares = payload.get("shares", "")
    actual_entry_price = to_float_or_none(price)

    timestamp_utc = datetime.now(timezone.utc).isoformat()

    linked_signal_timestamp_utc = ""
    linked_signal_entry = ""
    linked_signal_stop = ""
    linked_signal_target = ""
    linked_signal_confidence = ""

    inference = {
        "inferred_stop_hit": "",
        "inferred_target_hit": "",
        "inferred_first_level_hit": "",
        "inferred_analysis_start_utc": "",
        "inferred_analysis_end_utc": "",
    }

    if event_type == "OPEN":
        matched_signal = find_best_signal_match(symbol, actual_entry_price, timestamp_utc)
        if matched_signal:
            linked_signal_timestamp_utc = matched_signal.get("timestamp_utc", "")
            linked_signal_entry = matched_signal.get("entry", "")
            linked_signal_stop = matched_signal.get("stop", "")
            linked_signal_target = matched_signal.get("target", "")
            linked_signal_confidence = matched_signal.get("confidence", "")

        entry_price = price
        stop_price = linked_signal_stop
        target_price = linked_signal_target
        exit_price = ""
        exit_reason = ""
        status = "OPEN"

    elif event_type == "STOP_HIT":
        open_row = find_latest_open_trade(symbol, trade_source=trade_source, broker_parent_order_id=broker_parent_order_id)
        if open_row:
            linked_signal_timestamp_utc = open_row.get("linked_signal_timestamp_utc", "")
            linked_signal_entry = open_row.get("linked_signal_entry", "")
            linked_signal_stop = open_row.get("linked_signal_stop", "")
            linked_signal_target = open_row.get("linked_signal_target", "")
            linked_signal_confidence = open_row.get("linked_signal_confidence", "")
            shares = shares or open_row.get("shares", "")

        entry_price = ""
        stop_price = linked_signal_stop
        target_price = linked_signal_target
        exit_price = price
        exit_reason = "STOP_HIT"
        status = "CLOSED"

    elif event_type == "TARGET_HIT":
        open_row = find_latest_open_trade(symbol, trade_source=trade_source, broker_parent_order_id=broker_parent_order_id)
        if open_row:
            linked_signal_timestamp_utc = open_row.get("linked_signal_timestamp_utc", "")
            linked_signal_entry = open_row.get("linked_signal_entry", "")
            linked_signal_stop = open_row.get("linked_signal_stop", "")
            linked_signal_target = open_row.get("linked_signal_target", "")
            linked_signal_confidence = open_row.get("linked_signal_confidence", "")
            shares = shares or open_row.get("shares", "")
            try:
                inference = infer_first_level_hit(open_row, timestamp_utc)
            except Exception as e:
                print(f"Inference failed for {symbol}: {e}", flush=True)

        entry_price = ""
        stop_price = linked_signal_stop
        target_price = linked_signal_target
        exit_price = price
        exit_reason = "TARGET_HIT"
        status = "CLOSED"

    else:  # MANUAL_CLOSE
        open_row = find_latest_open_trade(symbol, trade_source=trade_source, broker_parent_order_id=broker_parent_order_id)
        if open_row:
            linked_signal_timestamp_utc = open_row.get("linked_signal_timestamp_utc", "")
            linked_signal_entry = open_row.get("linked_signal_entry", "")
            linked_signal_stop = open_row.get("linked_signal_stop", "")
            linked_signal_target = open_row.get("linked_signal_target", "")
            linked_signal_confidence = open_row.get("linked_signal_confidence", "")
            shares = shares or open_row.get("shares", "")
            try:
                inference = infer_first_level_hit(open_row, timestamp_utc)
            except Exception as e:
                print(f"Inference failed for {symbol}: {e}", flush=True)

        entry_price = ""
        stop_price = linked_signal_stop
        target_price = linked_signal_target
        exit_price = price
        exit_reason = "MANUAL_CLOSE"
        status = "CLOSED"

    try:
        append_trade_log({
            "timestamp_utc": timestamp_utc,
            "event_type": event_type,
            "symbol": symbol,
            "name": inferred_name,
            "mode": inferred_mode,
            "trade_source": trade_source,
            "broker": broker,
            "broker_order_id": broker_order_id,
            "broker_parent_order_id": broker_parent_order_id,
            "broker_status": broker_status,
            "broker_filled_qty": broker_filled_qty,
            "broker_filled_avg_price": broker_filled_avg_price,
            "broker_exit_order_id": broker_exit_order_id,
            "shares": shares,
            "entry_price": entry_price,
            "stop_price": stop_price,
            "target_price": target_price,
            "exit_price": exit_price,
            "exit_reason": exit_reason,
            "status": status,
            "notes": notes,
            "linked_signal_timestamp_utc": linked_signal_timestamp_utc,
            "linked_signal_entry": linked_signal_entry,
            "linked_signal_stop": linked_signal_stop,
            "linked_signal_target": linked_signal_target,
            "linked_signal_confidence": linked_signal_confidence,
            "inferred_stop_hit": inference["inferred_stop_hit"],
            "inferred_target_hit": inference["inferred_target_hit"],
            "inferred_first_level_hit": inference["inferred_first_level_hit"],
            "inferred_analysis_start_utc": inference["inferred_analysis_start_utc"],
            "inferred_analysis_end_utc": inference["inferred_analysis_end_utc"],
        })
    except Exception as e:
        print(f"Trade log write failed: {e}", flush=True)
        return jsonify({"ok": False, "error": f"trade log write failed: {e}"}), 500

    return jsonify({
        "ok": True,
        "message": "Trade event logged",
        "event_type": event_type,
        "symbol": symbol,
        "name": inferred_name,
        "mode": inferred_mode,
        "status": status,
        "broker_context": {
            "trade_source": trade_source,
            "broker": broker,
            "broker_order_id": broker_order_id,
            "broker_parent_order_id": broker_parent_order_id,
            "broker_status": broker_status,
            "broker_filled_qty": broker_filled_qty,
            "broker_filled_avg_price": broker_filled_avg_price,
            "broker_exit_order_id": broker_exit_order_id,
        },
        "linked_signal": {
            "timestamp_utc": linked_signal_timestamp_utc,
            "entry": linked_signal_entry,
            "stop": linked_signal_stop,
            "target": linked_signal_target,
            "confidence": linked_signal_confidence,
        },
        "inference": inference,
    })

@app.post("/close-paper-positions")
def close_paper_positions():
    try:
        positions = get_open_positions()
    except Exception as e:
        print(f"Open position read failed: {e}", flush=True)
        return jsonify({"ok": False, "error": f"open position read failed: {e}"}), 500

    open_paper_rows = get_open_paper_trades()
    open_paper_symbols = {
        str(row.get("symbol", "")).strip().upper()
        for row in open_paper_rows
        if str(row.get("symbol", "")).strip()
    }

    results = []
    closed_count = 0
    skipped_count = 0

    for position in positions:
        symbol = str(position.get("symbol", "")).strip().upper()
        qty = str(position.get("qty", "")).strip()
        side = str(position.get("side", "")).strip().lower()
        current_price = position.get("current_price", "")

        if not symbol:
            skipped_count += 1
            results.append({
                "closed": False,
                "reason": "missing_symbol",
            })
            continue

        if symbol not in open_paper_symbols:
            skipped_count += 1
            results.append({
                "symbol": symbol,
                "closed": False,
                "reason": "not_managed_by_app",
            })
            continue

        try:
            canceled_order_ids = cancel_open_orders_for_symbol(symbol)
        except Exception as e:
            print(f"Paper open-order cancel failed for {symbol}: {e}", flush=True)
            skipped_count += 1
            results.append({
                "symbol": symbol,
                "closed": False,
                "reason": "cancel_open_orders_exception",
                "details": str(e),
            })
            continue

        try:
            close_response = close_position(symbol)
        except Exception as e:
            print(f"Paper position close failed for {symbol}: {e}", flush=True)
            skipped_count += 1
            results.append({
                "symbol": symbol,
                "closed": False,
                "reason": "close_exception",
                "details": str(e),
            })
            continue

        close_order_id = str(close_response.get("id", "")).strip()
        close_order_status = str(close_response.get("status", "")).strip()
        close_filled_avg_price = ""
        close_filled_qty = qty

        if close_order_id:
            try:
                close_order = get_order_by_id(close_order_id, nested=False)
                close_order_status = str(close_order.get("status", close_order_status)).strip()
                close_filled_qty = str(close_order.get("filled_qty", close_filled_qty)).strip()

                close_filled_avg_price_raw = close_order.get("filled_avg_price", "")
                if close_filled_avg_price_raw not in (None, ""):
                    close_filled_avg_price = str(close_filled_avg_price_raw).strip()
            except Exception as order_read_error:
                print(f"Paper close order read failed for {symbol}: {order_read_error}", flush=True)

        matching_open_row = next(
            (row for row in open_paper_rows if str(row.get("symbol", "")).strip().upper() == symbol),
            None,
        )

        timestamp_utc = datetime.now(timezone.utc).isoformat()

        try:
            append_trade_log({
                "timestamp_utc": timestamp_utc,
                "event_type": "MANUAL_CLOSE",
                "symbol": symbol,
                "name": (matching_open_row or {}).get("name", ""),
                "mode": (matching_open_row or {}).get("mode", ""),
                "trade_source": "ALPACA_PAPER",
                "broker": "ALPACA",
                "broker_order_id": close_order_id,
                "broker_parent_order_id": (matching_open_row or {}).get("broker_parent_order_id", ""),
                "broker_status": close_order_status,
                "broker_filled_qty": close_filled_qty,
                "broker_filled_avg_price": close_filled_avg_price,
                "broker_exit_order_id": close_order_id,
                "shares": (matching_open_row or {}).get("shares", qty),
                "entry_price": (matching_open_row or {}).get("entry_price", ""),
                "stop_price": (matching_open_row or {}).get("stop_price", ""),
                "target_price": (matching_open_row or {}).get("target_price", ""),
                "exit_price": close_filled_avg_price if close_filled_avg_price else current_price,
                "exit_reason": "EOD_CLOSE",
                "status": "CLOSED",
                "notes": f"Paper position closed at end of day. side={side}; canceled_orders={len(canceled_order_ids)}",
                "linked_signal_timestamp_utc": (matching_open_row or {}).get("linked_signal_timestamp_utc", ""),
                "linked_signal_entry": (matching_open_row or {}).get("linked_signal_entry", ""),
                "linked_signal_stop": (matching_open_row or {}).get("linked_signal_stop", ""),
                "linked_signal_target": (matching_open_row or {}).get("linked_signal_target", ""),
                "linked_signal_confidence": (matching_open_row or {}).get("linked_signal_confidence", ""),
                "inferred_stop_hit": "",
                "inferred_target_hit": "",
                "inferred_first_level_hit": "",
                "inferred_analysis_start_utc": "",
                "inferred_analysis_end_utc": "",
            })
        except Exception as e:
            print(f"Paper EOD close log write failed for {symbol}: {e}", flush=True)
            skipped_count += 1
            results.append({
                "symbol": symbol,
                "closed": False,
                "reason": "log_write_failed",
                "details": str(e),
            })
            continue

        closed_count += 1
        results.append({
            "symbol": symbol,
            "closed": True,
            "qty": qty,
            "side": side,
            "exit_price": close_filled_avg_price if close_filled_avg_price else current_price,
            "close_order_id": close_order_id,
            "close_status": close_order_status,
            "close_filled_qty": close_filled_qty,
            "close_filled_avg_price": close_filled_avg_price,
            "canceled_order_count": len(canceled_order_ids),
        })

    return jsonify({
        "ok": True,
        "position_count": len(positions),
        "closed_count": closed_count,
        "skipped_count": skipped_count,
        "results": results,
    })

@app.post("/read-trades-by-date")
def read_trades_by_date():
    payload = request.get_json(silent=True) or {}
    target_date = str(payload.get("date", "")).strip()

    if not target_date:
        return jsonify({"ok": False, "error": "date is required in YYYY-MM-DD format"}), 400

    try:
        rows = read_trade_rows_for_date(target_date)
    except Exception as e:
        print(f"Trade log read failed: {e}", flush=True)
        return jsonify({"ok": False, "error": f"trade log read failed: {e}"}), 500

    formatted_lines = [f"Trade Log for {target_date}"]

    if not rows:
        formatted_lines.append("")
        formatted_lines.append("No trade events found.")
    else:
        for row in rows:
            ts = row.get("timestamp_utc", "")
            time_part = ts[11:16] if len(ts) >= 16 else ts
            formatted_lines.append(
                f"{time_part} UTC | "
                f"{row.get('event_type', '')} | "
                f"{row.get('symbol', '')} | "
                f"{row.get('mode', '')} | "
                f"{row.get('trade_source', 'MANUAL')} | "
                f"shares {row.get('shares', '')} | "
                f"entry {row.get('entry_price', '')} | "
                f"exit {row.get('exit_price', '')} | "
                f"{row.get('notes', '')}"
            )

    return jsonify({
        "ok": True,
        "date": target_date,
        "count": len(rows),
        "rows": rows,
        "formatted_text": "\n".join(formatted_lines),
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)