import os
import io
import csv
from datetime import datetime, timezone

import requests
from zoneinfo import ZoneInfo

from flask import Flask, request, jsonify
from google.cloud import storage

from trade_scan import (
    run_scan,
    market_time_check,
    MIN_CONFIDENCE,
    PRIMARY_INSTRUMENTS,
    SECONDARY_INSTRUMENTS,
    THIRD_INSTRUMENTS,
    FOURTH_INSTRUMENTS,
)

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
    ]
    append_csv_row(SIGNALS_CSV_PATH, headers, row)


def append_trade_log(row: dict) -> None:
    headers = [
        "timestamp_utc",
        "event_type",
        "symbol",
        "name",
        "mode",
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


def find_latest_open_trade(symbol: str) -> dict | None:
    rows = read_all_trade_rows()
    current_open = None

    for row in rows:
        if str(row.get("symbol", "")).strip().upper() != symbol.strip().upper():
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
        "endpoints": ["/scan", "/log-trade", "/read-trades-by-date"]
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
        })

    trades, evaluations, _fetch_ok, _fetch_fail, benchmark_directions, source = run_scan(account_size, mode)

    response = {
        "ok": True,
        "timing_ok": True,
        "message": timing_msg,
        "source": source,
        "benchmark_directions": benchmark_directions,
        "min_confidence": MIN_CONFIDENCE,
        "trade_count": len(trades),
        "trades": [trade_to_dict(t) for t in trades],
    }

    if debug:
        response["evaluations"] = [debug_to_dict(ev) for ev in evaluations]

    try:
        top_trade = trades[0] if trades else None
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
        })
    except Exception as e:
        print(f"Signal log write failed: {e}", flush=True)

    return jsonify(response)


@app.post("/log-trade")
def log_trade():
    payload = request.get_json(silent=True) or {}

    event_type = str(payload.get("event_type", "")).strip().upper()
    symbol = str(payload.get("symbol", "")).strip().upper()
    notes = str(payload.get("notes", "")).strip()

    if event_type not in {"OPEN", "STOP_HIT", "TARGET_HIT", "MANUAL_CLOSE"}:
        return jsonify({
            "ok": False,
            "error": "event_type must be OPEN, STOP_HIT, TARGET_HIT, or MANUAL_CLOSE"
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
        open_row = find_latest_open_trade(symbol)
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
        status = "OPEN"

    elif event_type == "TARGET_HIT":
        open_row = find_latest_open_trade(symbol)
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
        status = "OPEN"

    else:  # MANUAL_CLOSE
        open_row = find_latest_open_trade(symbol)
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
        "linked_signal": {
            "timestamp_utc": linked_signal_timestamp_utc,
            "entry": linked_signal_entry,
            "stop": linked_signal_stop,
            "target": linked_signal_target,
            "confidence": linked_signal_confidence,
        },
        "inference": inference,
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