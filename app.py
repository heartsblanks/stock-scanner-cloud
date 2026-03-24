import os
import io
import csv
from datetime import datetime, timezone

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

    existing = ""
    if blob.exists():
        existing = blob.download_as_text()

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers)

    if not existing.strip():
        writer.writeheader()
    else:
        output.write(existing)
        if not existing.endswith("\n"):
            output.write("\n")

    writer.writerow(row)
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
    ]
    append_csv_row(TRADES_CSV_PATH, headers, row)


@app.get("/")
def home():
    return jsonify({
        "ok": True,
        "service": "stock-scanner",
        "endpoints": ["/scan", "/log-trade"]
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

    # Simple shortcut-friendly input: one generic price field.
    # OPEN uses it as entry_price; other events use it as exit_price.
    price = payload.get("price", "")
    shares = payload.get("shares", "")
    stop_price = payload.get("stop_price", "")
    target_price = payload.get("target_price", "")

    if event_type == "OPEN":
        entry_price = price
        exit_price = ""
        exit_reason = ""
        status = "OPEN"
    elif event_type == "STOP_HIT":
        entry_price = ""
        exit_price = price
        exit_reason = "STOP_HIT"
        status = "OPEN"
    elif event_type == "TARGET_HIT":
        entry_price = ""
        exit_price = price
        exit_reason = "TARGET_HIT"
        status = "OPEN"
    else:  # MANUAL_CLOSE
        entry_price = ""
        exit_price = price
        exit_reason = "MANUAL_CLOSE"
        status = "CLOSED"

    timestamp_utc = datetime.now(timezone.utc).isoformat()

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
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)