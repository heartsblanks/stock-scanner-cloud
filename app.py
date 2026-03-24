import os
import io
import csv
from datetime import datetime, timezone

from flask import Flask, request, jsonify
from google.cloud import storage

from trade_scan import run_scan, market_time_check, MIN_CONFIDENCE

app = Flask(__name__)

LOG_BUCKET = os.getenv("LOG_BUCKET", "stock-scanner-490821-logs")
SIGNALS_CSV_PATH = os.getenv("SIGNALS_CSV_PATH", "signals/signals.csv")


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


def append_signal_log(row: dict) -> None:
    client = storage.Client()
    bucket = client.bucket(LOG_BUCKET)
    blob = bucket.blob(SIGNALS_CSV_PATH)

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


@app.get("/")
def home():
    return jsonify({
        "ok": True,
        "service": "stock-scanner",
        "endpoints": ["/scan"]
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)