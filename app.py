import os
from flask import Flask, request, jsonify
from trade_scan import run_scan, market_time_check, MIN_CONFIDENCE

app = Flask(__name__)


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
    if not ok:
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

    return jsonify(response)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
