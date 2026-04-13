from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo


NY_TZ = ZoneInfo("America/New_York")
ALPACA_SCHEDULED_MODE_ORDER = [
    "core_one",
    "core_two",
    "core_three",
    "primary",
    "secondary",
    "sixth",
    "third",
    "fourth",
    "fifth",
]

IBKR_SCHEDULED_MODE_ORDER = [
    "primary",
    "secondary",
    "third",
    "fourth",
    "fifth",
    "sixth",
    "core_one",
    "core_two",
    "core_three",
]


def scheduled_round_robin_mode(now_ny: datetime | None = None, *, mode_order: list[str] | None = None) -> str | None:
    now_ny = now_ny or datetime.now(NY_TZ)
    effective_mode_order = mode_order or ALPACA_SCHEDULED_MODE_ORDER
    total_minutes = (now_ny.hour * 60) + now_ny.minute
    first_scan_minute = (9 * 60) + 50
    last_scan_minute = (15 * 60) + 50

    if total_minutes < first_scan_minute or total_minutes > last_scan_minute:
        return None

    slot_index = ((total_minutes - first_scan_minute) // 10) % len(effective_mode_order)
    return effective_mode_order[slot_index]


def build_scheduled_scan_payload(
    payload: dict[str, Any],
    now_ny: datetime | None = None,
    *,
    mode_order: list[str] | None = None,
) -> dict[str, Any]:
    now_ny = now_ny or datetime.now(NY_TZ)
    effective_mode_order = mode_order or ALPACA_SCHEDULED_MODE_ORDER
    scheduled_mode = scheduled_round_robin_mode(now_ny, mode_order=effective_mode_order)
    if scheduled_mode is None:
        raise ValueError("outside scheduled paper scan window")

    return {
        "mode": scheduled_mode,
        "paper_trade": True,
        "debug": payload.get("debug", False),
        "scan_source": "SCHEDULED",
        "scheduled_mode_order": effective_mode_order,
    }


def parse_iso_utc(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def to_float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_scan_id(timestamp_utc: str, mode: str) -> str:
    safe_ts = str(timestamp_utc).replace(":", "-")
    return f"{safe_ts}_{mode}"


def market_phase_from_timestamp(timestamp_utc: str) -> str:
    try:
        dt_ny = parse_iso_utc(timestamp_utc).astimezone(NY_TZ)
    except Exception:
        return "UNKNOWN"

    minutes = (dt_ny.hour * 60) + dt_ny.minute
    open_minute = (9 * 60) + 30
    if minutes < open_minute:
        return "PREMARKET"
    if minutes < open_minute + 30:
        return "OPENING"
    if minutes < (12 * 60):
        return "MORNING"
    if minutes < (14 * 60):
        return "MIDDAY"
    if minutes < (15 * 60) + 30:
        return "AFTERNOON"
    return "POWER_HOUR"


def trade_to_dict(eval_result: dict[str, Any]) -> dict[str, Any]:
    metrics = eval_result["metrics"]
    return {
        "name": eval_result["name"],
        "symbol": metrics["symbol"],
        "confidence": metrics["final_confidence"],
        "direction": metrics["direction"],
        "manual_eligible": metrics.get("manual_eligible", metrics["direction"] == "BUY"),
        "paper_eligible": metrics.get("paper_eligible", False),
        "current_price": round(metrics["price"], 4),
        "entry": round(metrics["entry"], 4),
        "stop": round(metrics["stop"], 4),
        "target": round(metrics["target"], 4),
        "shares": metrics["shares"],
        "position_cost": round(metrics["actual_position_cost"], 2),
        "per_trade_notional": round(float(metrics.get("per_trade_notional", 0) or 0), 2),
        "remaining_slots": int(float(metrics.get("remaining_slots", 0) or 0)),
        "remaining_allocatable_capital": round(float(metrics.get("remaining_allocatable_capital", 0) or 0), 2),
        "risk_per_share": round(metrics["risk_per_share"], 4),
        "actual_risk": round(metrics["actual_risk"], 2),
        "take_profit_dollars": round(float(metrics.get("take_profit_dollars", 0) or 0), 2),
        "or_high": round(metrics["or_high"], 4),
        "or_low": round(metrics["or_low"], 4),
        "vwap": round(metrics["vwap"], 4),
        "benchmark_key": metrics.get("benchmark_key"),
        "benchmark_direction": metrics.get("benchmark_direction"),
        "current_open_positions": int(float(metrics.get("current_open_positions", 0) or 0)),
        "current_open_exposure": round(float(metrics.get("current_open_exposure", 0) or 0), 2),
        "max_total_allocated_capital": round(float(metrics.get("max_total_allocated_capital", 0) or 0), 2),
        "max_capital_allocation_pct": round(float(metrics.get("max_capital_allocation_pct", 0) or 0), 4),
        "reason": eval_result["final_reason"],
    }


def debug_to_dict(eval_result: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": eval_result["name"],
        "decision": eval_result["decision"],
        "final_reason": eval_result["final_reason"],
        "checks": eval_result.get("checks", {}),
        "metrics": eval_result.get("metrics", {}),
    }


def _long_market_regime_allows_entry(eval_result: dict[str, Any], direction: str) -> bool:
    if direction != "BUY":
        return True

    benchmark_directions = eval_result.get("benchmark_directions") or {}
    required_benchmarks = ("SPY", "QQQ")
    observed_directions = [
        str(benchmark_directions.get(symbol, "")).strip().upper()
        for symbol in required_benchmarks
        if str(benchmark_directions.get(symbol, "")).strip()
    ]

    if not observed_directions:
        return True

    return all(signal_direction == "BUY" for signal_direction in observed_directions)


def paper_candidate_from_evaluation(eval_result: dict[str, Any], paper_trade_min_confidence: float) -> dict[str, Any] | None:
    decision = str(eval_result.get("decision", "")).strip().upper()
    if decision != "VALID":
        return None

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
    try:
        confidence_value = float(confidence)
    except (TypeError, ValueError):
        confidence_value = None

    if confidence_value is None or confidence_value < paper_trade_min_confidence:
        eval_result["final_reason"] = "below_paper_trade_confidence_threshold"
        metrics["paper_eligible"] = False
        return None

    if not _long_market_regime_allows_entry(eval_result, direction):
        eval_result["final_reason"] = "long_market_regime_blocked"
        metrics["paper_eligible"] = False
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
        "manual_eligible": direction == "BUY",
        "paper_eligible": True,
    }

    return {
        "name": eval_result.get("name", ""),
        "final_reason": eval_result.get("final_reason", ""),
        "decision": "PAPER_CANDIDATE",
        "checks": eval_result.get("checks", {}),
        "metrics": normalized_metrics,
        "info": eval_result.get("info"),
        "candles": eval_result.get("candles"),
        "benchmark_directions": eval_result.get("benchmark_directions"),
    }
