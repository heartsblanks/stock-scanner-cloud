from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable
import os
from core.logging_utils import log_exception, log_info, log_warning
from services.alert_service import send_telegram_alert, telegram_alerts_enabled

# NOTE: requires implementation
# def get_recent_closed_trades_for_symbol(symbol: str, limit: int = 5) -> list[dict]


# --- Helper: fetch recent closed trades for a symbol (to be replaced with DB-backed implementation) ---
def get_recent_closed_trades_for_symbol(symbol: str, limit: int = 5) -> list[dict]:
    """
    Fetch recent CLOSED trades for a symbol from trade_lifecycles.
    Expected to be replaced with DB-backed implementation.
    """
    try:
        # Lazy import to avoid circular deps
        from core.db import fetch_recent_closed_trades_for_symbol  # type: ignore
        return fetch_recent_closed_trades_for_symbol(symbol=symbol, limit=limit) or []
    except Exception:
        return []


def _normalize_trade_key(symbol: str, broker_parent_order_id: str, broker_order_id: str, broker: str | None = None) -> str:
    normalized_broker = str(broker or "").strip().upper()
    base_key = broker_parent_order_id or broker_order_id or symbol
    if normalized_broker == "IBKR":
        normalized_symbol = str(symbol or "").strip().upper()
        if normalized_symbol and base_key:
            return f"{normalized_broker}:{normalized_symbol}:{base_key}"
    return base_key


def _to_upper_or_none(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    return text or None


def _infer_direction(entry_price, stop_price, target_price, side) -> str | None:
    side_text = str(side or "").strip().upper()
    if side_text == "BUY":
        return "LONG"
    if side_text == "SELL":
        return "SHORT"

    try:
        entry_val = float(entry_price) if entry_price not in (None, "") else None
    except Exception:
        entry_val = None
    try:
        stop_val = float(stop_price) if stop_price not in (None, "") else None
    except Exception:
        stop_val = None
    try:
        target_val = float(target_price) if target_price not in (None, "") else None
    except Exception:
        target_val = None

    if entry_val is not None and target_val is not None and stop_val is not None:
        if target_val > entry_val and stop_val < entry_val:
            return "LONG"
        if target_val < entry_val and stop_val > entry_val:
            return "SHORT"

    return None

def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


PAPER_CONSECUTIVE_LOSS_COOLDOWN_THRESHOLD = _to_int(os.getenv("PAPER_CONSECUTIVE_LOSS_COOLDOWN_THRESHOLD", 2), 2)
PAPER_CONSECUTIVE_LOSS_COOLDOWN_MINUTES = _to_int(os.getenv("PAPER_CONSECUTIVE_LOSS_COOLDOWN_MINUTES", 60), 60)
PAPER_SYMBOL_GATING_ENABLED = str(os.getenv("PAPER_SYMBOL_GATING_ENABLED", "true")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
PAPER_SYMBOL_GATING_LOOKBACK = _to_int(os.getenv("PAPER_SYMBOL_GATING_LOOKBACK", 5), 5)
PAPER_SYMBOL_GATING_MIN_TRADES = _to_int(os.getenv("PAPER_SYMBOL_GATING_MIN_TRADES", 3), 3)
PAPER_SYMBOL_GATING_MAX_AVG_PNL_PCT = _to_float(os.getenv("PAPER_SYMBOL_GATING_MAX_AVG_PNL_PCT", -0.5), -0.5)
LOW_PRICE_NOTIONAL_CAP_ENABLED = str(os.getenv("LOW_PRICE_NOTIONAL_CAP_ENABLED", "true")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
LOW_PRICE_THRESHOLD = _to_float(os.getenv("LOW_PRICE_THRESHOLD", 20.0), 20.0)
LOW_PRICE_MAX_NOTIONAL = _to_float(os.getenv("LOW_PRICE_MAX_NOTIONAL", 5000.0), 5000.0)


def _paper_trade_broker_name(paper_trade_result: dict[str, Any]) -> str:
    broker = str(paper_trade_result.get("broker", "") or "").strip().upper()
    return broker or "IBKR"


def _paper_trade_source(broker: str) -> str:
    normalized = str(broker or "").strip().upper() or "IBKR"
    return f"{normalized}_PAPER"


def _paper_trade_order_id(paper_trade_result: dict[str, Any]) -> str:
    return str(
        paper_trade_result.get("broker_order_id")
        or paper_trade_result.get("order_id")
        or ""
    ).strip()


def _paper_trade_parent_order_id(paper_trade_result: dict[str, Any]) -> str:
    return str(
        paper_trade_result.get("broker_parent_order_id")
        or paper_trade_result.get("parent_order_id")
        or _paper_trade_order_id(paper_trade_result)
        or ""
    ).strip()


def _paper_trade_order_status(paper_trade_result: dict[str, Any]) -> str:
    return str(
        paper_trade_result.get("broker_order_status")
        or paper_trade_result.get("order_status")
        or ""
    ).strip()


def evaluate_symbol_performance_gate(recent_trades: list[dict[str, Any]]) -> tuple[bool, str, dict[str, float | int]]:
    normalized_trades = [trade for trade in (recent_trades or []) if isinstance(trade, dict)]
    trade_count = len(normalized_trades)
    diagnostics: dict[str, float | int] = {
        "trade_count": trade_count,
        "loss_count": 0,
        "win_count": 0,
        "avg_pnl_pct": 0.0,
    }

    if not PAPER_SYMBOL_GATING_ENABLED or trade_count < PAPER_SYMBOL_GATING_MIN_TRADES:
        return False, "", diagnostics

    pnl_pcts = []
    loss_count = 0
    win_count = 0
    for trade in normalized_trades[:PAPER_SYMBOL_GATING_LOOKBACK]:
        pnl_pct = _to_float(trade.get("realized_pnl_percent"), 0.0)
        pnl_pcts.append(pnl_pct)
        if pnl_pct < 0:
            loss_count += 1
        elif pnl_pct > 0:
            win_count += 1

    effective_trade_count = len(pnl_pcts)
    avg_pnl_pct = (sum(pnl_pcts) / effective_trade_count) if effective_trade_count else 0.0
    diagnostics.update({
        "trade_count": effective_trade_count,
        "loss_count": loss_count,
        "win_count": win_count,
        "avg_pnl_pct": round(avg_pnl_pct, 4),
    })

    should_block = (
        effective_trade_count >= PAPER_SYMBOL_GATING_MIN_TRADES
        and loss_count == effective_trade_count
        and avg_pnl_pct <= PAPER_SYMBOL_GATING_MAX_AVG_PNL_PCT
    )
    if not should_block:
        return False, "", diagnostics

    reason = f"symbol_performance_blocked_{effective_trade_count}t_{avg_pnl_pct:.2f}pct"
    return True, reason, diagnostics


def _normalize_paper_trade_results(raw_result: Any) -> list[dict[str, Any]]:
    if isinstance(raw_result, list):
        return [item for item in raw_result if isinstance(item, dict)]
    if isinstance(raw_result, dict):
        return [raw_result]
    return []


def _get_live_ibkr_account_equity(payload: dict[str, Any]) -> float:
    """
    Resolve sizing equity from IBKR first, with a safe fallback chain:
    1. live IBKR account equity
    2. request payload account_size
    3. env fallback for emergency/manual use

    This keeps scan sizing aligned with the actual broker account while still
    allowing the service to run if IBKR is temporarily unavailable.
    """
    try:
        account_getters: list[Callable[[], Any]] = []

        try:
            from orchestration.runtime_context import IBKR_PAPER_BROKER

            get_account = getattr(IBKR_PAPER_BROKER, "get_account", None)
            if callable(get_account):
                account_getters.append(get_account)
        except Exception:
            pass

        for getter in account_getters:
            try:
                account = getter()
                if account is None:
                    continue

                equity_value = None
                if isinstance(account, dict):
                    equity_value = account.get("equity")
                else:
                    equity_value = getattr(account, "equity", None)
                    if equity_value is None and hasattr(account, "get"):
                        try:
                            equity_value = account.get("equity")
                        except Exception:
                            equity_value = None

                equity = _to_float(equity_value, 0.0)
                if equity > 0:
                    return equity
            except Exception:
                continue
    except Exception:
        pass

    payload_account_size = _to_float(payload.get("account_size"), 0.0)
    if payload_account_size > 0:
        return payload_account_size

    env_account_size = _to_float(os.getenv("SCHEDULED_PAPER_ACCOUNT_SIZE"), 0.0)
    if env_account_size > 0:
        return env_account_size

    raise ValueError("Unable to resolve account equity from IBKR, payload, or environment")


HIGH_QUALITY_LONG_SIGNAL_ALERT_MIN_CONFIDENCE = _to_int(
    os.getenv("HIGH_QUALITY_LONG_SIGNAL_ALERT_MIN_CONFIDENCE", "92"),
    92,
)


def _maybe_send_high_quality_long_signal_alert(
    *,
    scan_id: str,
    scan_source: str,
    mode: str,
    top_trade: dict[str, Any] | None,
    trades: list[dict[str, Any]],
    paper_trade: bool,
    current_open_positions: int,
    current_open_exposure: float,
    source: str,
    benchmark_directions: dict[str, Any],
) -> dict[str, Any] | None:
    if not telegram_alerts_enabled():
        return None
    if not trades:
        return None

    long_trades = [trade for trade in trades if str(trade.get("metrics", {}).get("direction", "")).strip().upper() == "BUY"]
    if not long_trades:
        return None

    best_long_trade = max(
        long_trades,
        key=lambda trade: _to_float((trade.get("metrics") or {}).get("final_confidence"), 0.0),
    )
    best_metrics = best_long_trade.get("metrics") or {}
    confidence = _to_float(best_metrics.get("final_confidence"), 0.0)
    if confidence < HIGH_QUALITY_LONG_SIGNAL_ALERT_MIN_CONFIDENCE:
        return None

    symbol = str(best_metrics.get("symbol", "")).strip().upper()
    alert_result = send_telegram_alert(
        alert_key=f"high-quality-long:{scan_id}:{symbol}",
        message=(
            f"High-quality long signal detected: {symbol}\n"
            f"Confidence: {confidence:.0f}\n"
            f"Entry: {best_metrics.get('entry', '')} | Stop: {best_metrics.get('stop', '')} | Target: {best_metrics.get('target', '')}\n"
            f"Mode: {mode} | Source: {scan_source} | Broker: IBKR"
        ),
        payload={
            "scan_id": scan_id,
            "symbol": symbol,
            "mode": mode,
            "source": source,
            "scan_source": scan_source,
            "confidence": confidence,
            "entry": best_metrics.get("entry", ""),
            "stop": best_metrics.get("stop", ""),
            "target": best_metrics.get("target", ""),
            "paper_trade": paper_trade,
            "current_open_positions": current_open_positions,
            "current_open_exposure": current_open_exposure,
            "benchmark_sp500": (benchmark_directions or {}).get("SP500", ""),
            "benchmark_nasdaq": (benchmark_directions or {}).get("NASDAQ", ""),
        },
    )
    log_info(
        "High-quality long signal alert evaluated",
        component="scan_service",
        operation="execute_full_scan",
        scan_id=scan_id,
        symbol=symbol,
        confidence=confidence,
        alert_sent=bool(alert_result.get("sent")),
        alert_reason=alert_result.get("reason"),
    )
    return alert_result


# --- Patch: Minimum viable position sizing for high-priced symbols ---
def _apply_minimum_viable_position_sizing(metrics: dict[str, Any]) -> None:
    """
    Ensure position sizing remains usable when equal slot allocation would otherwise
    produce zero shares for high-priced symbols.

    Strategy:
    - keep the existing remaining capital ceiling
    - compress the effective slot count based on what can actually buy at least 1 share
    - recompute per-trade notional and shares from that compressed slot count
    """
    entry_price = _to_float(metrics.get("entry"), 0.0)
    remaining_allocatable_capital = _to_float(metrics.get("remaining_allocatable_capital"), 0.0)
    remaining_slots = _to_int(metrics.get("remaining_slots"), 0)
    current_shares = _to_int(metrics.get("shares"), 0)

    if entry_price <= 0 or remaining_allocatable_capital <= 0 or remaining_slots <= 0:
        return

    # If current sizing is already viable, do nothing.
    if current_shares > 0:
        return

    # Cannot buy even 1 share with all remaining capital.
    if remaining_allocatable_capital < entry_price:
        return

    # Compress slot count so that each slot can fund at least one share.
    affordable_single_share_slots = max(1, int(remaining_allocatable_capital // entry_price))
    effective_slots = max(1, min(remaining_slots, affordable_single_share_slots))

    adjusted_notional = remaining_allocatable_capital / effective_slots
    adjusted_shares = int(adjusted_notional / entry_price)

    if adjusted_shares <= 0:
        adjusted_shares = 1
        adjusted_notional = entry_price

    actual_position_cost = adjusted_shares * entry_price
    if actual_position_cost > remaining_allocatable_capital:
        return

    metrics["effective_remaining_slots"] = effective_slots
    metrics["per_trade_notional"] = round(adjusted_notional, 4)
    metrics["shares"] = adjusted_shares
    metrics["notional_capped_shares"] = adjusted_shares
    metrics["cash_affordable_shares"] = max(_to_int(metrics.get("cash_affordable_shares"), 0), adjusted_shares)
    metrics["actual_position_cost"] = round(actual_position_cost, 4)
    metrics["actual_risk"] = round(adjusted_shares * _to_float(metrics.get("risk_per_share"), 0.0), 4)


def _apply_confidence_loss_sizing(
    metrics: dict[str, Any],
    *,
    confidence_multiplier: float,
    loss_multiplier: float,
    final_multiplier: float,
) -> None:
    if final_multiplier == 1.0:
        metrics["confidence_multiplier"] = confidence_multiplier
        metrics["loss_multiplier"] = loss_multiplier
        metrics["final_multiplier"] = final_multiplier
        return

    base_notional = _to_float(metrics.get("per_trade_notional"), 0.0)
    entry_price = _to_float(metrics.get("entry"), 0.0)
    adjusted_notional = base_notional * final_multiplier

    metrics["confidence_multiplier"] = confidence_multiplier
    metrics["loss_multiplier"] = loss_multiplier
    metrics["final_multiplier"] = final_multiplier
    metrics["adjusted_per_trade_notional"] = round(adjusted_notional, 4)

    if entry_price <= 0 or adjusted_notional <= 0:
        return

    adjusted_shares = int(adjusted_notional / entry_price)
    metrics["shares"] = adjusted_shares
    metrics["per_trade_notional"] = round(adjusted_notional, 4)
    metrics["actual_position_cost"] = round(adjusted_shares * entry_price, 4)
    metrics["actual_risk"] = round(adjusted_shares * _to_float(metrics.get("risk_per_share"), 0.0), 4)


def _apply_hard_notional_cap(metrics: dict[str, Any]) -> None:
    entry_price = _to_float(metrics.get("entry"), 0.0)
    if entry_price <= 0:
        return

    remaining_allocatable_capital = _to_float(metrics.get("remaining_allocatable_capital"), 0.0)
    configured_hard_cap = _to_float(os.getenv("PAPER_MAX_NOTIONAL"), 0.0)

    hard_cap_candidates = [value for value in (remaining_allocatable_capital, configured_hard_cap) if value > 0]
    if not hard_cap_candidates:
        return

    max_allowed_notional = min(hard_cap_candidates)
    current_notional = _to_float(metrics.get("per_trade_notional"), 0.0)
    if current_notional <= 0:
        return

    capped_notional = min(current_notional, max_allowed_notional)
    capped_shares = int(capped_notional / entry_price)

    metrics["hard_max_notional"] = round(max_allowed_notional, 4)

    if capped_shares <= 0:
        metrics["shares"] = 0
        metrics["per_trade_notional"] = round(capped_notional, 4)
        metrics["adjusted_per_trade_notional"] = round(capped_notional, 4)
        metrics["actual_position_cost"] = 0.0
        metrics["actual_risk"] = 0.0
        return

    final_notional = capped_shares * entry_price
    metrics["shares"] = capped_shares
    metrics["per_trade_notional"] = round(final_notional, 4)
    metrics["adjusted_per_trade_notional"] = round(final_notional, 4)
    metrics["notional_capped_shares"] = capped_shares
    metrics["actual_position_cost"] = round(final_notional, 4)
    metrics["actual_risk"] = round(capped_shares * _to_float(metrics.get("risk_per_share"), 0.0), 4)


def _apply_low_price_notional_cap(metrics: dict[str, Any]) -> None:
    if not LOW_PRICE_NOTIONAL_CAP_ENABLED:
        return

    entry_price = _to_float(metrics.get("entry"), 0.0)
    if entry_price <= 0 or entry_price > LOW_PRICE_THRESHOLD:
        return

    if LOW_PRICE_MAX_NOTIONAL <= 0:
        return

    current_notional = _to_float(metrics.get("per_trade_notional"), 0.0)
    if current_notional <= 0:
        return

    capped_notional = min(current_notional, LOW_PRICE_MAX_NOTIONAL)
    capped_shares = int(capped_notional / entry_price)

    metrics["low_price_threshold"] = round(LOW_PRICE_THRESHOLD, 4)
    metrics["low_price_max_notional"] = round(LOW_PRICE_MAX_NOTIONAL, 4)

    if capped_shares <= 0:
        metrics["shares"] = 0
        metrics["per_trade_notional"] = 0.0
        metrics["adjusted_per_trade_notional"] = 0.0
        metrics["actual_position_cost"] = 0.0
        metrics["actual_risk"] = 0.0
        return

    final_notional = capped_shares * entry_price
    metrics["shares"] = capped_shares
    metrics["per_trade_notional"] = round(final_notional, 4)
    metrics["adjusted_per_trade_notional"] = round(final_notional, 4)
    metrics["notional_capped_shares"] = capped_shares
    metrics["actual_position_cost"] = round(final_notional, 4)
    metrics["actual_risk"] = round(capped_shares * _to_float(metrics.get("risk_per_share"), 0.0), 4)


def execute_scan_request(payload: dict[str, Any], *, handler: Callable[[dict[str, Any]], Any]) -> Any:
    return handler(payload)


def execute_scheduled_scan_request(
    payload: dict[str, Any],
    *,
    now_ny: datetime,
    build_scheduled_scan_payload: Callable[..., dict[str, Any]],
    handler: Callable[[dict[str, Any]], Any],
) -> Any:
    scheduled_payload = build_scheduled_scan_payload(payload, now_ny=now_ny)
    return handler(scheduled_payload)


def execute_full_scan(
    payload: dict[str, Any],
    *,
    market_time_check: Callable[[], tuple[bool, str]],
    build_scan_id: Callable[[str, str], str],
    market_phase_from_timestamp: Callable[[str], str],
    append_signal_log: Callable[[dict[str, Any]], None],
    safe_insert_paper_trade_attempt: Callable[..., None],
    safe_insert_scan_run: Callable[..., None],
    parse_iso_utc: Callable[[str], Any],
    run_scan: Callable[..., Any],
    trade_to_dict: Callable[[Any], dict[str, Any]],
    debug_to_dict: Callable[[Any], dict[str, Any]],
    paper_candidate_from_evaluation: Callable[[Any], Any],
    evaluate_symbol: Callable[..., Any],
    get_latest_open_paper_trade_for_symbol: Callable[[str], Any],
    is_symbol_in_paper_cooldown: Callable[[str, str], tuple[bool, str]],
    place_paper_orders_from_trade: Callable[[Any], Any],
    append_trade_log: Callable[[dict[str, Any]], None],
    safe_insert_trade_event: Callable[..., None],
    safe_insert_broker_order: Callable[..., None],
    upsert_trade_lifecycle: Callable[..., None],
    to_float_or_none: Callable[[Any], float | None],
    MIN_CONFIDENCE: float,
    resolve_account_size: Callable[[dict[str, Any]], float],
    active_broker: str = "IBKR",
) -> dict[str, Any] | tuple[dict[str, Any], int]:
    mode = str(payload.get("mode", "primary")).lower()
    debug_raw = payload.get("debug", False)
    if isinstance(debug_raw, bool):
        debug = debug_raw
    elif isinstance(debug_raw, str):
        debug = debug_raw.strip().lower() in {"true", "1", "yes", "y", "on"}
    else:
        debug = bool(debug_raw)

    scan_source_raw = payload.get("scan_source", "MANUAL")
    scan_source = str(scan_source_raw).strip().upper() or "MANUAL"
    if scan_source not in {"MANUAL", "SCHEDULED"}:
        return {"ok": False, "error": "scan_source must be MANUAL or SCHEDULED"}, 400

    paper_trade_raw = payload.get("paper_trade", False)
    if isinstance(paper_trade_raw, bool):
        paper_trade = paper_trade_raw
    elif isinstance(paper_trade_raw, str):
        paper_trade = paper_trade_raw.strip().lower() in {"true", "1", "yes", "y", "on"}
    else:
        paper_trade = bool(paper_trade_raw)

    try:
        account_size = float(resolve_account_size(payload))
    except Exception as e:
        return {
            "ok": False,
            "error": f"unable to resolve account_size from {active_broker}",
            "details": str(e),
        }, 400
    
    current_open_positions = _to_int(payload.get("current_open_positions", 0), 0)
    current_open_exposure = _to_float(payload.get("current_open_exposure", 0.0), 0.0)
    payload["account_size"] = account_size

    if mode not in {"primary", "secondary", "third", "fourth", "fifth", "sixth", "core_one", "core_two", "core_three"}:
        return {
            "ok": False,
            "error": "mode must be primary, secondary, third, fourth, fifth, sixth, core_one, core_two, or core_three",
        }, 400

    ok, timing_msg = market_time_check()
    scan_started_at = datetime.now(timezone.utc)
    timestamp_utc = datetime.now(timezone.utc).isoformat()

    scan_id = build_scan_id(timestamp_utc, mode)
    market_phase = market_phase_from_timestamp(timestamp_utc)

    def record_attempt(
        decision_stage: str,
        *,
        symbol: str,
        metrics: dict[str, Any] | None = None,
        final_reason: str | None = None,
        placed: bool | None = None,
        broker: str | None = None,
        broker_order_id: str | None = None,
        broker_parent_order_id: str | None = None,
        broker_rejection_reason: str | None = None,
    ) -> None:
        attempt_metrics = metrics or {}
        normalized_broker = str(broker or active_broker or "").strip().upper() or "IBKR"
        safe_insert_paper_trade_attempt(
            timestamp_utc=parse_iso_utc(timestamp_utc),
            scan_id=scan_id,
            mode=mode,
            scan_source=scan_source,
            market_phase=market_phase,
            symbol=symbol,
            decision_stage=decision_stage,
            final_reason=final_reason,
            direction=str(attempt_metrics.get("direction", "") or ""),
            entry=to_float_or_none(attempt_metrics.get("entry", "")),
            stop=to_float_or_none(attempt_metrics.get("stop", "")),
            target=to_float_or_none(attempt_metrics.get("target", "")),
            confidence=to_float_or_none(attempt_metrics.get("final_confidence", "")),
            account_size=account_size,
            current_open_positions=_to_int(attempt_metrics.get("current_open_positions", current_open_positions), current_open_positions),
            current_open_exposure=_to_float(attempt_metrics.get("current_open_exposure", current_open_exposure), current_open_exposure),
            remaining_slots=_to_int(attempt_metrics.get("remaining_slots", 0), 0),
            effective_remaining_slots=_to_int(attempt_metrics.get("effective_remaining_slots", 0), 0),
            remaining_allocatable_capital=_to_float(attempt_metrics.get("remaining_allocatable_capital", 0.0), 0.0),
            per_trade_notional=to_float_or_none(attempt_metrics.get("per_trade_notional", "")),
            adjusted_per_trade_notional=to_float_or_none(attempt_metrics.get("adjusted_per_trade_notional", "")),
            shares=to_float_or_none(attempt_metrics.get("shares", "")),
            cash_affordable_shares=_to_int(attempt_metrics.get("cash_affordable_shares", 0), 0),
            notional_capped_shares=_to_int(attempt_metrics.get("notional_capped_shares", 0), 0),
            confidence_multiplier=to_float_or_none(attempt_metrics.get("confidence_multiplier", "")),
            loss_multiplier=to_float_or_none(attempt_metrics.get("loss_multiplier", "")),
            final_multiplier=to_float_or_none(attempt_metrics.get("final_multiplier", "")),
            placed=placed,
            broker=normalized_broker,
            broker_order_id=broker_order_id,
            broker_parent_order_id=broker_parent_order_id,
            broker_rejection_reason=broker_rejection_reason,
        )

    if not ok:
        try:
            append_signal_log({
                "timestamp_utc": timestamp_utc,
                "scan_id": scan_id,
                "scan_source": scan_source,
                "market_phase": market_phase,
                "scan_execution_time_ms": int((datetime.now(timezone.utc) - scan_started_at).total_seconds() * 1000),
                "mode": mode,
                "account_size": account_size,
                "current_open_positions": current_open_positions,
                "current_open_exposure": current_open_exposure,
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
                "paper_candidate_confidences": "",
                "paper_skipped_symbols": "",
                "paper_skip_reasons": "",
                "paper_placed_symbols": "",
                "paper_trade_ids": "",
            })
        except Exception as e:
            log_exception("Signal log write failed", e, component="scan_service", operation="execute_full_scan", mode=mode)
        safe_insert_scan_run(
            scan_time=parse_iso_utc(timestamp_utc),
            mode=mode,
            scan_source=scan_source,
            market_phase=market_phase,
            candidate_count=0,
            placed_count=0,
            skipped_count=0,
        )
        return {
            "ok": True,
            "timing_ok": False,
            "message": timing_msg,
            "source": mode.upper(),
            "trades": [],
            "min_confidence": MIN_CONFIDENCE,
            "paper_trade_enabled": paper_trade,
        }

    all_trades, evaluations, _fetch_ok, _fetch_fail, benchmark_directions, source = run_scan(
        account_size,
        mode,
        current_open_positions=current_open_positions,
        current_open_exposure=current_open_exposure,
    )
    try:
        log_info("Using live IBKR equity/account_size", component="scan_service", operation="execute_full_scan", account_size=account_size, mode=mode)
    except Exception:
        pass
    trades = [t for t in all_trades if t["metrics"].get("direction") == "BUY"]
    paper_trade_candidates = []
    for ev in evaluations:
        ev_metrics = ev.get("metrics") or {}
        rejected_symbol = str(ev_metrics.get("symbol", "")).strip().upper()
        if paper_trade and str(ev.get("decision", "")).strip().upper() != "VALID":
            if rejected_symbol:
                record_attempt(
                    "SCAN_REJECTED",
                    symbol=rejected_symbol,
                    metrics=ev_metrics,
                    final_reason=str(ev.get("final_reason", "") or "scan_rejected"),
                    placed=False,
                )
        candidate = paper_candidate_from_evaluation(ev)
        if candidate is not None:
            if paper_trade:
                record_attempt(
                    "PAPER_CANDIDATE",
                    symbol=str(candidate["metrics"].get("symbol", "")).strip().upper(),
                    metrics=candidate["metrics"],
                    final_reason=str(candidate.get("final_reason", "") or "paper_candidate"),
                    placed=False,
                )
            paper_trade_candidates.append(candidate)
        elif paper_trade and rejected_symbol and str(ev.get("decision", "")).strip().upper() == "VALID":
            record_attempt(
                "SCAN_REJECTED",
                symbol=rejected_symbol,
                metrics=ev_metrics,
                final_reason=str(ev.get("final_reason", "") or "paper_candidate_rejected"),
                placed=False,
            )
    paper_long_candidates = [t for t in paper_trade_candidates if t["metrics"].get("direction") == "BUY"]
    paper_short_candidates = [t for t in paper_trade_candidates if t["metrics"].get("direction") == "SELL"]

    paper_candidate_symbols = ",".join(
        t["metrics"].get("symbol", "") for t in paper_trade_candidates if t["metrics"].get("symbol", "")
    )

    paper_candidate_confidences = ",".join(
        str(t["metrics"].get("final_confidence", "")) for t in paper_trade_candidates if t["metrics"].get("symbol", "")
    )

    response = {
        "ok": True,
        "timing_ok": True,
        "message": timing_msg,
        "source": source,
        "benchmark_directions": benchmark_directions,
        "min_confidence": MIN_CONFIDENCE,
        "trade_count": len(trades),
        "paper_trade_candidate_count": len(paper_trade_candidates),
        "paper_trade_long_candidate_count": len(paper_long_candidates),
        "paper_trade_short_candidate_count": len(paper_short_candidates),
        "trades": [trade_to_dict(t) for t in trades],
        "paper_trade_enabled": paper_trade,
        "scan_source": scan_source,
        "current_open_positions": current_open_positions,
        "current_open_exposure": current_open_exposure,
    }

    # --- Daily Risk Guardrail (block trading on drawdown) ---
    # Expect optional payload fields from upstream (app layer):
    # - daily_realized_pnl
    # - daily_unrealized_pnl
    # - account_size (already available)
    try:
        daily_realized_pnl = _to_float(payload.get("daily_realized_pnl"), 0.0)
        daily_unrealized_pnl = _to_float(payload.get("daily_unrealized_pnl"), 0.0)
        combined_daily_pnl = daily_realized_pnl + daily_unrealized_pnl
        daily_pnl_pct = (combined_daily_pnl / account_size) if account_size > 0 else 0.0
    except Exception:
        combined_daily_pnl = 0.0
        daily_pnl_pct = 0.0

    # If drawdown exceeds -2%, block new placements
    trading_blocked = daily_pnl_pct <= -0.02
    response["daily_pnl_pct"] = round(daily_pnl_pct * 100.0, 2)
    response["trading_blocked"] = trading_blocked

    if debug:
        response["evaluations"] = [debug_to_dict(ev) for ev in evaluations]

    top_trade = trades[0] if trades else None
    high_quality_long_signal_alert = _maybe_send_high_quality_long_signal_alert(
        scan_id=scan_id,
        scan_source=scan_source,
        mode=mode,
        top_trade=top_trade,
        trades=trades,
        paper_trade=paper_trade,
        current_open_positions=current_open_positions,
        current_open_exposure=current_open_exposure,
        source=source,
        benchmark_directions=benchmark_directions,
    )
    if high_quality_long_signal_alert is not None:
        response["high_quality_long_signal_alert"] = high_quality_long_signal_alert

    if paper_trade:
        # Enforce daily risk guardrail
        if trading_blocked:
            for candidate in paper_trade_candidates:
                candidate_metrics = candidate.get("metrics") or {}
                candidate_symbol = str(candidate_metrics.get("symbol", "")).strip().upper()
                if candidate_symbol:
                    record_attempt(
                        "PLACEMENT_SKIPPED",
                        symbol=candidate_symbol,
                        metrics=candidate_metrics,
                        final_reason="daily_loss_guardrail_blocked",
                        placed=False,
                    )
            response["paper_trade_result"] = {
                "attempted": False,
                "placed": False,
                "reason": "daily_loss_guardrail_blocked",
                "daily_pnl_pct": response.get("daily_pnl_pct"),
            }
            return response
        if not paper_trade_candidates:
            response["paper_trade_result"] = {
                "attempted": False,
                "placed": False,
                "reason": "no_paper_trade_candidates_at_or_above_threshold",
                "candidate_count": len(paper_trade_candidates),
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
            skipped_symbols = []
            skip_reasons = []
            placed_trade_ids = []

            for paper_trade_candidate in paper_trade_candidates:
                # --- Adaptive cooldown + sizing ---
                candidate_symbol = str(paper_trade_candidate["metrics"].get("symbol", "")).strip().upper()

                try:
                    # Fetch recent closed trades for symbol (last 5)
                    recent_trades = get_recent_closed_trades_for_symbol(candidate_symbol, limit=PAPER_SYMBOL_GATING_LOOKBACK)
                except Exception:
                    recent_trades = []

                should_block_symbol, symbol_block_reason, symbol_gate_details = evaluate_symbol_performance_gate(recent_trades)
                if should_block_symbol:
                    record_attempt(
                        "PLACEMENT_SKIPPED",
                        symbol=candidate_symbol,
                        metrics=paper_trade_candidate["metrics"],
                        final_reason=symbol_block_reason,
                        placed=False,
                    )
                    paper_results.append({
                        "attempted": False,
                        "placed": False,
                        "symbol": candidate_symbol,
                        "reason": symbol_block_reason,
                        "details": symbol_gate_details,
                    })
                    skipped_symbols.append(candidate_symbol)
                    skip_reasons.append(f"{candidate_symbol}:symbol_performance_blocked")
                    continue

                consecutive_losses = 0
                for t in recent_trades:
                    pnl = _to_float(t.get("realized_pnl"), 0.0)
                    if pnl < 0:
                        consecutive_losses += 1
                    else:
                        break

                # Cooldown rule
                if consecutive_losses >= PAPER_CONSECUTIVE_LOSS_COOLDOWN_THRESHOLD:
                    cooldown_minutes = PAPER_CONSECUTIVE_LOSS_COOLDOWN_MINUTES
                    last_trade_time = recent_trades[0].get("exit_time") if recent_trades else None
                    if last_trade_time:
                        try:
                            last_dt = parse_iso_utc(str(last_trade_time))
                            now_dt = parse_iso_utc(timestamp_utc)
                            minutes_since = (now_dt - last_dt).total_seconds() / 60
                            if minutes_since < cooldown_minutes:
                                record_attempt(
                                    "PLACEMENT_SKIPPED",
                                    symbol=candidate_symbol,
                                    metrics=paper_trade_candidate["metrics"],
                                    final_reason=f"cooldown_active_{int(minutes_since)}m",
                                    placed=False,
                                )
                                paper_results.append({
                                    "attempted": False,
                                    "placed": False,
                                    "symbol": candidate_symbol,
                                    "reason": f"cooldown_active_{int(minutes_since)}m",
                                })
                                skipped_symbols.append(candidate_symbol)
                                skip_reasons.append(f"{candidate_symbol}:cooldown_active")
                                continue
                        except Exception:
                            pass

                # --- Confidence-weighted + loss-aware sizing ---
                confidence = _to_float(paper_trade_candidate["metrics"].get("final_confidence"), 0.0)

                # Base confidence scaling (AI-like)
                # 70 → 0.5x, 85 → 1.0x, 100 → 1.5x
                confidence_multiplier = max(0.5, min(1.5, (confidence - 70) / 30 + 0.5))

                # Loss-based penalty
                loss_multiplier = 1.0
                if consecutive_losses >= 3:
                    loss_multiplier = 0.25
                elif consecutive_losses == 2:
                    loss_multiplier = 0.5

                final_multiplier = confidence_multiplier * loss_multiplier

                _apply_confidence_loss_sizing(
                    paper_trade_candidate["metrics"],
                    confidence_multiplier=confidence_multiplier,
                    loss_multiplier=loss_multiplier,
                    final_multiplier=final_multiplier,
                )
                _apply_hard_notional_cap(paper_trade_candidate["metrics"])
                _apply_low_price_notional_cap(paper_trade_candidate["metrics"])
                # --- Expose multipliers at response level (latest candidate wins) ---
                try:
                    response["confidence_multiplier"] = round(confidence_multiplier, 4)
                    response["loss_multiplier"] = round(loss_multiplier, 4)
                    response["final_sizing_multiplier"] = round(final_multiplier, 4)
                except Exception:
                    pass
                paper_metrics = paper_trade_candidate["metrics"]
                paper_metrics["current_open_positions"] = current_open_positions
                paper_metrics["current_open_exposure"] = current_open_exposure
                candidate_symbol = str(paper_metrics.get("symbol", "")).strip().upper()

                _apply_minimum_viable_position_sizing(paper_metrics)

                candidate_name = paper_trade_candidate.get("name", "")
                candidate_info = paper_trade_candidate.get("info")
                candidate_candles = paper_trade_candidate.get("candles")
                candidate_benchmark_directions = paper_trade_candidate.get("benchmark_directions", benchmark_directions)

                if candidate_info is not None and candidate_candles is not None:
                    refreshed_evaluation = evaluate_symbol(
                        candidate_name,
                        candidate_info,
                        candidate_candles,
                        account_size,
                        candidate_benchmark_directions,
                        current_open_positions=current_open_positions,
                        current_open_exposure=current_open_exposure,
                    )
                    refreshed_candidate = paper_candidate_from_evaluation(refreshed_evaluation)
                    if refreshed_candidate is None:
                        record_attempt(
                            "REFRESH_REJECTED",
                            symbol=candidate_symbol,
                            metrics=paper_metrics,
                            final_reason=str(refreshed_evaluation.get("final_reason", "") or "candidate_no_longer_valid"),
                            placed=False,
                        )
                        paper_results.append({
                            "attempted": False,
                            "placed": False,
                            "reason": refreshed_evaluation.get("final_reason", "candidate_no_longer_valid"),
                            "symbol": candidate_symbol,
                        })
                        skipped_symbols.append(candidate_symbol)
                        skip_reasons.append(f"{candidate_symbol}:{refreshed_evaluation.get('final_reason', 'candidate_no_longer_valid')}")
                        continue
                    paper_trade_candidate = refreshed_candidate
                    paper_metrics = paper_trade_candidate["metrics"]
                    paper_metrics["current_open_positions"] = current_open_positions
                    paper_metrics["current_open_exposure"] = current_open_exposure
                    candidate_symbol = str(paper_metrics.get("symbol", "")).strip().upper()

                    _apply_confidence_loss_sizing(
                        paper_metrics,
                        confidence_multiplier=confidence_multiplier,
                        loss_multiplier=loss_multiplier,
                        final_multiplier=final_multiplier,
                    )
                    _apply_hard_notional_cap(paper_metrics)
                    _apply_low_price_notional_cap(paper_metrics)
                    _apply_minimum_viable_position_sizing(paper_metrics)

                if _to_int(paper_metrics.get("shares"), 0) <= 0:
                    log_warning(
                        "Paper trade candidate rejected after sizing",
                        component="scan_service",
                        operation="execute_full_scan",
                        scan_id=scan_id,
                        symbol=candidate_symbol,
                        mode=mode,
                        reason="position_size_too_small_after_slot_compression",
                        account_size=account_size,
                        current_open_positions=current_open_positions,
                        current_open_exposure=current_open_exposure,
                        entry_price=_to_float(paper_metrics.get("entry"), 0.0),
                        stop_price=_to_float(paper_metrics.get("stop"), 0.0),
                        target_price=_to_float(paper_metrics.get("target"), 0.0),
                        shares=_to_int(paper_metrics.get("shares"), 0),
                        remaining_slots=_to_int(paper_metrics.get("remaining_slots"), 0),
                        effective_remaining_slots=_to_int(paper_metrics.get("effective_remaining_slots"), 0),
                        remaining_allocatable_capital=_to_float(paper_metrics.get("remaining_allocatable_capital"), 0.0),
                        per_trade_notional=_to_float(paper_metrics.get("per_trade_notional"), 0.0),
                        adjusted_per_trade_notional=_to_float(paper_metrics.get("adjusted_per_trade_notional"), 0.0),
                        cash_affordable_shares=_to_int(paper_metrics.get("cash_affordable_shares"), 0),
                        notional_capped_shares=_to_int(paper_metrics.get("notional_capped_shares"), 0),
                        confidence=_to_float(paper_metrics.get("final_confidence"), 0.0),
                        confidence_multiplier=_to_float(paper_metrics.get("confidence_multiplier"), 0.0),
                        loss_multiplier=_to_float(paper_metrics.get("loss_multiplier"), 0.0),
                        final_multiplier=_to_float(paper_metrics.get("final_multiplier"), 0.0),
                    )
                    record_attempt(
                        "PLACEMENT_SKIPPED",
                        symbol=candidate_symbol,
                        metrics=paper_metrics,
                        final_reason="position_size_too_small_after_slot_compression",
                        placed=False,
                    )
                    paper_results.append({
                        "attempted": False,
                        "placed": False,
                        "reason": "position_size_too_small_after_slot_compression",
                        "symbol": candidate_symbol,
                        "entry_price": _to_float(paper_metrics.get("entry"), 0.0),
                        "remaining_allocatable_capital": _to_float(paper_metrics.get("remaining_allocatable_capital"), 0.0),
                        "remaining_slots": _to_int(paper_metrics.get("remaining_slots"), 0),
                    })
                    skipped_symbols.append(candidate_symbol)
                    skip_reasons.append(f"{candidate_symbol}:position_size_too_small_after_slot_compression")
                    continue

                existing_open_trade = get_latest_open_paper_trade_for_symbol(candidate_symbol)
                if existing_open_trade is not None:
                    record_attempt(
                        "PLACEMENT_SKIPPED",
                        symbol=candidate_symbol,
                        metrics=paper_metrics,
                        final_reason="symbol_already_open",
                        placed=False,
                    )
                    paper_results.append({
                        "attempted": False,
                        "placed": False,
                        "reason": "symbol_already_open",
                        "symbol": candidate_symbol,
                    })
                    skipped_symbols.append(candidate_symbol)
                    skip_reasons.append(f"{candidate_symbol}:symbol_already_open")
                    continue

                in_cooldown, cooldown_reason = is_symbol_in_paper_cooldown(candidate_symbol, timestamp_utc)
                if in_cooldown:
                    record_attempt(
                        "PLACEMENT_SKIPPED",
                        symbol=candidate_symbol,
                        metrics=paper_metrics,
                        final_reason=cooldown_reason,
                        placed=False,
                    )
                    paper_results.append({
                        "attempted": False,
                        "placed": False,
                        "reason": cooldown_reason,
                        "symbol": candidate_symbol,
                    })
                    skipped_symbols.append(candidate_symbol)
                    skip_reasons.append(f"{candidate_symbol}:{cooldown_reason}")
                    continue

                try:
                    broker_results = _normalize_paper_trade_results(place_paper_orders_from_trade(paper_trade_candidate))
                    paper_results.extend(broker_results)
                    candidate_placed_any = False
                    candidate_rejection_reasons: list[str] = []

                    for paper_trade_result in broker_results:
                        broker_name = _paper_trade_broker_name(paper_trade_result)
                        if paper_trade_result.get("placed"):
                            candidate_placed_any = True
                            trade_source = _paper_trade_source(broker_name)
                            broker_order_id = _paper_trade_order_id(paper_trade_result)
                            broker_parent_order_id = _paper_trade_parent_order_id(paper_trade_result)
                            broker_order_status = _paper_trade_order_status(paper_trade_result)
                            record_attempt(
                                "PLACED",
                                symbol=candidate_symbol,
                                metrics=paper_metrics,
                                final_reason="placed",
                                placed=True,
                                broker=broker_name,
                                broker_order_id=broker_order_id,
                                broker_parent_order_id=broker_parent_order_id,
                            )
                            placed_trade_ids.append(str(paper_trade_result.get("client_order_id", "")).strip())
                            append_trade_log({
                                "timestamp_utc": timestamp_utc,
                                "event_type": "OPEN",
                                "symbol": paper_metrics.get("symbol", ""),
                                "name": paper_trade_candidate.get("name", ""),
                                "mode": mode,
                                "trade_source": trade_source,
                                "broker": broker_name,
                                "broker_order_id": broker_order_id,
                                "broker_parent_order_id": broker_parent_order_id,
                                "broker_status": broker_order_status,
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
                            safe_insert_trade_event(
                                event_time=parse_iso_utc(timestamp_utc),
                                event_type="OPEN",
                                symbol=str(paper_metrics.get("symbol", "") or ""),
                                side=str(paper_metrics.get("direction", "") or ""),
                                shares=to_float_or_none(paper_trade_result.get("shares", "")),
                                price=to_float_or_none(paper_metrics.get("entry", "")),
                                mode=mode,
                                broker=broker_name,
                                order_id=broker_order_id,
                                parent_order_id=broker_parent_order_id,
                                status="OPEN",
                            )
                            safe_insert_broker_order(
                                order_id=broker_order_id,
                                broker=broker_name,
                                symbol=str(paper_metrics.get("symbol", "") or ""),
                                side=str(paper_metrics.get("direction", "") or ""),
                                order_type="bracket_entry",
                                status=broker_order_status,
                                qty=to_float_or_none(paper_trade_result.get("shares", "")),
                                filled_qty=None,
                                avg_fill_price=None,
                                submitted_at=parse_iso_utc(timestamp_utc),
                                filled_at=None,
                            )

                            entry_price = paper_metrics.get("entry", "")
                            stop_price = paper_metrics.get("stop", "")
                            target_price = paper_metrics.get("target", "")
                            shares_value = paper_trade_result.get("shares", "")
                            direction = _infer_direction(
                                entry_price=entry_price,
                                stop_price=stop_price,
                                target_price=target_price,
                                side=paper_metrics.get("direction", ""),
                            )
                            trade_key = _normalize_trade_key(
                                str(paper_metrics.get("symbol", "") or ""),
                                broker_parent_order_id,
                                broker_order_id,
                                broker_name,
                            )

                            upsert_trade_lifecycle(
                                trade_key=trade_key,
                                symbol=str(paper_metrics.get("symbol", "") or ""),
                                mode=mode,
                                side=_to_upper_or_none(paper_metrics.get("direction", "")),
                                direction=direction,
                                status="OPEN",
                                entry_time=parse_iso_utc(timestamp_utc),
                                entry_price=to_float_or_none(entry_price),
                                exit_time=None,
                                exit_price=None,
                                stop_price=to_float_or_none(stop_price),
                                target_price=to_float_or_none(target_price),
                                exit_reason=None,
                                shares=to_float_or_none(shares_value),
                                realized_pnl=None,
                                realized_pnl_percent=None,
                                duration_minutes=None,
                                signal_timestamp=parse_iso_utc(timestamp_utc),
                                signal_entry=to_float_or_none(entry_price),
                                signal_stop=to_float_or_none(stop_price),
                                signal_target=to_float_or_none(target_price),
                                signal_confidence=to_float_or_none(paper_metrics.get("final_confidence", "")),
                                broker=broker_name,
                                order_id=broker_order_id,
                                parent_order_id=broker_parent_order_id,
                                exit_order_id=None,
                            )
                        else:
                            rejection_reason = str(paper_trade_result.get("reason", "") or "not_placed")
                            candidate_rejection_reasons.append(f"{broker_name}:{rejection_reason}")
                            record_attempt(
                                "PLACEMENT_REJECTED",
                                symbol=candidate_symbol,
                                metrics=paper_metrics,
                                final_reason=rejection_reason,
                                placed=False,
                                broker=broker_name,
                                broker_order_id=_paper_trade_order_id(paper_trade_result),
                                broker_parent_order_id=_paper_trade_parent_order_id(paper_trade_result),
                                broker_rejection_reason=str(paper_trade_result.get("details", "") or rejection_reason or ""),
                            )

                    if candidate_placed_any:
                        placed_count += 1
                        if paper_metrics.get("direction") == "BUY":
                            placed_long_count += 1
                        elif paper_metrics.get("direction") == "SELL":
                            placed_short_count += 1
                        current_open_positions += 1
                        representative_result = next((item for item in broker_results if item.get("placed")), None)
                        if representative_result is not None:
                            current_open_exposure += _to_float(representative_result.get("estimated_notional", 0.0), 0.0)
                            paper_metrics["current_open_positions"] = current_open_positions
                            paper_metrics["current_open_exposure"] = current_open_exposure
                    else:
                        skipped_symbols.append(candidate_symbol)
                        skip_reasons.append(
                            f"{candidate_symbol}:{'|'.join(candidate_rejection_reasons) if candidate_rejection_reasons else 'not_placed'}"
                        )
                except Exception as e:
                    log_exception(
                        "Paper trade placement failed",
                        e,
                        component="scan_service",
                        operation="execute_full_scan",
                        symbol=candidate_symbol,
                        mode=mode,
                    )
                    record_attempt(
                        "PLACEMENT_REJECTED",
                        symbol=candidate_symbol,
                        metrics=paper_metrics,
                        final_reason="paper_trade_exception",
                        placed=False,
                        broker_rejection_reason=str(e),
                    )
                    paper_results.append({
                        "attempted": True,
                        "placed": False,
                        "reason": "paper_trade_exception",
                        "details": str(e),
                        "symbol": candidate_symbol,
                    })
                    skipped_symbols.append(candidate_symbol)
                    skip_reasons.append(f"{candidate_symbol}:paper_trade_exception")

            response["paper_trade_result"] = {
                "attempted": True,
                "placed": placed_count > 0,
                "candidate_count": len(paper_trade_candidates),
                "long_candidate_count": len(paper_long_candidates),
                "short_candidate_count": len(paper_short_candidates),
                "placed_count": placed_count,
                "placed_long_count": placed_long_count,
                "placed_short_count": placed_short_count,
                "skipped_symbols": skipped_symbols,
                "skip_reasons": skip_reasons,
                "placed_trade_ids": placed_trade_ids,
                "results": paper_results,
            }

    paper_trade_result = response.get("paper_trade_result", {}) if paper_trade else {}
    paper_trade_placed_count = paper_trade_result.get("placed_count", 0) if isinstance(paper_trade_result, dict) else 0
    paper_trade_placed_long_count = paper_trade_result.get("placed_long_count", 0) if isinstance(paper_trade_result, dict) else 0
    paper_trade_placed_short_count = paper_trade_result.get("placed_short_count", 0) if isinstance(paper_trade_result, dict) else 0
    paper_skipped_symbols = ""
    paper_skip_reasons = ""
    paper_trade_ids = ""
    paper_placed_symbols = ""
    if isinstance(paper_trade_result, dict):
        paper_result_items = paper_trade_result.get("results", []) or []
        paper_placed_symbols = ",".join(
            str(item.get("symbol", "")).strip().upper()
            for item in paper_result_items
            if isinstance(item, dict) and item.get("placed") and str(item.get("symbol", "")).strip()
        )
        paper_skipped_symbols = ",".join(
            str(symbol).strip().upper()
            for symbol in (paper_trade_result.get("skipped_symbols", []) or [])
            if str(symbol).strip()
        )
        paper_skip_reasons = "|".join(
            str(reason).strip()
            for reason in (paper_trade_result.get("skip_reasons", []) or [])
            if str(reason).strip()
        )
        paper_trade_ids = ",".join(
            str(trade_id).strip()
            for trade_id in (paper_trade_result.get("placed_trade_ids", []) or [])
            if str(trade_id).strip()
        )

    try:
        top_metrics = top_trade["metrics"] if top_trade else {}

        append_signal_log({
            "timestamp_utc": timestamp_utc,
            "scan_id": scan_id,
            "scan_source": scan_source,
            "market_phase": market_phase,
            "scan_execution_time_ms": int((datetime.now(timezone.utc) - scan_started_at).total_seconds() * 1000),
            "mode": mode,
            "account_size": account_size,
            "current_open_positions": current_open_positions,
            "current_open_exposure": current_open_exposure,
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
            "paper_trade_candidate_count": len(paper_trade_candidates),
            "paper_trade_long_candidate_count": len(paper_long_candidates),
            "paper_trade_short_candidate_count": len(paper_short_candidates),
            "paper_trade_placed_count": paper_trade_placed_count,
            "paper_trade_placed_long_count": paper_trade_placed_long_count,
            "paper_trade_placed_short_count": paper_trade_placed_short_count,
            "paper_candidate_symbols": paper_candidate_symbols,
            "paper_candidate_confidences": paper_candidate_confidences,
            "paper_skipped_symbols": paper_skipped_symbols,
            "paper_skip_reasons": paper_skip_reasons,
            "paper_placed_symbols": paper_placed_symbols,
            "paper_trade_ids": paper_trade_ids,
        })
    except Exception as e:
        log_exception("Signal log write failed", e, component="scan_service", operation="execute_full_scan", mode=mode)
    safe_insert_scan_run(
        scan_time=parse_iso_utc(timestamp_utc),
        mode=mode,
        scan_source=scan_source,
        market_phase=market_phase,
        candidate_count=len(paper_trade_candidates),
        placed_count=paper_trade_placed_count,
        skipped_count=len([s for s in paper_skipped_symbols.split(",") if s.strip()]),
    )
    return response
