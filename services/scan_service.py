from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable
import os
from logging_utils import log_exception, log_info

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
        from db import fetch_recent_closed_trades_for_symbol  # type: ignore
        return fetch_recent_closed_trades_for_symbol(symbol=symbol, limit=limit) or []
    except Exception:
        return []


def _normalize_trade_key(symbol: str, broker_parent_order_id: str, broker_order_id: str) -> str:
    return broker_parent_order_id or broker_order_id or symbol


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


def _get_live_alpaca_account_equity(payload: dict[str, Any]) -> float:
    """
    Resolve sizing equity from Alpaca first, with a safe fallback chain:
    1. live Alpaca account equity
    2. request payload account_size
    3. env fallback for emergency/manual use

    This keeps scan sizing aligned with the actual broker account while still
    allowing the service to run if Alpaca is temporarily unavailable.
    """
    try:
        account_getters: list[Callable[[], Any]] = []

        for module_name in ("paper_alpaca", "services.paper_alpaca"):
            try:
                module = __import__(module_name, fromlist=["get_account", "get_paper_account"])
            except Exception:
                continue

            get_account = getattr(module, "get_account", None)
            if callable(get_account):
                account_getters.append(get_account)

            get_paper_account = getattr(module, "get_paper_account", None)
            if callable(get_paper_account):
                account_getters.append(get_paper_account)

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

    raise ValueError("Unable to resolve account equity from Alpaca, payload, or environment")


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
    safe_insert_scan_run: Callable[..., None],
    parse_iso_utc: Callable[[str], Any],
    run_scan: Callable[..., Any],
    trade_to_dict: Callable[[Any], dict[str, Any]],
    debug_to_dict: Callable[[Any], dict[str, Any]],
    paper_candidate_from_evaluation: Callable[[Any], Any],
    evaluate_symbol: Callable[..., Any],
    get_latest_open_paper_trade_for_symbol: Callable[[str], Any],
    is_symbol_in_paper_cooldown: Callable[[str, str], tuple[bool, str]],
    place_paper_bracket_order_from_trade: Callable[[Any], dict[str, Any]],
    append_trade_log: Callable[[dict[str, Any]], None],
    safe_insert_trade_event: Callable[..., None],
    safe_insert_broker_order: Callable[..., None],
    upsert_trade_lifecycle: Callable[..., None],
    to_float_or_none: Callable[[Any], float | None],
    MIN_CONFIDENCE: float,
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
        account_size = float(_get_live_alpaca_account_equity(payload))
    except Exception as e:
        return {
            "ok": False,
            "error": "unable to resolve account_size from Alpaca",
            "details": str(e),
        }, 400
    
    current_open_positions = _to_int(payload.get("current_open_positions", 0), 0)
    current_open_exposure = _to_float(payload.get("current_open_exposure", 0.0), 0.0)
    payload["account_size"] = account_size

    if mode not in {"primary", "secondary", "third", "fourth", "core_one", "core_two"}:
        return {"ok": False, "error": "mode must be primary, secondary, third, fourth, core_one, or core_two"}, 400

    ok, timing_msg = market_time_check()
    scan_started_at = datetime.now(timezone.utc)
    timestamp_utc = datetime.now(timezone.utc).isoformat()

    scan_id = build_scan_id(timestamp_utc, mode)
    market_phase = market_phase_from_timestamp(timestamp_utc)

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
        log_info("Using live Alpaca equity/account_size", component="scan_service", operation="execute_full_scan", account_size=account_size, mode=mode)
    except Exception:
        pass
    trades = [t for t in all_trades if t["metrics"].get("direction") == "BUY"]
    paper_trade_candidates = []
    for ev in evaluations:
        candidate = paper_candidate_from_evaluation(ev)
        if candidate is not None:
            paper_trade_candidates.append(candidate)
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

    if paper_trade:
        # Enforce daily risk guardrail
        if trading_blocked:
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
                    recent_trades = get_recent_closed_trades_for_symbol(candidate_symbol, limit=5)
                except Exception:
                    recent_trades = []

                consecutive_losses = 0
                for t in recent_trades:
                    pnl = _to_float(t.get("realized_pnl"), 0.0)
                    if pnl < 0:
                        consecutive_losses += 1
                    else:
                        break

                # Cooldown rule
                if consecutive_losses >= 2:
                    cooldown_minutes = 60
                    last_trade_time = recent_trades[0].get("exit_time") if recent_trades else None
                    if last_trade_time:
                        try:
                            last_dt = parse_iso_utc(str(last_trade_time))
                            now_dt = parse_iso_utc(timestamp_utc)
                            minutes_since = (now_dt - last_dt).total_seconds() / 60
                            if minutes_since < cooldown_minutes:
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

                if final_multiplier < 1.0 or final_multiplier > 1.0:
                    base_notional = _to_float(paper_trade_candidate["metrics"].get("per_trade_notional"), 0.0)
                    adjusted_notional = base_notional * final_multiplier
                    entry_price = _to_float(paper_trade_candidate["metrics"].get("entry", 0.0))

                    if entry_price > 0:
                        adjusted_shares = int(adjusted_notional / entry_price)
                        if adjusted_shares > 0:
                            paper_trade_candidate["metrics"]["shares"] = adjusted_shares
                            paper_trade_candidate["metrics"]["per_trade_notional"] = adjusted_notional
                            paper_trade_candidate["metrics"]["confidence_multiplier"] = confidence_multiplier
                            paper_trade_candidate["metrics"]["loss_multiplier"] = loss_multiplier
                            paper_trade_candidate["metrics"]["final_multiplier"] = final_multiplier
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

                    _apply_minimum_viable_position_sizing(paper_metrics)

                if _to_int(paper_metrics.get("shares"), 0) <= 0:
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
                    paper_trade_result = place_paper_bracket_order_from_trade(paper_trade_candidate)
                    paper_results.append(paper_trade_result)

                    if paper_trade_result.get("placed"):
                        placed_count += 1
                        if paper_metrics.get("direction") == "BUY":
                            placed_long_count += 1
                        elif paper_metrics.get("direction") == "SELL":
                            placed_short_count += 1

                        placed_trade_ids.append(str(paper_trade_result.get("client_order_id", "")).strip())
                        current_open_positions += 1
                        current_open_exposure += _to_float(paper_trade_result.get("estimated_notional", 0.0), 0.0)
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
                        safe_insert_trade_event(
                            event_time=parse_iso_utc(timestamp_utc),
                            event_type="OPEN",
                            symbol=str(paper_metrics.get("symbol", "") or ""),
                            side=str(paper_metrics.get("direction", "") or ""),
                            shares=to_float_or_none(paper_trade_result.get("shares", "")),
                            price=to_float_or_none(paper_metrics.get("entry", "")),
                            mode=mode,
                            order_id=str(paper_trade_result.get("alpaca_order_id", "") or ""),
                            parent_order_id=str(paper_trade_result.get("alpaca_order_id", "") or ""),
                            status="OPEN",
                        )
                        safe_insert_broker_order(
                            order_id=str(paper_trade_result.get("alpaca_order_id", "") or ""),
                            symbol=str(paper_metrics.get("symbol", "") or ""),
                            side=str(paper_metrics.get("direction", "") or ""),
                            order_type="bracket_entry",
                            status=str(paper_trade_result.get("alpaca_order_status", "") or ""),
                            qty=to_float_or_none(paper_trade_result.get("shares", "")),
                            filled_qty=None,
                            avg_fill_price=None,
                            submitted_at=parse_iso_utc(timestamp_utc),
                            filled_at=None,
                        )

                        broker_order_id = str(paper_trade_result.get("alpaca_order_id", "") or "")
                        broker_parent_order_id = str(paper_trade_result.get("alpaca_order_id", "") or "")
                        entry_price = paper_metrics.get("entry", "")
                        stop_price = paper_metrics.get("stop", "")
                        target_price = paper_metrics.get("target", "")
                        shares_value = paper_trade_result.get("shares", "")
                        paper_metrics["current_open_positions"] = current_open_positions
                        paper_metrics["current_open_exposure"] = current_open_exposure
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
                            order_id=broker_order_id,
                            parent_order_id=broker_parent_order_id,
                            exit_order_id=None,
                        )
                    else:
                        skipped_symbols.append(candidate_symbol)
                        skip_reasons.append(f"{candidate_symbol}:{paper_trade_result.get('reason', 'not_placed')}")
                except Exception as e:
                    log_exception(
                        "Paper trade placement failed",
                        e,
                        component="scan_service",
                        operation="execute_full_scan",
                        symbol=candidate_symbol,
                        mode=mode,
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
