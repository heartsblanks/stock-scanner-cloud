from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable


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
    run_scan: Callable[[float, str], Any],
    trade_to_dict: Callable[[Any], dict[str, Any]],
    debug_to_dict: Callable[[Any], dict[str, Any]],
    paper_candidate_from_evaluation: Callable[[Any], Any],
    get_latest_open_paper_trade_for_symbol: Callable[[str], Any],
    is_symbol_in_paper_cooldown: Callable[[str, str], tuple[bool, str]],
    place_paper_bracket_order_from_trade: Callable[[Any], dict[str, Any]],
    append_trade_log: Callable[[dict[str, Any]], None],
    safe_insert_trade_event: Callable[..., None],
    safe_insert_broker_order: Callable[..., None],
    to_float_or_none: Callable[[Any], float | None],
    MIN_CONFIDENCE: float,
) -> dict[str, Any] | tuple[dict[str, Any], int]:
    account_size = payload.get("account_size")
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

    if account_size is None:
        return {"ok": False, "error": "account_size is required"}, 400

    try:
        account_size = float(account_size)
    except (TypeError, ValueError):
        return {"ok": False, "error": "account_size must be numeric"}, 400

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
            print(f"Signal log write failed: {e}", flush=True)
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

    paper_candidate_confidences = ",".join(
        str(t["metrics"].get("final_confidence", "")) for t in paper_trades if t["metrics"].get("symbol", "")
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
        "scan_source": scan_source,
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
            skipped_symbols = []
            skip_reasons = []
            placed_trade_ids = []

            for paper_trade_candidate in paper_trades:
                paper_metrics = paper_trade_candidate["metrics"]
                candidate_symbol = str(paper_metrics.get("symbol", "")).strip().upper()

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
                    else:
                        skipped_symbols.append(candidate_symbol)
                        skip_reasons.append(f"{candidate_symbol}:{paper_trade_result.get('reason', 'not_placed')}")
                except Exception as e:
                    print(f"Paper trade placement failed: {e}", flush=True)
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
                "candidate_count": len(paper_trades),
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
            "paper_candidate_confidences": paper_candidate_confidences,
            "paper_skipped_symbols": paper_skipped_symbols,
            "paper_skip_reasons": paper_skip_reasons,
            "paper_placed_symbols": paper_placed_symbols,
            "paper_trade_ids": paper_trade_ids,
        })
    except Exception as e:
        print(f"Signal log write failed: {e}", flush=True)
    safe_insert_scan_run(
        scan_time=parse_iso_utc(timestamp_utc),
        mode=mode,
        scan_source=scan_source,
        market_phase=market_phase,
        candidate_count=len(paper_trades),
        placed_count=paper_trade_placed_count,
        skipped_count=len([s for s in paper_skipped_symbols.split(",") if s.strip()]),
    )
    return response