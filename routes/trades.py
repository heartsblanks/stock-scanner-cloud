from datetime import datetime, timezone

from flask import jsonify, request
from core.logging_utils import log_exception, log_warning
from core.trade_math import (
    compute_duration_minutes,
    compute_realized_pnl,
    compute_realized_pnl_percent,
    infer_direction,
    normalize_trade_key,
)



def _format_trade_log_time(timestamp_value) -> str:
    if timestamp_value is None:
        return ""

    if isinstance(timestamp_value, datetime):
        try:
            return timestamp_value.astimezone(timezone.utc).strftime("%H:%M")
        except Exception:
            return timestamp_value.strftime("%H:%M")

    text = str(timestamp_value).strip()
    if not text:
        return ""

    if len(text) >= 16 and (text[10] == "T" or text[10] == " "):
        return text[11:16]

    try:
        parsed = datetime.strptime(text, "%a, %d %b %Y %H:%M:%S GMT")
        return parsed.strftime("%H:%M")
    except Exception:
        return text


def register_trade_routes(
    app,
    *,
    append_trade_log,
    safe_insert_trade_event,
    safe_insert_broker_order,
    close_all_paper_positions,
    read_trade_rows_for_date,
    find_instrument_by_symbol,
    find_best_signal_match,
    find_latest_open_trade,
    infer_first_level_hit,
    to_float_or_none,
    parse_iso_utc,
    get_open_trade_events,
    get_closed_trade_events,
    get_recent_trade_event_rows,
    get_latest_scan_summary,
    get_trade_lifecycles,
    get_trade_lifecycle_summary_from_table,
    get_open_positions_for_broker_name=None,
    get_open_orders_for_broker_name=None,
    get_open_state_for_broker_name=None,
    upsert_trade_lifecycle,
):
    def _safe_float(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _enrich_open_trade_rows(rows: list[dict], broker_filter: str | None = None) -> tuple[list[dict], dict]:
        normalized_broker_filter = str(broker_filter or "").strip().upper()
        should_enrich_ibkr = not normalized_broker_filter or normalized_broker_filter == "IBKR"
        if not rows or not should_enrich_ibkr:
            return rows, {"ibkr_live_available": False, "ibkr_live_error": None}

        ibkr_positions = []
        ibkr_orders = []
        positions_error = None
        orders_error = None
        if get_open_state_for_broker_name is not None:
            try:
                open_state = get_open_state_for_broker_name("IBKR") or {}
                ibkr_positions = list(open_state.get("positions") or [])
                ibkr_orders = list(open_state.get("orders") or [])
            except Exception as exc:
                positions_error = str(exc)
                orders_error = str(exc)
                log_warning("IBKR live open-state enrichment failed", route="/open-trades", error=str(exc))
        else:
            if get_open_positions_for_broker_name is None or get_open_orders_for_broker_name is None:
                return rows, {"ibkr_live_available": False, "ibkr_live_error": None}
            try:
                ibkr_positions = get_open_positions_for_broker_name("IBKR") or []
            except Exception as exc:
                positions_error = str(exc)
                log_warning("IBKR live position enrichment failed", route="/open-trades", error=positions_error)
            try:
                ibkr_orders = get_open_orders_for_broker_name("IBKR") or []
            except Exception as exc:
                orders_error = str(exc)
                log_warning("IBKR live order enrichment failed", route="/open-trades", error=orders_error)

        ibkr_live_available = not (positions_error and orders_error)
        ibkr_live_error = "; ".join(error for error in (positions_error, orders_error) if error) or None

        positions_by_symbol = {
            str(position.get("symbol", "")).strip().upper(): position
            for position in ibkr_positions
            if str(position.get("symbol", "")).strip()
        }
        orders_by_symbol: dict[str, list[dict]] = {}
        for order in ibkr_orders:
            symbol = str(order.get("symbol", "")).strip().upper()
            if not symbol:
                continue
            orders_by_symbol.setdefault(symbol, []).append(order)

        enriched_rows: list[dict] = []
        for row in rows:
            broker_name = str(row.get("broker", "") or "ALPACA").strip().upper() or "ALPACA"
            if broker_name != "IBKR":
                enriched_rows.append(row)
                continue

            enriched_row = dict(row)
            symbol = str(row.get("symbol", "")).strip().upper()
            parent_order_id = str(row.get("parent_order_id", "") or "").strip()
            direction = str(row.get("direction", "") or "").strip().upper()
            side = str(row.get("side", "") or "").strip().upper()
            is_long = direction == "LONG" or side == "BUY"
            expected_exit_side = "sell" if is_long else "buy"

            position = positions_by_symbol.get(symbol)
            symbol_orders = list(orders_by_symbol.get(symbol, []))
            matched_orders = [
                order
                for order in symbol_orders
                if parent_order_id and str(order.get("parent_id", "")).strip() == parent_order_id
            ] or symbol_orders
            exit_orders = [
                order
                for order in matched_orders
                if str(order.get("side", "")).strip().lower() == expected_exit_side
            ] or matched_orders

            live_target_order = next(
                (order for order in exit_orders if _safe_float(order.get("limit_price")) not in (None, 0.0)),
                None,
            )
            live_stop_order = next(
                (order for order in exit_orders if _safe_float(order.get("stop_price")) not in (None, 0.0)),
                None,
            )

            stored_entry_price = _safe_float(row.get("entry_price"))
            stored_stop_price = _safe_float(row.get("stop_price"))
            stored_target_price = _safe_float(row.get("target_price"))
            stored_shares = _safe_float(row.get("shares"))
            live_entry_price = _safe_float((position or {}).get("avg_entry_price"))
            live_current_price = _safe_float((position or {}).get("current_price"))
            live_market_value = _safe_float((position or {}).get("market_value"))
            live_unrealized_pl = _safe_float((position or {}).get("unrealized_pl"))
            live_shares = _safe_float((position or {}).get("qty"))
            live_target_price = _safe_float((live_target_order or {}).get("limit_price"))
            live_stop_price = _safe_float((live_stop_order or {}).get("stop_price"))

            enriched_row.update({
                "stored_entry_price": stored_entry_price,
                "stored_stop_price": stored_stop_price,
                "stored_target_price": stored_target_price,
                "stored_shares": stored_shares,
                "live_entry_price": live_entry_price,
                "live_current_price": live_current_price,
                "live_market_value": live_market_value,
                "live_unrealized_pl": live_unrealized_pl,
                "live_shares": live_shares,
                "live_target_price": live_target_price,
                "live_stop_price": live_stop_price,
                "live_target_order_id": str((live_target_order or {}).get("id", "")).strip(),
                "live_target_status": str((live_target_order or {}).get("status", "")).strip(),
                "live_stop_order_id": str((live_stop_order or {}).get("id", "")).strip(),
                "live_stop_status": str((live_stop_order or {}).get("status", "")).strip(),
                "live_position_detected": position is not None,
                "live_orders_detected": len(exit_orders),
                "live_parent_match": bool(parent_order_id) and any(str(order.get("parent_id", "")).strip() == parent_order_id for order in symbol_orders),
                "live_sync_available": ibkr_live_available,
                "live_positions_available": positions_error is None,
                "live_orders_available": orders_error is None,
                "live_positions_error": positions_error,
                "live_orders_error": orders_error,
                "live_sync_error": ibkr_live_error,
                "live_source": "ibkr_positions_and_open_orders" if ibkr_live_available else "trade_lifecycle_only",
                "entry_price_mismatch": (
                    stored_entry_price is not None and live_entry_price is not None and abs(stored_entry_price - live_entry_price) >= 0.005
                ),
                "stop_price_mismatch": (
                    stored_stop_price is not None and live_stop_price is not None and abs(stored_stop_price - live_stop_price) >= 0.005
                ),
                "target_price_mismatch": (
                    stored_target_price is not None and live_target_price is not None and abs(stored_target_price - live_target_price) >= 0.005
                ),
                "shares_mismatch": (
                    stored_shares is not None and live_shares is not None and abs(stored_shares - live_shares) >= 0.5
                ),
                "position_cost": live_market_value if live_market_value is not None else row.get("position_cost"),
            })
            if live_entry_price is not None:
                enriched_row["entry_price"] = live_entry_price
            enriched_rows.append(enriched_row)

        return enriched_rows, {
            "ibkr_live_available": ibkr_live_available,
            "ibkr_live_error": ibkr_live_error,
        }

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

        if event_type not in {"OPEN", "STOP_HIT", "TARGET_HIT", "MANUAL_CLOSE", "EOD_CLOSE"}:
            return jsonify({
                "ok": False,
                "error": "event_type must be OPEN, STOP_HIT, TARGET_HIT, MANUAL_CLOSE, or EOD_CLOSE",
            }), 400
        if trade_source not in {"MANUAL", "ALPACA_PAPER"}:
            return jsonify({
                "ok": False,
                "error": "trade_source must be MANUAL or ALPACA_PAPER",
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
        trade_key = normalize_trade_key(symbol, broker_parent_order_id, broker_order_id)

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
            direction = infer_direction(entry_price, exit_price, stop_price, target_price)

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
            open_timestamp_utc = open_row.get("timestamp_utc", "") if open_row else ""
            open_entry_price = open_row.get("entry_price", "") if open_row else ""
            direction = infer_direction(open_entry_price, exit_price, stop_price, target_price)
            realized_pnl = compute_realized_pnl(open_entry_price, exit_price, shares, direction)
            realized_pnl_percent = compute_realized_pnl_percent(open_entry_price, exit_price, direction)
            duration_minutes = compute_duration_minutes(
                parse_iso_utc(open_timestamp_utc) if open_timestamp_utc else None,
                parse_iso_utc(timestamp_utc),
            )

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
                    log_warning("Inference failed", route="/log-trade", symbol=symbol, event_type=event_type, error=str(e))

            entry_price = ""
            stop_price = linked_signal_stop
            target_price = linked_signal_target
            exit_price = price
            exit_reason = "TARGET_HIT"
            status = "CLOSED"
            open_timestamp_utc = open_row.get("timestamp_utc", "") if open_row else ""
            open_entry_price = open_row.get("entry_price", "") if open_row else ""
            direction = infer_direction(open_entry_price, exit_price, stop_price, target_price)
            realized_pnl = compute_realized_pnl(open_entry_price, exit_price, shares, direction)
            realized_pnl_percent = compute_realized_pnl_percent(open_entry_price, exit_price, direction)
            duration_minutes = compute_duration_minutes(
                parse_iso_utc(open_timestamp_utc) if open_timestamp_utc else None,
                parse_iso_utc(timestamp_utc),
            )

        else:
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
                    log_warning("Inference failed", route="/log-trade", symbol=symbol, event_type=event_type, error=str(e))

            entry_price = ""
            stop_price = linked_signal_stop
            target_price = linked_signal_target
            exit_price = price
            exit_reason = "EOD_CLOSE" if event_type == "EOD_CLOSE" else "MANUAL_CLOSE"
            status = "CLOSED"
            open_timestamp_utc = open_row.get("timestamp_utc", "") if open_row else ""
            open_entry_price = open_row.get("entry_price", "") if open_row else ""
            direction = infer_direction(open_entry_price, exit_price, stop_price, target_price)
            realized_pnl = compute_realized_pnl(open_entry_price, exit_price, shares, direction)
            realized_pnl_percent = compute_realized_pnl_percent(open_entry_price, exit_price, direction)
            duration_minutes = compute_duration_minutes(
                parse_iso_utc(open_timestamp_utc) if open_timestamp_utc else None,
                parse_iso_utc(timestamp_utc),
            )

        if event_type == "OPEN":
            realized_pnl = None
            realized_pnl_percent = None
            duration_minutes = None
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
            safe_insert_trade_event(
                event_time=parse_iso_utc(timestamp_utc),
                event_type=event_type,
                symbol=symbol,
                side=None,
                shares=to_float_or_none(shares),
                price=to_float_or_none(price),
                mode=inferred_mode,
                order_id=broker_order_id,
                parent_order_id=broker_parent_order_id,
                status=status,
            )
            if broker_order_id:
                safe_insert_broker_order(
                    order_id=broker_order_id,
                    symbol=symbol,
                    side=None,
                    order_type=event_type.lower(),
                    status=broker_status,
                    qty=to_float_or_none(shares),
                    filled_qty=to_float_or_none(broker_filled_qty),
                    avg_fill_price=to_float_or_none(broker_filled_avg_price),
                    submitted_at=parse_iso_utc(timestamp_utc),
                    filled_at=parse_iso_utc(timestamp_utc) if status == "CLOSED" else None,
                )
            if event_type == "OPEN":
                upsert_trade_lifecycle(
                    trade_key=trade_key,
                    symbol=symbol,
                    mode=inferred_mode,
                    side=None,
                    direction=direction,
                    status="OPEN",
                    entry_time=parse_iso_utc(timestamp_utc),
                    entry_price=to_float_or_none(entry_price),
                    exit_time=None,
                    exit_price=None,
                    stop_price=to_float_or_none(stop_price),
                    target_price=to_float_or_none(target_price),
                    exit_reason=None,
                    shares=to_float_or_none(shares),
                    realized_pnl=None,
                    realized_pnl_percent=None,
                    duration_minutes=None,
                    signal_timestamp=parse_iso_utc(linked_signal_timestamp_utc) if linked_signal_timestamp_utc else None,
                    signal_entry=to_float_or_none(linked_signal_entry),
                    signal_stop=to_float_or_none(linked_signal_stop),
                    signal_target=to_float_or_none(linked_signal_target),
                    signal_confidence=to_float_or_none(linked_signal_confidence),
                    order_id=broker_order_id,
                    parent_order_id=broker_parent_order_id,
                    exit_order_id=None,
                )
            else:
                upsert_trade_lifecycle(
                    trade_key=trade_key,
                    symbol=symbol,
                    mode=inferred_mode,
                    side=None,
                    direction=direction,
                    status="CLOSED",
                    entry_time=parse_iso_utc(open_timestamp_utc) if open_timestamp_utc else None,
                    entry_price=to_float_or_none(open_entry_price),
                    exit_time=parse_iso_utc(timestamp_utc),
                    exit_price=to_float_or_none(exit_price),
                    stop_price=to_float_or_none(stop_price),
                    target_price=to_float_or_none(target_price),
                    exit_reason=exit_reason,
                    shares=to_float_or_none(shares),
                    realized_pnl=realized_pnl,
                    realized_pnl_percent=realized_pnl_percent,
                    duration_minutes=duration_minutes,
                    signal_timestamp=parse_iso_utc(linked_signal_timestamp_utc) if linked_signal_timestamp_utc else None,
                    signal_entry=to_float_or_none(linked_signal_entry),
                    signal_stop=to_float_or_none(linked_signal_stop),
                    signal_target=to_float_or_none(linked_signal_target),
                    signal_confidence=to_float_or_none(linked_signal_confidence),
                    order_id=broker_order_id,
                    parent_order_id=broker_parent_order_id,
                    exit_order_id=broker_exit_order_id,
                )
        except Exception as e:
            log_exception("Trade log write failed", e, route="/log-trade", symbol=symbol, event_type=event_type)
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
    def close_positions():
        try:
            result = close_all_paper_positions()
            return jsonify({"ok": True, **result})
        except Exception as e:
            log_exception("close-paper-positions failed", e, route="/close-paper-positions")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/read-trades-by-date")
    def read_trades_by_date():
        payload = request.get_json(silent=True) or {}
        target_date = str(payload.get("date", "")).strip()

        if not target_date:
            return jsonify({"ok": False, "error": "date is required in YYYY-MM-DD format"}), 400

        try:
            rows = read_trade_rows_for_date(target_date)
        except Exception as e:
            log_exception("Trade log read failed", e, route="/read-trades-by-date", target_date=target_date)
            return jsonify({"ok": False, "error": f"trade log read failed: {e}"}), 500

        formatted_lines = [f"Trade Log for {target_date}"]

        if not rows:
            formatted_lines.append("")
            formatted_lines.append("No trade events found.")
        else:
            for row in rows:
                time_part = _format_trade_log_time(row.get("timestamp_utc", ""))
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

    @app.get("/open-trades")
    def open_trades():
        try:
            limit = int(request.args.get("limit", 100))
            broker = request.args.get("broker")
            rows = get_trade_lifecycles(limit=limit, status="OPEN", broker=broker)
            enriched_rows, enrichment = _enrich_open_trade_rows(rows, broker_filter=broker)
            return jsonify({
                "ok": True,
                "count": len(enriched_rows),
                "broker_filter": broker,
                "rows": enriched_rows,
                **enrichment,
            })
        except Exception as e:
            log_exception("open-trades failed", e, route="/open-trades")
            return jsonify({"ok": False, "error": str(e)}), 500


    @app.get("/closed-trades")
    def closed_trades():
        try:
            limit = int(request.args.get("limit", 100))
            broker = request.args.get("broker")
            rows = get_trade_lifecycles(limit=limit, status="CLOSED", broker=broker)
            return jsonify({
                "ok": True,
                "count": len(rows),
                "broker_filter": broker,
                "rows": rows,
            })
        except Exception as e:
            log_exception("closed-trades failed", e, route="/closed-trades")
            return jsonify({"ok": False, "error": str(e)}), 500


    @app.get("/recent-trades")
    def recent_trades():
        try:
            limit = int(request.args.get("limit", 100))
            broker = request.args.get("broker")
            rows = get_recent_trade_event_rows(limit=limit, broker=broker)
            return jsonify({
                "ok": True,
                "count": len(rows),
                "broker_filter": broker,
                "rows": rows,
            })
        except Exception as e:
            log_exception("recent-trades failed", e, route="/recent-trades")
            return jsonify({"ok": False, "error": str(e)}), 500


    @app.get("/latest-scan-summary")
    def latest_scan_summary():
        try:
            summary = get_latest_scan_summary()
            return jsonify({
                "ok": True,
                **summary,
            })
        except Exception as e:
            log_exception("latest-scan-summary failed", e, route="/latest-scan-summary")
            return jsonify({"ok": False, "error": str(e)}), 500


    @app.get("/trade-lifecycle")
    def trade_lifecycle():
        try:
            limit_raw = request.args.get("limit", "100")
            status = request.args.get("status")
            broker = request.args.get("broker")

            try:
                limit = max(1, min(1000, int(limit_raw)))
            except Exception:
                return jsonify({"ok": False, "error": "limit must be an integer"}), 400

            rows = get_trade_lifecycles(limit=limit, status=status, broker=broker)

            return jsonify({
                "ok": True,
                "count": len(rows),
                "limit": limit,
                "status_filter": status,
                "broker_filter": broker,
                "rows": rows,
            })
        except Exception as e:
            log_exception("trade-lifecycle failed", e, route="/trade-lifecycle")
            return jsonify({"ok": False, "error": str(e)}), 500


    @app.get("/trade-lifecycle-summary")
    def trade_lifecycle_summary():
        try:
            limit_raw = request.args.get("limit", "1000")
            broker = request.args.get("broker")

            try:
                limit = max(1, min(5000, int(limit_raw)))
            except Exception:
                return jsonify({"ok": False, "error": "limit must be an integer"}), 400

            summary = get_trade_lifecycle_summary_from_table(limit=limit, broker=broker)

            return jsonify({
                "ok": True,
                "limit": limit,
                "broker_filter": broker,
                **summary,
            })
        except Exception as e:
            log_exception("trade-lifecycle-summary failed", e, route="/trade-lifecycle-summary")
            return jsonify({"ok": False, "error": str(e)}), 500
