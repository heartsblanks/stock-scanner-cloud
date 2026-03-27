from datetime import datetime, timezone

from flask import jsonify, request


def _normalize_trade_key(symbol: str, broker_parent_order_id: str, broker_order_id: str) -> str:
    return broker_parent_order_id or broker_order_id or symbol


def _infer_direction_from_prices(entry_price, exit_price, stop_price, target_price) -> str | None:
    entry_val = to_float_fallback(entry_price)
    stop_val = to_float_fallback(stop_price)
    target_val = to_float_fallback(target_price)
    exit_val = to_float_fallback(exit_price)

    if entry_val is not None and target_val is not None and stop_val is not None:
        if target_val > entry_val and stop_val < entry_val:
            return "LONG"
        if target_val < entry_val and stop_val > entry_val:
            return "SHORT"

    if entry_val is not None and exit_val is not None:
        if exit_val > entry_val:
            return "LONG"
        if exit_val < entry_val:
            return "SHORT"

    return None


def _compute_realized_pnl(entry_price, exit_price, shares, direction):
    entry_val = to_float_fallback(entry_price)
    exit_val = to_float_fallback(exit_price)
    shares_val = to_float_fallback(shares)
    direction_val = str(direction or "").strip().upper()

    if entry_val is None or exit_val is None or shares_val is None:
        return None

    if direction_val == "LONG":
        return round((exit_val - entry_val) * shares_val, 6)
    if direction_val == "SHORT":
        return round((entry_val - exit_val) * shares_val, 6)
    return None


def _compute_realized_pnl_percent(entry_price, exit_price, direction):
    entry_val = to_float_fallback(entry_price)
    exit_val = to_float_fallback(exit_price)
    direction_val = str(direction or "").strip().upper()

    if entry_val in (None, 0) or exit_val is None:
        return None

    if direction_val == "LONG":
        return round(((exit_val - entry_val) / entry_val) * 100.0, 6)
    if direction_val == "SHORT":
        return round(((entry_val - exit_val) / entry_val) * 100.0, 6)
    return None


def _compute_duration_minutes(entry_timestamp_utc: str | None, exit_timestamp_utc: str | None, parse_iso_utc_func):
    if not entry_timestamp_utc or not exit_timestamp_utc:
        return None
    try:
        entry_dt = parse_iso_utc_func(entry_timestamp_utc)
        exit_dt = parse_iso_utc_func(exit_timestamp_utc)
        return round((exit_dt - entry_dt).total_seconds() / 60.0, 2)
    except Exception:
        return None


def to_float_fallback(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None



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
    get_trade_lifecycle_rows,
    get_trade_lifecycle_summary,
    upsert_trade_lifecycle,
):
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
                "error": "event_type must be OPEN, STOP_HIT, TARGET_HIT, or MANUAL_CLOSE",
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
        trade_key = _normalize_trade_key(symbol, broker_parent_order_id, broker_order_id)

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
            direction = _infer_direction_from_prices(entry_price, exit_price, stop_price, target_price)

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
            direction = _infer_direction_from_prices(open_entry_price, exit_price, stop_price, target_price)
            realized_pnl = _compute_realized_pnl(open_entry_price, exit_price, shares, direction)
            realized_pnl_percent = _compute_realized_pnl_percent(open_entry_price, exit_price, direction)
            duration_minutes = _compute_duration_minutes(open_timestamp_utc, timestamp_utc, parse_iso_utc)

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
            open_timestamp_utc = open_row.get("timestamp_utc", "") if open_row else ""
            open_entry_price = open_row.get("entry_price", "") if open_row else ""
            direction = _infer_direction_from_prices(open_entry_price, exit_price, stop_price, target_price)
            realized_pnl = _compute_realized_pnl(open_entry_price, exit_price, shares, direction)
            realized_pnl_percent = _compute_realized_pnl_percent(open_entry_price, exit_price, direction)
            duration_minutes = _compute_duration_minutes(open_timestamp_utc, timestamp_utc, parse_iso_utc)

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
                    print(f"Inference failed for {symbol}: {e}", flush=True)

            entry_price = ""
            stop_price = linked_signal_stop
            target_price = linked_signal_target
            exit_price = price
            exit_reason = "MANUAL_CLOSE"
            status = "CLOSED"
            open_timestamp_utc = open_row.get("timestamp_utc", "") if open_row else ""
            open_entry_price = open_row.get("entry_price", "") if open_row else ""
            direction = _infer_direction_from_prices(open_entry_price, exit_price, stop_price, target_price)
            realized_pnl = _compute_realized_pnl(open_entry_price, exit_price, shares, direction)
            realized_pnl_percent = _compute_realized_pnl_percent(open_entry_price, exit_price, direction)
            duration_minutes = _compute_duration_minutes(open_timestamp_utc, timestamp_utc, parse_iso_utc)

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
    def close_positions():
        try:
            result = close_all_paper_positions()
            return jsonify({"ok": True, **result})
        except Exception as e:
            print(f"close-paper-positions failed: {e}", flush=True)
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

    @app.get("/open-trades")
    def open_trades():
        try:
            limit = int(request.args.get("limit", 100))
            rows = get_open_trade_events(limit=limit)
            return jsonify({
                "ok": True,
                "count": len(rows),
                "rows": rows,
            })
        except Exception as e:
            print(f"open-trades failed: {e}", flush=True)
            return jsonify({"ok": False, "error": str(e)}), 500


    @app.get("/closed-trades")
    def closed_trades():
        try:
            limit = int(request.args.get("limit", 100))
            rows = get_closed_trade_events(limit=limit)
            return jsonify({
                "ok": True,
                "count": len(rows),
                "rows": rows,
            })
        except Exception as e:
            print(f"closed-trades failed: {e}", flush=True)
            return jsonify({"ok": False, "error": str(e)}), 500


    @app.get("/recent-trades")
    def recent_trades():
        try:
            limit = int(request.args.get("limit", 100))
            rows = get_recent_trade_event_rows(limit=limit)
            return jsonify({
                "ok": True,
                "count": len(rows),
                "rows": rows,
            })
        except Exception as e:
            print(f"recent-trades failed: {e}", flush=True)
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
            print(f"latest-scan-summary failed: {e}", flush=True)
            return jsonify({"ok": False, "error": str(e)}), 500


    @app.get("/trade-lifecycle")
    def trade_lifecycle():
        try:
            limit_raw = request.args.get("limit", "100")
            status = request.args.get("status")

            try:
                limit = max(1, min(1000, int(limit_raw)))
            except Exception:
                return jsonify({"ok": False, "error": "limit must be an integer"}), 400

            rows = get_trade_lifecycle_rows(limit=limit, status=status)

            return jsonify({
                "ok": True,
                "count": len(rows),
                "limit": limit,
                "status_filter": status,
                "rows": rows,
            })
        except Exception as e:
            print(f"trade-lifecycle failed: {e}", flush=True)
            return jsonify({"ok": False, "error": str(e)}), 500


    @app.get("/trade-lifecycle-summary")
    def trade_lifecycle_summary():
        try:
            limit_raw = request.args.get("limit", "1000")

            try:
                limit = max(1, min(5000, int(limit_raw)))
            except Exception:
                return jsonify({"ok": False, "error": "limit must be an integer"}), 400

            summary = get_trade_lifecycle_summary(limit=limit)

            return jsonify({
                "ok": True,
                "limit": limit,
                **summary,
            })
        except Exception as e:
            print(f"trade-lifecycle-summary failed: {e}", flush=True)
            return jsonify({"ok": False, "error": str(e)}), 500