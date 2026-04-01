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
            rows = get_trade_lifecycles(limit=limit, status="OPEN")
            return jsonify({
                "ok": True,
                "count": len(rows),
                "rows": rows,
            })
        except Exception as e:
            log_exception("open-trades failed", e, route="/open-trades")
            return jsonify({"ok": False, "error": str(e)}), 500


    @app.get("/closed-trades")
    def closed_trades():
        try:
            limit = int(request.args.get("limit", 100))
            rows = get_trade_lifecycles(limit=limit, status="CLOSED")
            return jsonify({
                "ok": True,
                "count": len(rows),
                "rows": rows,
            })
        except Exception as e:
            log_exception("closed-trades failed", e, route="/closed-trades")
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

            try:
                limit = max(1, min(1000, int(limit_raw)))
            except Exception:
                return jsonify({"ok": False, "error": "limit must be an integer"}), 400

            rows = get_trade_lifecycles(limit=limit, status=status)

            return jsonify({
                "ok": True,
                "count": len(rows),
                "limit": limit,
                "status_filter": status,
                "rows": rows,
            })
        except Exception as e:
            log_exception("trade-lifecycle failed", e, route="/trade-lifecycle")
            return jsonify({"ok": False, "error": str(e)}), 500


    @app.get("/trade-lifecycle-summary")
    def trade_lifecycle_summary():
        try:
            limit_raw = request.args.get("limit", "1000")

            try:
                limit = max(1, min(5000, int(limit_raw)))
            except Exception:
                return jsonify({"ok": False, "error": "limit must be an integer"}), 400

            summary = get_trade_lifecycle_summary_from_table(limit=limit)

            return jsonify({
                "ok": True,
                "limit": limit,
                **summary,
            })
        except Exception as e:
            log_exception("trade-lifecycle-summary failed", e, route="/trade-lifecycle-summary")
            return jsonify({"ok": False, "error": str(e)}), 500
