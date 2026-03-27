from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable



def execute_close_all_paper_positions(
    *,
    get_open_positions: Callable[[], list[dict[str, Any]]],
    get_managed_open_paper_trades_for_eod_close: Callable[[], list[dict[str, Any]]],
    cancel_open_orders_for_symbol: Callable[[str], list[str]],
    close_position: Callable[[str], dict[str, Any]],
    get_order_by_id: Callable[..., dict[str, Any]],
    safe_insert_broker_order: Callable[..., None],
    to_float_or_none: Callable[[Any], float | None],
    parse_iso_utc: Callable[[str], Any],
) -> dict[str, Any] | tuple[dict[str, Any], int]:
    try:
        positions = get_open_positions()
    except Exception as e:
        print(f"Open position read failed: {e}", flush=True)
        return {"ok": False, "error": f"open position read failed: {e}"}, 500

    open_paper_rows = get_managed_open_paper_trades_for_eod_close()
    open_paper_symbols = {
        str(row.get("symbol", "")).strip().upper()
        for row in open_paper_rows
        if str(row.get("symbol", "")).strip()
    }

    results: list[dict[str, Any]] = []
    closed_count = 0
    skipped_count = 0

    for position in positions:
        symbol = str(position.get("symbol", "")).strip().upper()
        qty = str(position.get("qty", "")).strip()
        side = str(position.get("side", "")).strip().lower()
        current_price = position.get("current_price", "")

        if not symbol:
            skipped_count += 1
            results.append({
                "closed": False,
                "reason": "missing_symbol",
            })
            continue

        if symbol not in open_paper_symbols:
            skipped_count += 1
            results.append({
                "symbol": symbol,
                "closed": False,
                "reason": "not_managed_by_app",
            })
            continue

        try:
            canceled_order_ids = cancel_open_orders_for_symbol(symbol)
        except Exception as e:
            print(f"Paper open-order cancel failed for {symbol}: {e}", flush=True)
            skipped_count += 1
            results.append({
                "symbol": symbol,
                "closed": False,
                "reason": "cancel_open_orders_exception",
                "details": str(e),
            })
            continue

        try:
            close_response = close_position(symbol, cancel_orders=True)
        except Exception as e:
            print(f"Paper position close failed for {symbol}: {e}", flush=True)
            skipped_count += 1
            results.append({
                "symbol": symbol,
                "closed": False,
                "reason": "close_exception",
                "details": str(e),
            })
            continue

        close_order_id = str(close_response.get("id", "")).strip()
        close_order_status = str(close_response.get("status", "")).strip()
        close_filled_avg_price = ""
        close_filled_qty = qty

        if close_order_id:
            try:
                close_order = get_order_by_id(close_order_id, nested=False)
                close_order_status = str(close_order.get("status", close_order_status)).strip()
                close_filled_qty = str(close_order.get("filled_qty", close_filled_qty)).strip()

                close_filled_avg_price_raw = close_order.get("filled_avg_price", "")
                if close_filled_avg_price_raw not in (None, ""):
                    close_filled_avg_price = str(close_filled_avg_price_raw).strip()
            except Exception as order_read_error:
                print(f"Paper close order read failed for {symbol}: {order_read_error}", flush=True)

        timestamp_utc = datetime.now(timezone.utc).isoformat()
        safe_insert_broker_order(
            order_id=close_order_id or f"close-request-{symbol}-{timestamp_utc}",
            symbol=symbol,
            side=side,
            order_type="eod_close_request",
            status=close_order_status,
            qty=to_float_or_none(qty),
            filled_qty=to_float_or_none(close_filled_qty),
            avg_fill_price=to_float_or_none(close_filled_avg_price),
            submitted_at=parse_iso_utc(timestamp_utc),
            filled_at=parse_iso_utc(timestamp_utc) if close_filled_avg_price else None,
        )
        closed_count += 1
        results.append({
            "symbol": symbol,
            "closed": True,
            "qty": qty,
            "side": side,
            "exit_price": close_filled_avg_price if close_filled_avg_price else current_price,
            "close_order_id": close_order_id,
            "close_status": close_order_status,
            "close_filled_qty": close_filled_qty,
            "close_filled_avg_price": close_filled_avg_price,
            "canceled_order_count": len(canceled_order_ids),
        })

    return {
        "position_count": len(positions),
        "closed_count": closed_count,
        "skipped_count": skipped_count,
        "results": results,
    }
