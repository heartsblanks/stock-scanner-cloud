"""
Internal endpoints called by the Claude market-ops agent.
These are not public API endpoints — they are called server-to-agent only.
"""
from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

from flask import jsonify, request
from core.logging_utils import log_exception


def _min_confidence() -> int:
    return int(os.getenv("IBKR_PAPER_TRADE_MIN_CONFIDENCE", os.getenv("PAPER_TRADE_MIN_CONFIDENCE", "70")))


def _max_candidates() -> int:
    return int(os.getenv("INTERNAL_SCAN_MAX_CANDIDATES", "3"))


def register_internal_routes(app) -> None:

    @app.post("/internal/cache-candles")
    def cache_candles():
        """
        Accept candle data fetched by the Claude agent via IBKR MCP connector
        and write it to the market_data_candles Neon table.

        Body: { "entries": [{ "symbol": str, "candles": [...], "contract_id": int|null }] }
        """
        try:
            from repositories.market_data_cache_repo import upsert_market_data_candles

            body = request.get_json(silent=True) or {}
            entries = body.get("entries") or []

            if not entries:
                return jsonify({"ok": False, "error": "entries required"}), 400

            stored: list[str] = []
            failed: list[dict] = []
            now = datetime.now(UTC)

            for entry in entries:
                symbol = str(entry.get("symbol") or "").strip().upper()
                candles = entry.get("candles") or []
                if not symbol:
                    failed.append({"symbol": symbol, "reason": "missing_symbol"})
                    continue
                if not candles:
                    failed.append({"symbol": symbol, "reason": "no_candles"})
                    continue
                try:
                    contract_id = entry.get("contract_id")
                    upsert_market_data_candles(
                        broker="IBKR",
                        symbol=symbol,
                        interval="1min",
                        candles=list(candles),
                        fetched_at=now,
                        source="ibkr_mcp_connector",
                        ibkr_contract_id=int(contract_id) if contract_id else None,
                    )
                    stored.append(symbol)
                except Exception as e:
                    failed.append({"symbol": symbol, "reason": str(e)[:200]})

            return jsonify({
                "ok": len(failed) == 0,
                "stored": len(stored),
                "failed": len(failed),
                "errors": failed[:3],
            })
        except Exception as e:
            log_exception("cache-candles failed", e, route="/internal/cache-candles")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/internal/scan")
    def internal_scan():
        """
        Run the scan using candles already in market_data_candles (Neon cache).
        Returns ranked candidates ready for order instruction creation.

        Body: {
          "account_size": float,
          "open_positions": int,
          "open_exposure": float,
          "mode": str,           # optional — scans all scheduled modes if omitted
          "max_candidates": int  # optional — defaults to INTERNAL_SCAN_MAX_CANDIDATES env var
        }
        """
        try:
            from analytics.trade_scan import run_scan
            from orchestration.scan_context import (
                scheduled_round_robin_mode,
                paper_candidate_from_evaluation as build_paper_candidate,
            )
            from repositories.market_data_cache_repo import get_market_data_candles
            body = request.get_json(silent=True) or {}
            account_size = float(body.get("account_size") or 50000)
            open_positions = int(body.get("open_positions") or 0)
            open_exposure = float(body.get("open_exposure") or 0.0)
            mode = str(body.get("mode") or "").strip().lower() or None
            max_candidates = int(body.get("max_candidates") or _max_candidates())
            min_confidence = _min_confidence()

            # Use round-robin mode if not specified
            if not mode:
                mode = scheduled_round_robin_mode()
            if not mode:
                return jsonify({"ok": False, "error": "no_mode_available"}), 400

            # Fetch allowed symbols from DB eligibility list
            try:
                from services.symbol_eligibility_service import resolve_session_symbol_allowlist
                allowlist_row = resolve_session_symbol_allowlist(mode=mode)
                allowed_symbols = allowlist_row.get("allowed_symbols") if allowlist_row else None
            except Exception:
                allowed_symbols = None

            # Build a fetch function that reads from Neon cache only (no live IBKR calls)
            def fetch_from_neon_cache(symbol: str, interval: str = "1min", outputsize: int | None = None, **_kwargs) -> list[dict]:
                row = get_market_data_candles(broker="IBKR", symbol=symbol.upper().strip(), interval="1min")
                if not row:
                    return []
                candles = row.get("candles") or []
                if outputsize and outputsize > 0:
                    candles = list(candles)[-int(outputsize):]
                return list(candles)

            # run_scan returns (valid_trades, evaluations, fetch_ok, fetch_fail, benchmark_directions, source_label)
            valid_trades, evaluations, fetch_ok, fetch_fail, _benchmark_directions, _source_label = run_scan(
                account_size=account_size,
                mode=mode,
                current_open_positions=open_positions,
                current_open_exposure=open_exposure,
                allowed_symbols=allowed_symbols,
                fetch_intraday_fn=fetch_from_neon_cache,
            )

            # Extract and rank valid candidates
            candidates: list[dict[str, Any]] = []
            for eval_result in evaluations:
                if eval_result.get("decision") != "VALID":
                    continue
                candidate = build_paper_candidate(eval_result, min_confidence)
                if candidate:
                    candidates.append(candidate)

            # Sort by confidence descending, cap at max_candidates
            candidates.sort(key=lambda c: float(c.get("confidence") or 0), reverse=True)
            top_candidates = candidates[:max_candidates]

            return jsonify({
                "ok": True,
                "mode": mode,
                "candidate_count": len(candidates),
                "returned_count": len(top_candidates),
                "candidates": top_candidates,
                "scan_summary": {
                    "evaluated_count": len(evaluations),
                    "valid_count": len(valid_trades),
                    "fetch_ok_count": len(fetch_ok),
                    "fetch_fail_count": len(fetch_fail),
                    "fetch_fail": fetch_fail[:5],
                },
            })

        except Exception as e:
            log_exception("internal-scan failed", e, route="/internal/scan")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.get("/internal/market-ops-context")
    def market_ops_context():
        """
        Returns everything the Claude market-ops agent needs at the start of a tick:
        - All scan symbols with known IBKR contract_ids (from cache)
        - Open positions from Neon (trade_lifecycles)
        - NYSE trading day status
        - Current ET time and whether it's EOD tick
        """
        try:
            from analytics.instruments import get_instrument_groups
            from analytics.trade_scan import holiday_and_early_close_status
            from repositories.market_data_cache_repo import get_known_contract_ids
            from repositories.trades_repo import get_trade_lifecycles
            from zoneinfo import ZoneInfo
            from datetime import datetime

            ny_tz = ZoneInfo("America/New_York")
            now_ny = datetime.now(ny_tz)

            # NYSE trading day check
            is_trading_day, is_early_close, market_open, market_close, calendar_msg = holiday_and_early_close_status(now_ny)

            # Ranked symbols — top N only to limit candle fetch token cost
            fetch_limit = int(os.getenv("MARKET_OPS_SYMBOL_FETCH_LIMIT", "25"))

            # Get symbol rankings from Neon (sorted by performance)
            from repositories.trades_repo import get_latest_symbol_ranking_rows
            ranked_rows = get_latest_symbol_ranking_rows(broker="IBKR", window_days=5)
            ranked_symbols: list[str] = [
                str(r.get("symbol") or "").strip().upper()
                for r in ranked_rows
                if str(r.get("symbol") or "").strip()
            ]

            # Fall back to full instrument list if rankings empty
            if not ranked_symbols:
                groups = get_instrument_groups()
                seen: set[str] = set()
                for group_instruments in groups.values():
                    for info in group_instruments.values():
                        sym = str(info.get("symbol") or "").strip().upper()
                        if sym and sym not in seen:
                            ranked_symbols.append(sym)
                            seen.add(sym)

            # Cap at fetch_limit
            fetch_symbols = ranked_symbols[:fetch_limit]

            # Known contract_ids from Neon cache
            known_ids = get_known_contract_ids(broker="IBKR")

            # Symbol list with contract_ids
            symbols = [
                {"symbol": sym, "contract_id": known_ids.get(sym)}
                for sym in fetch_symbols
            ]

            # Open positions from Neon
            try:
                lifecycle_rows = get_trade_lifecycles(status="OPEN", limit=50, broker=None)
                open_trades = lifecycle_rows if isinstance(lifecycle_rows, list) else []
            except Exception:
                open_trades = []

            current_time_str = now_ny.strftime("%H:%M")
            is_eod_tick = now_ny.hour == 15 and now_ny.minute == 55

            return jsonify({
                "ok": True,
                "t": current_time_str,
                "trading": is_trading_day,
                "eod": is_eod_tick,
                "symbols": symbols,
                "open_trades": [
                    {
                        "sym": t.get("symbol"),
                        "side": t.get("side"),
                        "entry": t.get("entry_price"),
                        "stop": t.get("stop_price"),
                        "target": t.get("target_price"),
                        "shares": t.get("shares"),
                    }
                    for t in open_trades
                ],
            })

        except Exception as e:
            log_exception("market-ops-context failed", e, route="/internal/market-ops-context")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/internal/record-attempts")
    def record_attempts():
        """Record paper_trade_attempts for candidates the Claude agent evaluated."""
        try:
            from repositories.scans_repo import insert_paper_trade_attempt
            from datetime import datetime, timezone

            body = request.get_json(silent=True) or {}
            candidates = body.get("candidates") or []
            scan_id = body.get("scan_id") or ""
            mode = body.get("mode") or ""
            account_size = float(body.get("account_size") or 0)
            open_positions = int(body.get("open_positions") or 0)
            open_exposure = float(body.get("open_exposure") or 0.0)
            now_utc = datetime.now(timezone.utc)

            stored = 0
            for c in candidates:
                try:
                    insert_paper_trade_attempt(
                        timestamp_utc=now_utc,
                        scan_id=scan_id,
                        mode=mode,
                        scan_source="claude_mcp_agent",
                        symbol=str(c.get("symbol") or ""),
                        decision_stage=str(c.get("decision", "PLACED")),
                        final_reason=str(c.get("final_reason") or "mcp_instruction_created"),
                        direction=str(c.get("side") or ""),
                        entry=float(c["entry_price"]) if c.get("entry_price") else None,
                        stop=float(c["stop_price"]) if c.get("stop_price") else None,
                        target=float(c["target_price"]) if c.get("target_price") else None,
                        confidence=float(c["confidence"]) if c.get("confidence") else None,
                        shares=float(c["shares"]) if c.get("shares") else None,
                        account_size=account_size or None,
                        current_open_positions=open_positions or None,
                        current_open_exposure=open_exposure or None,
                    )
                    stored += 1
                except Exception:
                    pass

            return jsonify({"ok": True, "stored": stored})
        except Exception as e:
            log_exception("record-attempts failed", e, route="/internal/record-attempts")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/internal/send-alert")
    def send_alert():
        """Send a Telegram alert using the backend's bot credentials."""
        try:
            import os
            import requests as _requests

            body = request.get_json(silent=True) or {}
            message = str(body.get("message") or "").strip()
            if not message:
                return jsonify({"ok": False, "error": "message required"}), 400

            bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
            chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

            if not bot_token or not chat_id:
                return jsonify({"ok": False, "reason": "telegram_not_configured"})

            resp = _requests.post(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                json={"chat_id": chat_id, "text": message, "disable_web_page_preview": False},
                timeout=10,
            )
            return jsonify({"ok": resp.ok, "status_code": resp.status_code})
        except Exception as e:
            log_exception("send-alert failed", e, route="/internal/send-alert")
            return jsonify({"ok": False, "error": str(e)}), 500
