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
                    upsert_market_data_candles(
                        broker="IBKR",
                        symbol=symbol,
                        interval="1min",
                        candles=list(candles),
                        fetched_at=now,
                        source="ibkr_mcp_connector",
                    )
                    stored.append(symbol)
                except Exception as e:
                    failed.append({"symbol": symbol, "reason": str(e)[:200]})

            return jsonify({
                "ok": len(failed) == 0,
                "stored_count": len(stored),
                "failed_count": len(failed),
                "stored": stored,
                "failed": failed,
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
                "account_size": account_size,
                "open_positions": open_positions,
                "open_exposure": open_exposure,
                "candidate_count": len(candidates),
                "returned_count": len(top_candidates),
                "max_candidates": max_candidates,
                "candidates": top_candidates,
                "scan_summary": {
                    "fetch_ok": fetch_ok,
                    "fetch_fail": fetch_fail,
                    "evaluated_count": len(evaluations),
                    "valid_count": len(valid_trades),
                },
            })

        except Exception as e:
            log_exception("internal-scan failed", e, route="/internal/scan")
            return jsonify({"ok": False, "error": str(e)}), 500
