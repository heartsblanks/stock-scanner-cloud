from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from typing import Any, Callable
from zoneinfo import ZoneInfo

from analytics.instruments import get_instrument_groups, sync_quality_candidate_instruments
from core.logging_utils import log_exception, log_info, log_warning
from repositories.trades_repo import get_latest_symbol_ranking_rows
from repositories.symbol_eligibility_repo import (
    get_current_symbol_session_eligibility_rows,
    get_latest_symbol_session_eligibility_rows,
    replace_symbol_session_eligibility_rows,
)


NY_TZ = ZoneInfo("America/New_York")


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _configured_notional_cap() -> float:
    cap = _to_float(
        os.getenv("SYMBOL_ELIGIBILITY_MAX_NOTIONAL"),
        _to_float(os.getenv("PAPER_MAX_NOTIONAL"), 250.0),
    )
    return cap if cap > 0 else 250.0


def _allow_non_usd_symbols() -> bool:
    raw = str(os.getenv("ALLOW_NON_USD_SYMBOLS", "false")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _symbol_eligibility_max_symbols_per_mode() -> int:
    try:
        value = int(os.getenv("SYMBOL_ELIGIBILITY_MAX_SYMBOLS_PER_MODE", "6"))
    except Exception:
        value = 6
    return max(1, value)


def _symbol_ranking_window_days() -> int:
    try:
        value = int(os.getenv("SYMBOL_RANKING_WINDOW_DAYS", "5"))
    except Exception:
        value = 5
    return max(1, value)


def _symbol_ranking_broker() -> str:
    return str(os.getenv("SYMBOL_RANKING_BROKER", "IBKR")).strip().upper() or "IBKR"


def _next_nyse_trading_day(reference_date: date) -> date:
    import pandas_market_calendars as mcal

    nyse = mcal.get_calendar("NYSE")
    start = reference_date + timedelta(days=1)
    end = reference_date + timedelta(days=14)
    schedule = nyse.schedule(start_date=start.isoformat(), end_date=end.isoformat())
    if schedule.empty:
        return start
    return schedule.index[0].date()


def _extract_last_close(candles: list[dict[str, Any]]) -> float | None:
    if not isinstance(candles, list):
        return None
    for candle in reversed(candles):
        if not isinstance(candle, dict):
            continue
        close = _to_float(candle.get("close"), float("nan"))
        if close == close and close > 0:  # NaN-safe check
            return float(close)
    return None


def _priority_by_symbol(instruments: dict[str, dict[str, Any]]) -> dict[str, int]:
    priority_map: dict[str, int] = {}
    for info in instruments.values():
        symbol = str(info.get("symbol", "")).strip().upper()
        if symbol:
            try:
                priority_map[symbol] = int(info.get("priority") or 0)
            except Exception:
                priority_map[symbol] = 0
    return priority_map


def _apply_ranking_filter_to_mode_rows(
    *,
    mode: str,
    rows: list[dict[str, Any]],
    instruments: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    max_symbols = _symbol_eligibility_max_symbols_per_mode()
    priority_map = _priority_by_symbol(instruments)
    ranking_rows: list[dict[str, Any]] = []
    ranking_by_symbol: dict[str, dict[str, Any]] = {}
    ranking_available = False

    try:
        ranking_rows = get_latest_symbol_ranking_rows(
            broker=_symbol_ranking_broker(),
            window_days=_symbol_ranking_window_days(),
            mode=mode,
        )
        ranking_by_symbol = {
            str(row.get("symbol", "")).strip().upper(): row
            for row in ranking_rows
            if str(row.get("symbol", "")).strip()
        }
        ranking_available = bool(ranking_by_symbol)
    except Exception as exc:
        log_warning(
            "Symbol ranking unavailable; falling back to priority ordering",
            component="symbol_eligibility_service",
            operation="refresh_symbol_eligibility_for_date",
            mode=mode,
            error=str(exc),
        )

    demoted_count = 0
    ranked_below_count = 0
    candidates: list[dict[str, Any]] = []

    for row in rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol or not bool(row.get("eligible")):
            continue

        ranking = ranking_by_symbol.get(symbol)
        if ranking and bool(ranking.get("demoted")):
            row["eligible"] = False
            row["ineligible_reason"] = "symbol_rank_demoted"
            demoted_count += 1
            continue

        if ranking:
            try:
                rank_value = int(ranking.get("rank") or 999999)
            except Exception:
                rank_value = 999999
            score = _to_float(ranking.get("score"), 0.0)
            candidates.append(
                {
                    "symbol": symbol,
                    "rank": rank_value,
                    "score": score,
                    "priority": priority_map.get(symbol, 0),
                    "ranking_available": True,
                }
            )
        else:
            candidates.append(
                {
                    "symbol": symbol,
                    "rank": 999999,
                    "score": 0.0,
                    "priority": priority_map.get(symbol, 0),
                    "ranking_available": False,
                }
            )

    candidates.sort(
        key=lambda item: (
            int(item["rank"]),
            -float(item["score"]),
            -int(item["priority"]),
            str(item["symbol"]),
        )
        if ranking_available
        else (
            -int(item["priority"]),
            str(item["symbol"]),
        )
    )
    allowed_symbols = {str(item["symbol"]) for item in candidates[:max_symbols]}

    for row in rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        if symbol and bool(row.get("eligible")) and symbol not in allowed_symbols:
            row["eligible"] = False
            row["ineligible_reason"] = "ranked_below_live_allowlist"
            ranked_below_count += 1

    return {
        "mode": mode,
        "max_symbols": max_symbols,
        "ranking_available": ranking_available,
        "ranking_row_count": len(ranking_rows),
        "allowed_symbols": sorted(allowed_symbols),
        "demoted_count": demoted_count,
        "ranked_below_count": ranked_below_count,
    }


def refresh_symbol_eligibility_for_date(
    *,
    target_session_date: str,
    fetch_intraday_fn: Callable[..., list[dict[str, Any]]],
    modes: list[str] | None = None,
    source: str = "ibkr_intraday",
) -> dict[str, Any]:
    notional_cap = _configured_notional_cap()
    allow_non_usd = _allow_non_usd_symbols()
    catalog_sync_result: dict[str, Any] | None = None
    try:
        catalog_sync_result = sync_quality_candidate_instruments()
    except Exception as exc:
        log_warning(
            "Quality candidate catalog sync failed; continuing with current DB catalog",
            component="symbol_eligibility_service",
            operation="refresh_symbol_eligibility_for_date",
            error=str(exc),
        )

    instrument_groups = get_instrument_groups(force_refresh=True)
    selected_modes = [str(mode).strip().lower() for mode in (modes or list(instrument_groups.keys())) if str(mode).strip()]

    mode_summaries: list[dict[str, Any]] = []
    total_symbols = 0
    total_eligible = 0
    total_ineligible = 0

    for mode in selected_modes:
        instruments = instrument_groups.get(mode) or {}
        rows: list[dict[str, Any]] = []
        mode_total = 0
        mode_eligible = 0
        mode_ineligible = 0
        mode_above_cap = 0
        mode_non_usd = 0
        mode_errors = 0

        for display_name, info in instruments.items():
            mode_total += 1
            symbol = str(info.get("symbol", "")).strip().upper()
            if not symbol:
                continue

            currency = str(info.get("currency", "") or "USD").strip().upper() or "USD"
            exchange = str(info.get("exchange", "")).strip().upper() or None
            primary_exchange = str(info.get("primary_exchange", "")).strip().upper() or None

            eligible = False
            ineligible_reason: str | None = None
            last_price: float | None = None
            price_timestamp: datetime | None = None

            if currency != "USD" and not allow_non_usd:
                ineligible_reason = "non_usd_symbol_excluded"
                mode_non_usd += 1
            else:
                try:
                    candles = fetch_intraday_fn(
                        symbol,
                        exchange=exchange,
                        primary_exchange=primary_exchange,
                        currency=currency,
                    )
                    last_price = _extract_last_close(candles or [])
                    if last_price is None:
                        ineligible_reason = "price_unavailable"
                    elif last_price > notional_cap:
                        ineligible_reason = f"price_above_cap_{notional_cap:.2f}"
                        mode_above_cap += 1
                    else:
                        eligible = True
                        ineligible_reason = None
                        price_timestamp = datetime.now(NY_TZ)
                except Exception as exc:
                    ineligible_reason = "price_fetch_error"
                    mode_errors += 1
                    log_exception(
                        "Failed to refresh symbol eligibility price",
                        exc,
                        component="symbol_eligibility_service",
                        operation="refresh_symbol_eligibility_for_date",
                        mode=mode,
                        symbol=symbol,
                    )

            if eligible:
                mode_eligible += 1
            else:
                mode_ineligible += 1

            rows.append(
                {
                    "symbol": symbol,
                    "display_name": display_name,
                    "currency": currency,
                    "last_price": last_price,
                    "max_notional": notional_cap,
                    "eligible": eligible,
                    "ineligible_reason": ineligible_reason,
                    "source": source,
                    "price_timestamp": price_timestamp,
                }
            )

        ranking_filter = _apply_ranking_filter_to_mode_rows(
            mode=mode,
            rows=rows,
            instruments=instruments,
        )
        mode_eligible = sum(1 for row in rows if bool(row.get("eligible")))
        mode_ineligible = mode_total - mode_eligible

        replace_symbol_session_eligibility_rows(
            session_date=target_session_date,
            mode=mode,
            rows=rows,
        )

        total_symbols += mode_total
        total_eligible += mode_eligible
        total_ineligible += mode_ineligible
        mode_summaries.append(
            {
                "mode": mode,
                "symbol_count": mode_total,
                "eligible_count": mode_eligible,
                "ineligible_count": mode_ineligible,
                "above_cap_count": mode_above_cap,
                "non_usd_excluded_count": mode_non_usd,
                "price_error_count": mode_errors,
                "ranking_filter": ranking_filter,
            }
        )

    result = {
        "ok": True,
        "target_session_date": target_session_date,
        "max_notional": notional_cap,
        "allow_non_usd_symbols": allow_non_usd,
        "mode_count": len(selected_modes),
        "symbol_count": total_symbols,
        "eligible_count": total_eligible,
        "ineligible_count": total_ineligible,
        "modes": mode_summaries,
        "catalog_sync": catalog_sync_result,
    }
    log_info(
        "Refreshed symbol session eligibility",
        component="symbol_eligibility_service",
        operation="refresh_symbol_eligibility_for_date",
        **result,
    )
    return result


def refresh_symbol_eligibility_for_next_session(
    *,
    now_ny: datetime | None = None,
    fetch_intraday_fn: Callable[..., list[dict[str, Any]]] | None = None,
    modes: list[str] | None = None,
) -> dict[str, Any]:
    if fetch_intraday_fn is None:
        from orchestration.runtime_context import fetch_ibkr_intraday

        fetch_intraday_fn = fetch_ibkr_intraday

    current_ny = now_ny.astimezone(NY_TZ) if now_ny is not None else datetime.now(NY_TZ)
    target_session_date = _next_nyse_trading_day(current_ny.date()).isoformat()
    return refresh_symbol_eligibility_for_date(
        target_session_date=target_session_date,
        fetch_intraday_fn=fetch_intraday_fn,
        modes=modes,
        source="ibkr_intraday",
    )


def resolve_session_symbol_allowlist(
    *,
    mode: str,
    now_ny: datetime | None = None,
) -> dict[str, Any]:
    normalized_mode = str(mode or "").strip().lower()
    if not normalized_mode:
        return {
            "filter_applied": False,
            "reason": "missing_mode",
            "allowed_symbols": None,
        }

    current_ny = now_ny.astimezone(NY_TZ) if now_ny is not None else datetime.now(NY_TZ)
    requested_session_date = current_ny.date().isoformat()
    try:
        rows = get_current_symbol_session_eligibility_rows(mode=normalized_mode)
        if not rows:
            rows = get_latest_symbol_session_eligibility_rows(
                mode=normalized_mode,
                on_or_before_date=requested_session_date,
            )

        if not rows:
            return {
                "filter_applied": False,
                "reason": "no_symbol_session_eligibility_rows",
                "mode": normalized_mode,
                "requested_session_date": requested_session_date,
                "allowed_symbols": None,
            }

        source_date = str(rows[0].get("session_date") or requested_session_date)
        allowed_symbols = sorted(
            {
                str(row.get("symbol", "")).strip().upper()
                for row in rows
                if bool(row.get("eligible"))
                and str(row.get("symbol", "")).strip()
            }
        )
        excluded_symbols = sorted(
            {
                str(row.get("symbol", "")).strip().upper()
                for row in rows
                if (not bool(row.get("eligible")))
                and str(row.get("symbol", "")).strip()
            }
        )

        return {
            "filter_applied": True,
            "mode": normalized_mode,
            "requested_session_date": requested_session_date,
            "source_session_date": source_date,
            "fallback_used": False,
            "symbol_count": len(rows),
            "allowed_count": len(allowed_symbols),
            "excluded_count": len(excluded_symbols),
            "allowed_symbols": allowed_symbols,
            "excluded_symbols": excluded_symbols,
        }
    except Exception as exc:
        log_warning(
            "Failed to resolve symbol session allowlist",
            component="symbol_eligibility_service",
            operation="resolve_session_symbol_allowlist",
            mode=normalized_mode,
            requested_session_date=requested_session_date,
            error=str(exc),
        )
        return {
            "filter_applied": False,
            "mode": normalized_mode,
            "requested_session_date": requested_session_date,
            "reason": "allowlist_lookup_failed",
            "error": str(exc),
            "allowed_symbols": None,
        }
