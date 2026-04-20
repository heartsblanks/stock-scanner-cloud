from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from typing import Any, Callable
from zoneinfo import ZoneInfo

from analytics.instruments import INSTRUMENT_GROUPS
from core.logging_utils import log_exception, log_info, log_warning
from repositories.symbol_eligibility_repo import (
    get_latest_symbol_session_eligibility_rows,
    get_symbol_session_eligibility_rows,
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


def _fallback_to_latest_enabled() -> bool:
    raw = str(os.getenv("SYMBOL_ELIGIBILITY_FALLBACK_TO_LATEST", "true")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


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


def refresh_symbol_eligibility_for_date(
    *,
    target_session_date: str,
    fetch_intraday_fn: Callable[..., list[dict[str, Any]]],
    modes: list[str] | None = None,
    source: str = "ibkr_intraday",
) -> dict[str, Any]:
    notional_cap = _configured_notional_cap()
    allow_non_usd = _allow_non_usd_symbols()
    selected_modes = [str(mode).strip().lower() for mode in (modes or list(INSTRUMENT_GROUPS.keys())) if str(mode).strip()]

    mode_summaries: list[dict[str, Any]] = []
    total_symbols = 0
    total_eligible = 0
    total_ineligible = 0

    for mode in selected_modes:
        instruments = INSTRUMENT_GROUPS.get(mode) or {}
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
    fallback_used = False

    try:
        rows = get_symbol_session_eligibility_rows(
            session_date=requested_session_date,
            mode=normalized_mode,
        )
        if not rows and _fallback_to_latest_enabled():
            rows = get_latest_symbol_session_eligibility_rows(
                mode=normalized_mode,
                on_or_before_date=requested_session_date,
            )
            fallback_used = bool(rows)

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
            "fallback_used": fallback_used,
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
