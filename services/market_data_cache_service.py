from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any, Callable

from core.logging_utils import log_exception, log_info
from repositories.market_data_cache_repo import (
    get_market_data_cache_summary,
    get_market_data_candles,
    upsert_market_data_candles,
)


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _normalize_symbols(symbols: Any) -> list[str]:
    if not isinstance(symbols, (list, tuple, set)):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        normalized_symbol = str(symbol or "").strip().upper()
        if normalized_symbol and normalized_symbol not in seen:
            normalized.append(normalized_symbol)
            seen.add(normalized_symbol)
    return normalized


def _cache_max_age_seconds() -> int:
    return max(1, _to_int(os.getenv("PAPER_CANDLE_CACHE_MAX_AGE_SECONDS", "90"), 90))


def _is_fresh(row: dict[str, Any], *, max_age_seconds: int) -> bool:
    fetched_at = row.get("fetched_at")
    if not isinstance(fetched_at, datetime):
        return False
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=UTC)
    return (datetime.now(UTC) - fetched_at).total_seconds() <= max_age_seconds


def fetch_cached_intraday_or_live(
    fetch_live_fn: Callable[..., list[dict[str, Any]]],
    symbol: str,
    interval: str = "1min",
    outputsize: int | None = None,
    *,
    broker: str = "IBKR",
    max_age_seconds: int | None = None,
    refresh_on_miss: bool = True,
    **kwargs: Any,
) -> list[dict[str, Any]]:
    normalized_symbol = str(symbol or "").strip().upper()
    normalized_interval = str(interval or "1min").strip().lower() or "1min"
    effective_max_age = _cache_max_age_seconds() if max_age_seconds is None else max(1, int(max_age_seconds))
    if normalized_symbol:
        try:
            row = get_market_data_candles(
                broker=broker,
                symbol=normalized_symbol,
                interval=normalized_interval,
            )
            if row and _is_fresh(row, max_age_seconds=effective_max_age):
                candles = row.get("candles") or []
                if outputsize and outputsize > 0:
                    candles = list(candles)[-int(outputsize):]
                if candles:
                    return list(candles)
        except Exception as exc:
            log_exception(
                "Candle cache lookup failed; falling back to live fetch",
                exc,
                component="market_data_cache",
                operation="fetch_cached_intraday_or_live",
                symbol=normalized_symbol,
            )

    if not refresh_on_miss:
        return []

    candles = fetch_live_fn(
        normalized_symbol,
        interval=normalized_interval,
        outputsize=outputsize,
        **kwargs,
    )
    if candles:
        try:
            upsert_market_data_candles(
                broker=broker,
                symbol=normalized_symbol,
                interval=normalized_interval,
                candles=list(candles),
                fetched_at=datetime.now(UTC),
                source="ibkr_intraday_live_fallback",
            )
        except Exception as exc:
            log_exception(
                "Candle cache upsert after live fetch failed",
                exc,
                component="market_data_cache",
                operation="fetch_cached_intraday_or_live",
                symbol=normalized_symbol,
            )
    return candles


def refresh_market_data_cache(
    *,
    symbols: list[str] | tuple[str, ...] | set[str],
    fetch_intraday_fn: Callable[..., list[dict[str, Any]]],
    broker: str = "IBKR",
    interval: str = "1min",
    outputsize: int | None = None,
    max_symbols: int | None = None,
) -> dict[str, Any]:
    normalized_symbols = _normalize_symbols(symbols)
    if max_symbols is not None and max_symbols > 0:
        normalized_symbols = normalized_symbols[: int(max_symbols)]
    refreshed: list[str] = []
    failed: list[dict[str, str]] = []
    started_at = datetime.now(UTC)

    for symbol in normalized_symbols:
        try:
            candles = fetch_intraday_fn(symbol, interval=interval, outputsize=outputsize)
            if not candles:
                failed.append({"symbol": symbol, "reason": "no_candles"})
                continue
            upsert_market_data_candles(
                broker=broker,
                symbol=symbol,
                interval=interval,
                candles=list(candles),
                fetched_at=datetime.now(UTC),
                source="ibkr_intraday_refresh",
            )
            refreshed.append(symbol)
        except Exception as exc:
            failed.append({"symbol": symbol, "reason": str(exc)[:200]})

    result = {
        "ok": not failed,
        "requested_count": len(normalized_symbols),
        "refreshed_count": len(refreshed),
        "failed_count": len(failed),
        "refreshed_symbols": refreshed,
        "failed": failed,
        "duration_ms": int((datetime.now(UTC) - started_at).total_seconds() * 1000),
    }
    log_info(
        "Market data candle cache refresh completed",
        component="market_data_cache",
        operation="refresh_market_data_cache",
        **{key: value for key, value in result.items() if key not in {"refreshed_symbols", "failed"}},
    )
    return result


def market_data_cache_summary(*, broker: str = "IBKR", limit: int = 20) -> dict[str, Any]:
    rows = get_market_data_cache_summary(broker=broker, limit=limit)
    return {
        "ok": True,
        "broker": broker,
        "count": len(rows),
        "rows": rows,
    }
