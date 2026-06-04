from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Callable
from zoneinfo import ZoneInfo

from core.logging_utils import log_exception, log_info
from storage import get_pending_scan_gate_observations, update_scan_gate_observation_outcome


NY_TZ = ZoneInfo("America/New_York")


def _parse_candle_datetime(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value))
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=NY_TZ)
    return parsed.astimezone(NY_TZ)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _movement_for_window(
    *,
    candles: list[dict[str, Any]],
    observed_at_ny: datetime,
    entry: float,
    direction: str,
    minutes: int,
) -> tuple[float | None, float | None]:
    end_at = observed_at_ny + timedelta(minutes=minutes)
    window_candles = []
    for candle in candles:
        candle_dt = _parse_candle_datetime(candle.get("datetime"))
        if candle_dt is None:
            continue
        if observed_at_ny < candle_dt <= end_at:
            window_candles.append(candle)
    if not window_candles or entry <= 0:
        return None, None

    highs = [_to_float(candle.get("high"), 0.0) for candle in window_candles]
    lows = [_to_float(candle.get("low"), 0.0) for candle in window_candles]
    highs = [value for value in highs if value > 0]
    lows = [value for value in lows if value > 0]
    if not highs or not lows:
        return None, None

    normalized_direction = str(direction or "").strip().upper()
    if normalized_direction == "SELL":
        favorable = max(0.0, entry - min(lows))
        adverse = max(0.0, max(highs) - entry)
    else:
        favorable = max(0.0, max(highs) - entry)
        adverse = max(0.0, entry - min(lows))
    return round(favorable, 6), round(adverse, 6)


def refresh_scan_gate_observation_outcomes(
    *,
    fetch_intraday_fn: Callable[..., list[dict[str, Any]]],
    limit: int = 100,
) -> dict[str, Any]:
    rows = get_pending_scan_gate_observations(limit=limit)
    updated_count = 0
    skipped_count = 0
    error_count = 0
    samples: list[dict[str, Any]] = []

    for row in rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol:
            skipped_count += 1
            continue
        try:
            candles = fetch_intraday_fn(symbol, outputsize=390)
            observed_at = row.get("observed_at")
            if isinstance(observed_at, datetime):
                observed_at_ny = observed_at.astimezone(NY_TZ)
            else:
                observed_at_ny = datetime.fromisoformat(str(observed_at)).astimezone(NY_TZ)
            entry = _to_float(row.get("entry"), 0.0)
            direction = str(row.get("direction", "")).strip().upper()
            values: dict[int, tuple[float | None, float | None]] = {
                horizon: _movement_for_window(
                    candles=candles,
                    observed_at_ny=observed_at_ny,
                    entry=entry,
                    direction=direction,
                    minutes=horizon,
                )
                for horizon in (30, 60, 120)
            }
            status = "COMPLETE" if values[120][0] is not None or values[120][1] is not None else "INSUFFICIENT_CANDLES"
            update_scan_gate_observation_outcome(
                observation_id=int(row["id"]),
                max_favorable_30m=values[30][0],
                max_adverse_30m=values[30][1],
                max_favorable_60m=values[60][0],
                max_adverse_60m=values[60][1],
                max_favorable_120m=values[120][0],
                max_adverse_120m=values[120][1],
                outcome_status=status,
            )
            updated_count += 1
            if len(samples) < 5:
                samples.append({"id": row["id"], "symbol": symbol, "status": status})
        except Exception as exc:
            error_count += 1
            log_exception(
                "Failed to refresh scan gate observation outcome",
                exc,
                component="scan_observation_service",
                operation="refresh_scan_gate_observation_outcomes",
                symbol=symbol,
                observation_id=row.get("id"),
            )

    result = {
        "ok": error_count == 0,
        "requested_count": len(rows),
        "updated_count": updated_count,
        "skipped_count": skipped_count,
        "error_count": error_count,
        "samples": samples,
    }
    log_info(
        "Refreshed scan gate observation outcomes",
        component="scan_observation_service",
        operation="refresh_scan_gate_observation_outcomes",
        **result,
    )
    return result
