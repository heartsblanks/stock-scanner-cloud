from __future__ import annotations

from datetime import datetime
from typing import Optional

from core.db import execute, fetch_all, fetch_one
from repositories.common import normalize_text


def insert_scan_run(
    scan_time: datetime,
    mode: str,
    scan_source: Optional[str] = None,
    market_phase: Optional[str] = None,
    candidate_count: Optional[int] = None,
    placed_count: Optional[int] = None,
    skipped_count: Optional[int] = None,
) -> None:
    existing = fetch_one(
        """
        SELECT id
        FROM scan_runs
        WHERE scan_time = %(scan_time)s
          AND COALESCE(mode, '') = %(mode)s
          AND COALESCE(scan_source, '') = %(scan_source)s
          AND COALESCE(market_phase, '') = %(market_phase)s
          AND COALESCE(candidate_count, -1) = %(candidate_count_match)s
          AND COALESCE(placed_count, -1) = %(placed_count_match)s
          AND COALESCE(skipped_count, -1) = %(skipped_count_match)s
        LIMIT 1
        """,
        {
            "scan_time": scan_time,
            "mode": normalize_text(mode),
            "scan_source": normalize_text(scan_source),
            "market_phase": normalize_text(market_phase),
            "candidate_count_match": candidate_count if candidate_count is not None else -1,
            "placed_count_match": placed_count if placed_count is not None else -1,
            "skipped_count_match": skipped_count if skipped_count is not None else -1,
        },
    )
    if existing:
        return

    execute(
        """
        INSERT INTO scan_runs (
            scan_time,
            mode,
            scan_source,
            market_phase,
            candidate_count,
            placed_count,
            skipped_count
        )
        VALUES (
            %(scan_time)s,
            %(mode)s,
            %(scan_source)s,
            %(market_phase)s,
            %(candidate_count)s,
            %(placed_count)s,
            %(skipped_count)s
        )
        """,
        {
            "scan_time": scan_time,
            "mode": mode,
            "scan_source": scan_source,
            "market_phase": market_phase,
            "candidate_count": candidate_count,
            "placed_count": placed_count,
            "skipped_count": skipped_count,
        },
    )


def insert_signal_log(
    *,
    timestamp_utc: datetime,
    scan_id: Optional[str] = None,
    scan_source: Optional[str] = None,
    market_phase: Optional[str] = None,
    scan_execution_time_ms: Optional[int] = None,
    mode: Optional[str] = None,
    account_size: Optional[float] = None,
    current_open_positions: Optional[int] = None,
    current_open_exposure: Optional[float] = None,
    timing_ok: Optional[bool] = None,
    source: Optional[str] = None,
    trade_count: Optional[int] = None,
    top_name: Optional[str] = None,
    top_symbol: Optional[str] = None,
    current_price: Optional[float] = None,
    entry: Optional[float] = None,
    stop: Optional[float] = None,
    target: Optional[float] = None,
    shares: Optional[float] = None,
    confidence: Optional[float] = None,
    reason: Optional[str] = None,
    benchmark_sp500: Optional[float] = None,
    benchmark_nasdaq: Optional[float] = None,
    paper_trade_enabled: Optional[bool] = None,
    paper_trade_candidate_count: Optional[int] = None,
    paper_trade_long_candidate_count: Optional[int] = None,
    paper_trade_short_candidate_count: Optional[int] = None,
    paper_trade_placed_count: Optional[int] = None,
    paper_trade_placed_long_count: Optional[int] = None,
    paper_trade_placed_short_count: Optional[int] = None,
    paper_candidate_symbols: Optional[str] = None,
    paper_candidate_confidences: Optional[str] = None,
    paper_skipped_symbols: Optional[str] = None,
    paper_skip_reasons: Optional[str] = None,
    paper_placed_symbols: Optional[str] = None,
    paper_trade_ids: Optional[str] = None,
) -> None:
    existing = fetch_one(
        """
        SELECT id
        FROM signal_logs
        WHERE timestamp_utc = %(timestamp_utc)s
          AND COALESCE(scan_id, '') = %(scan_id)s
          AND COALESCE(mode, '') = %(mode)s
          AND COALESCE(top_symbol, '') = %(top_symbol)s
          AND COALESCE(source, '') = %(source)s
        LIMIT 1
        """,
        {
            "timestamp_utc": timestamp_utc,
            "scan_id": normalize_text(scan_id),
            "mode": normalize_text(mode),
            "top_symbol": normalize_text(top_symbol).upper(),
            "source": normalize_text(source),
        },
    )
    if existing:
        return

    execute(
        """
        INSERT INTO signal_logs (
            timestamp_utc,
            scan_id,
            scan_source,
            market_phase,
            scan_execution_time_ms,
            mode,
            account_size,
            current_open_positions,
            current_open_exposure,
            timing_ok,
            source,
            trade_count,
            top_name,
            top_symbol,
            current_price,
            entry,
            stop,
            target,
            shares,
            confidence,
            reason,
            benchmark_sp500,
            benchmark_nasdaq,
            paper_trade_enabled,
            paper_trade_candidate_count,
            paper_trade_long_candidate_count,
            paper_trade_short_candidate_count,
            paper_trade_placed_count,
            paper_trade_placed_long_count,
            paper_trade_placed_short_count,
            paper_candidate_symbols,
            paper_candidate_confidences,
            paper_skipped_symbols,
            paper_skip_reasons,
            paper_placed_symbols,
            paper_trade_ids
        )
        VALUES (
            %(timestamp_utc)s,
            %(scan_id)s,
            %(scan_source)s,
            %(market_phase)s,
            %(scan_execution_time_ms)s,
            %(mode)s,
            %(account_size)s,
            %(current_open_positions)s,
            %(current_open_exposure)s,
            %(timing_ok)s,
            %(source)s,
            %(trade_count)s,
            %(top_name)s,
            %(top_symbol)s,
            %(current_price)s,
            %(entry)s,
            %(stop)s,
            %(target)s,
            %(shares)s,
            %(confidence)s,
            %(reason)s,
            %(benchmark_sp500)s,
            %(benchmark_nasdaq)s,
            %(paper_trade_enabled)s,
            %(paper_trade_candidate_count)s,
            %(paper_trade_long_candidate_count)s,
            %(paper_trade_short_candidate_count)s,
            %(paper_trade_placed_count)s,
            %(paper_trade_placed_long_count)s,
            %(paper_trade_placed_short_count)s,
            %(paper_candidate_symbols)s,
            %(paper_candidate_confidences)s,
            %(paper_skipped_symbols)s,
            %(paper_skip_reasons)s,
            %(paper_placed_symbols)s,
            %(paper_trade_ids)s
        )
        """,
        {
            "timestamp_utc": timestamp_utc,
            "scan_id": scan_id,
            "scan_source": scan_source,
            "market_phase": market_phase,
            "scan_execution_time_ms": scan_execution_time_ms,
            "mode": mode,
            "account_size": account_size,
            "current_open_positions": current_open_positions,
            "current_open_exposure": current_open_exposure,
            "timing_ok": timing_ok,
            "source": source,
            "trade_count": trade_count,
            "top_name": top_name,
            "top_symbol": normalize_text(top_symbol).upper() or None,
            "current_price": current_price,
            "entry": entry,
            "stop": stop,
            "target": target,
            "shares": shares,
            "confidence": confidence,
            "reason": reason,
            "benchmark_sp500": benchmark_sp500,
            "benchmark_nasdaq": benchmark_nasdaq,
            "paper_trade_enabled": paper_trade_enabled,
            "paper_trade_candidate_count": paper_trade_candidate_count,
            "paper_trade_long_candidate_count": paper_trade_long_candidate_count,
            "paper_trade_short_candidate_count": paper_trade_short_candidate_count,
            "paper_trade_placed_count": paper_trade_placed_count,
            "paper_trade_placed_long_count": paper_trade_placed_long_count,
            "paper_trade_placed_short_count": paper_trade_placed_short_count,
            "paper_candidate_symbols": paper_candidate_symbols,
            "paper_candidate_confidences": paper_candidate_confidences,
            "paper_skipped_symbols": paper_skipped_symbols,
            "paper_skip_reasons": paper_skip_reasons,
            "paper_placed_symbols": paper_placed_symbols,
            "paper_trade_ids": paper_trade_ids,
        },
    )


def insert_paper_trade_attempt(
    *,
    timestamp_utc: datetime,
    scan_id: Optional[str] = None,
    mode: Optional[str] = None,
    scan_source: Optional[str] = None,
    market_phase: Optional[str] = None,
    symbol: str,
    decision_stage: str,
    final_reason: Optional[str] = None,
    direction: Optional[str] = None,
    entry: Optional[float] = None,
    stop: Optional[float] = None,
    target: Optional[float] = None,
    confidence: Optional[float] = None,
    account_size: Optional[float] = None,
    current_open_positions: Optional[int] = None,
    current_open_exposure: Optional[float] = None,
    remaining_slots: Optional[int] = None,
    effective_remaining_slots: Optional[int] = None,
    remaining_allocatable_capital: Optional[float] = None,
    per_trade_notional: Optional[float] = None,
    adjusted_per_trade_notional: Optional[float] = None,
    shares: Optional[float] = None,
    cash_affordable_shares: Optional[int] = None,
    notional_capped_shares: Optional[int] = None,
    confidence_multiplier: Optional[float] = None,
    loss_multiplier: Optional[float] = None,
    final_multiplier: Optional[float] = None,
    placed: Optional[bool] = None,
    broker_order_id: Optional[str] = None,
    broker_parent_order_id: Optional[str] = None,
    broker_rejection_reason: Optional[str] = None,
) -> None:
    execute(
        """
        INSERT INTO paper_trade_attempts (
            timestamp_utc, scan_id, mode, scan_source, market_phase, symbol, decision_stage,
            final_reason, direction, entry, stop, target, confidence, account_size,
            current_open_positions, current_open_exposure, remaining_slots, effective_remaining_slots,
            remaining_allocatable_capital, per_trade_notional, adjusted_per_trade_notional, shares,
            cash_affordable_shares, notional_capped_shares, confidence_multiplier, loss_multiplier,
            final_multiplier, placed, broker_order_id, broker_parent_order_id, broker_rejection_reason
        )
        VALUES (
            %(timestamp_utc)s, %(scan_id)s, %(mode)s, %(scan_source)s, %(market_phase)s, %(symbol)s, %(decision_stage)s,
            %(final_reason)s, %(direction)s, %(entry)s, %(stop)s, %(target)s, %(confidence)s, %(account_size)s,
            %(current_open_positions)s, %(current_open_exposure)s, %(remaining_slots)s, %(effective_remaining_slots)s,
            %(remaining_allocatable_capital)s, %(per_trade_notional)s, %(adjusted_per_trade_notional)s, %(shares)s,
            %(cash_affordable_shares)s, %(notional_capped_shares)s, %(confidence_multiplier)s, %(loss_multiplier)s,
            %(final_multiplier)s, %(placed)s, %(broker_order_id)s, %(broker_parent_order_id)s, %(broker_rejection_reason)s
        )
        """,
        {
            "timestamp_utc": timestamp_utc,
            "scan_id": normalize_text(scan_id) or None,
            "mode": normalize_text(mode) or None,
            "scan_source": normalize_text(scan_source) or None,
            "market_phase": normalize_text(market_phase) or None,
            "symbol": normalize_text(symbol).upper(),
            "decision_stage": normalize_text(decision_stage).upper(),
            "final_reason": normalize_text(final_reason) or None,
            "direction": normalize_text(direction).upper() or None,
            "entry": entry,
            "stop": stop,
            "target": target,
            "confidence": confidence,
            "account_size": account_size,
            "current_open_positions": current_open_positions,
            "current_open_exposure": current_open_exposure,
            "remaining_slots": remaining_slots,
            "effective_remaining_slots": effective_remaining_slots,
            "remaining_allocatable_capital": remaining_allocatable_capital,
            "per_trade_notional": per_trade_notional,
            "adjusted_per_trade_notional": adjusted_per_trade_notional,
            "shares": shares,
            "cash_affordable_shares": cash_affordable_shares,
            "notional_capped_shares": notional_capped_shares,
            "confidence_multiplier": confidence_multiplier,
            "loss_multiplier": loss_multiplier,
            "final_multiplier": final_multiplier,
            "placed": placed,
            "broker_order_id": normalize_text(broker_order_id) or None,
            "broker_parent_order_id": normalize_text(broker_parent_order_id) or None,
            "broker_rejection_reason": normalize_text(broker_rejection_reason) or None,
        },
    )


def get_recent_scan_runs(limit: int = 100) -> list[dict]:
    return fetch_all("SELECT * FROM scan_runs ORDER BY scan_time DESC, id DESC LIMIT %(limit)s", {"limit": limit})


def get_signal_log_rows(limit: int = 5000) -> list[dict]:
    return fetch_all(
        """
        SELECT
            timestamp_utc, scan_id, scan_source, market_phase, scan_execution_time_ms, mode, account_size,
            current_open_positions, current_open_exposure, timing_ok, source, trade_count, top_name, top_symbol,
            current_price, entry, stop, target, shares, confidence, reason, benchmark_sp500, benchmark_nasdaq,
            paper_trade_enabled, paper_trade_candidate_count, paper_trade_long_candidate_count,
            paper_trade_short_candidate_count, paper_trade_placed_count, paper_trade_placed_long_count,
            paper_trade_placed_short_count, paper_candidate_symbols, paper_candidate_confidences,
            paper_skipped_symbols, paper_skip_reasons, paper_placed_symbols, paper_trade_ids
        FROM signal_logs
        ORDER BY timestamp_utc ASC, id ASC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )


def get_recent_paper_trade_attempts(limit: int = 100, decision_stage: Optional[str] = None) -> list[dict]:
    if decision_stage:
        return fetch_all(
            """
            SELECT *
            FROM paper_trade_attempts
            WHERE UPPER(COALESCE(decision_stage, '')) = %(decision_stage)s
            ORDER BY timestamp_utc DESC, id DESC
            LIMIT %(limit)s
            """,
            {"decision_stage": normalize_text(decision_stage).upper(), "limit": limit},
        )
    return fetch_all("SELECT * FROM paper_trade_attempts ORDER BY timestamp_utc DESC, id DESC LIMIT %(limit)s", {"limit": limit})


def get_recent_paper_trade_rejections(limit: int = 100) -> list[dict]:
    return fetch_all(
        """
        SELECT *
        FROM paper_trade_attempts
        WHERE UPPER(COALESCE(decision_stage, '')) IN ('SCAN_REJECTED', 'REFRESH_REJECTED', 'PLACEMENT_SKIPPED', 'PLACEMENT_REJECTED')
        ORDER BY timestamp_utc DESC, id DESC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )


def get_paper_trade_attempt_stage_counts(limit_days: int = 7) -> list[dict]:
    return fetch_all(
        """
        SELECT COALESCE(decision_stage, '') AS decision_stage, COUNT(*)::INT AS count
        FROM paper_trade_attempts
        WHERE timestamp_utc >= NOW() - (%(limit_days)s::text || ' days')::interval
        GROUP BY COALESCE(decision_stage, '')
        ORDER BY count DESC, decision_stage ASC
        """,
        {"limit_days": max(1, limit_days)},
    )


def get_paper_trade_attempt_reason_counts(limit_days: int = 7, limit: int = 10) -> list[dict]:
    return fetch_all(
        """
        SELECT COALESCE(final_reason, '') AS final_reason, COUNT(*)::INT AS count
        FROM paper_trade_attempts
        WHERE timestamp_utc >= NOW() - (%(limit_days)s::text || ' days')::interval
          AND COALESCE(final_reason, '') <> ''
        GROUP BY COALESCE(final_reason, '')
        ORDER BY count DESC, final_reason ASC
        LIMIT %(limit)s
        """,
        {"limit_days": max(1, limit_days), "limit": max(1, limit)},
    )


def get_paper_trade_attempt_daily_summary(limit_days: int = 7) -> list[dict]:
    return fetch_all(
        """
        SELECT timestamp_utc::date AS trade_date, COALESCE(decision_stage, '') AS decision_stage, COUNT(*)::INT AS count
        FROM paper_trade_attempts
        WHERE timestamp_utc >= NOW() - (%(limit_days)s::text || ' days')::interval
        GROUP BY timestamp_utc::date, COALESCE(decision_stage, '')
        ORDER BY trade_date DESC, decision_stage ASC
        """,
        {"limit_days": max(1, limit_days)},
    )


def get_paper_trade_attempt_hourly_summary(limit_days: int = 7) -> list[dict]:
    return fetch_all(
        """
        WITH base AS (
            SELECT
                EXTRACT(HOUR FROM (timestamp_utc AT TIME ZONE 'America/New_York'))::INT AS hour_ny,
                COALESCE(decision_stage, '') AS decision_stage,
                COALESCE(final_reason, '') AS final_reason
            FROM paper_trade_attempts
            WHERE timestamp_utc >= NOW() - (%(limit_days)s::text || ' days')::interval
        ),
        reason_ranked AS (
            SELECT
                hour_ny,
                final_reason,
                COUNT(*)::INT AS reason_count,
                ROW_NUMBER() OVER (
                    PARTITION BY hour_ny
                    ORDER BY COUNT(*) DESC, final_reason ASC
                ) AS rn
            FROM base
            WHERE final_reason <> ''
              AND decision_stage IN ('SCAN_REJECTED', 'REFRESH_REJECTED', 'PLACEMENT_SKIPPED', 'PLACEMENT_REJECTED')
            GROUP BY hour_ny, final_reason
        )
        SELECT
            base.hour_ny,
            COUNT(*)::INT AS total_attempts,
            COUNT(*) FILTER (WHERE base.decision_stage = 'PLACED')::INT AS placed_count,
            COUNT(*) FILTER (WHERE base.decision_stage = 'SCAN_REJECTED')::INT AS scan_rejected_count,
            COUNT(*) FILTER (WHERE base.decision_stage = 'REFRESH_REJECTED')::INT AS refresh_rejected_count,
            COUNT(*) FILTER (WHERE base.decision_stage = 'PLACEMENT_SKIPPED')::INT AS placement_skipped_count,
            COUNT(*) FILTER (WHERE base.decision_stage = 'PLACEMENT_REJECTED')::INT AS placement_rejected_count,
            COUNT(*) FILTER (WHERE base.decision_stage = 'PAPER_CANDIDATE')::INT AS candidate_count,
            (
                COUNT(*) FILTER (WHERE base.decision_stage = 'PLACED')
                + COUNT(*) FILTER (
                    WHERE base.decision_stage IN ('SCAN_REJECTED', 'REFRESH_REJECTED', 'PLACEMENT_SKIPPED', 'PLACEMENT_REJECTED')
                )
            )::INT AS resolved_attempts,
            CASE
                WHEN (
                    COUNT(*) FILTER (WHERE base.decision_stage = 'PLACED')
                    + COUNT(*) FILTER (
                        WHERE base.decision_stage IN ('SCAN_REJECTED', 'REFRESH_REJECTED', 'PLACEMENT_SKIPPED', 'PLACEMENT_REJECTED')
                    )
                ) > 0
                THEN ROUND(
                    (
                        COUNT(*) FILTER (WHERE base.decision_stage = 'PLACED')::NUMERIC
                        / (
                            COUNT(*) FILTER (WHERE base.decision_stage = 'PLACED')
                            + COUNT(*) FILTER (
                                WHERE base.decision_stage IN ('SCAN_REJECTED', 'REFRESH_REJECTED', 'PLACEMENT_SKIPPED', 'PLACEMENT_REJECTED')
                            )
                        )::NUMERIC
                    ) * 100,
                    1
                )
                ELSE NULL
            END AS placement_rate,
            reason_ranked.final_reason AS top_non_placement_reason,
            reason_ranked.reason_count AS top_non_placement_reason_count
        FROM base
        LEFT JOIN reason_ranked
          ON reason_ranked.hour_ny = base.hour_ny
         AND reason_ranked.rn = 1
        GROUP BY base.hour_ny, reason_ranked.final_reason, reason_ranked.reason_count
        ORDER BY base.hour_ny ASC
        """,
        {"limit_days": max(1, limit_days)},
    )


def get_latest_scan_run() -> Optional[dict]:
    return fetch_one("SELECT * FROM scan_runs ORDER BY scan_time DESC, id DESC LIMIT 1", {})


def get_latest_scan_summary() -> dict:
    latest_scan = get_latest_scan_run()
    count_row = fetch_one("SELECT COUNT(*)::INT AS count FROM scan_runs", {})
    return {"latest_scan": latest_scan, "scan_runs_count": int(count_row["count"]) if count_row else 0}
