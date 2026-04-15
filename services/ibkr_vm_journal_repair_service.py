from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from scripts.repair_ibkr_stale_closes_from_vm_logs import (
    LifecycleRow,
    apply_repair,
    build_repair_candidates,
    parse_vm_journal,
)


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_upper(value: Any) -> str:
    return _normalize_text(value).upper()


def _lifecycle_row_from_dict(row: dict[str, Any]) -> LifecycleRow:
    return LifecycleRow(
        trade_key=_normalize_text(row.get("trade_key")),
        symbol=_normalize_upper(row.get("symbol")),
        mode=_normalize_text(row.get("mode")),
        side=_normalize_upper(row.get("side")),
        direction=_normalize_upper(row.get("direction")),
        status=_normalize_upper(row.get("status")),
        entry_time=row.get("entry_time"),
        entry_price=_to_float(row.get("entry_price")),
        exit_time=row.get("exit_time"),
        exit_price=_to_float(row.get("exit_price")),
        stop_price=_to_float(row.get("stop_price")),
        target_price=_to_float(row.get("target_price")),
        exit_reason=_normalize_text(row.get("exit_reason")) or None,
        shares=_to_float(row.get("shares")),
        realized_pnl=_to_float(row.get("realized_pnl")),
        realized_pnl_percent=_to_float(row.get("realized_pnl_percent")),
        signal_timestamp=row.get("signal_timestamp"),
        signal_entry=_to_float(row.get("signal_entry")),
        signal_stop=_to_float(row.get("signal_stop")),
        signal_target=_to_float(row.get("signal_target")),
        signal_confidence=_to_float(row.get("signal_confidence")),
        broker=_normalize_upper(row.get("broker")),
        order_id=_normalize_text(row.get("order_id")),
        parent_order_id=_normalize_text(row.get("parent_order_id")),
        exit_order_id=_normalize_text(row.get("exit_order_id")) or None,
    )


def _default_since_until(target_date: str) -> tuple[str, str]:
    start = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=UTC)
    end = start + timedelta(days=1)
    return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")


def repair_ibkr_stale_closes_from_bridge_journal(
    *,
    target_date: str,
    get_stale_ibkr_closed_trade_lifecycles: Callable[..., list[dict[str, Any]]],
    fetch_bridge_journal_lines: Callable[..., list[str]],
    since: str | None = None,
    until: str | None = None,
    year: int | None = None,
    apply_changes: bool = True,
) -> dict[str, Any]:
    stale_rows = get_stale_ibkr_closed_trade_lifecycles(target_date=target_date, limit=200)
    lifecycle_rows = [_lifecycle_row_from_dict(row) for row in stale_rows]
    rows_by_trade_key = {row.trade_key: row for row in lifecycle_rows}

    resolved_since, resolved_until = since or "", until or ""
    if not resolved_since or not resolved_until:
        resolved_since, resolved_until = _default_since_until(target_date)

    resolved_year = int(year or int(target_date.split("-")[0]))
    lines = fetch_bridge_journal_lines(since=resolved_since, until=resolved_until) or []
    executions, portfolios = parse_vm_journal(lines, year=resolved_year)
    candidates = build_repair_candidates(lifecycle_rows, executions, portfolios)
    repaired = apply_repair(rows_by_trade_key, candidates, dry_run=not apply_changes)

    return {
        "ok": True,
        "target_date": target_date,
        "since": resolved_since,
        "until": resolved_until,
        "year": resolved_year,
        "line_count": len(lines),
        "stale_row_count": len(lifecycle_rows),
        "candidate_count": len(candidates),
        "applied_count": len(repaired),
        "dry_run": not apply_changes,
        "repaired": repaired,
    }
