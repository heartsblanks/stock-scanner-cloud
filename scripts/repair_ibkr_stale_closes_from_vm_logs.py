from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.db import execute, fetch_all
from core.trade_math import compute_duration_minutes, compute_realized_pnl, compute_realized_pnl_percent
from repositories.broker_repo import insert_broker_order
from storage import upsert_trade_lifecycle


SYMBOL_RE = re.compile(r"symbol='(?P<symbol>[A-Z.]+)'")
COMMISSION_RE = re.compile(
    r"commissionReport: CommissionReport\(execId='(?P<exec_id>[^']+)'.*?realizedPNL=(?P<realized_pnl>-?\d+(?:\.\d+)?)"
)
PORTFOLIO_RE = re.compile(
    r"updatePortfolio: PortfolioItem\(contract=Stock\(.*?symbol='(?P<symbol>[A-Z.]+)'.*?\)"
    r".*?position=(?P<position>-?\d+(?:\.\d+)?)"
    r".*?unrealizedPNL=-?\d+(?:\.\d+)?"
    r".*?realizedPNL=(?P<realized_pnl>-?\d+(?:\.\d+)?)"
)
LINE_TS_RE = re.compile(r"^(?P<month>[A-Z][a-z]{2})\s+(?P<day>\d+)\s+(?P<hour>\d+):(?P<minute>\d+):(?P<second>\d+)")
EXEC_ID_RE = re.compile(r"execId='(?P<value>[^']+)'")
EXEC_TIME_RE = re.compile(
    r"time=datetime\.datetime\((?P<year>\d+), (?P<month>\d+), (?P<day>\d+), (?P<hour>\d+), (?P<minute>\d+), (?P<second>\d+)"
)
EXEC_SIDE_RE = re.compile(r"side='(?P<value>BOT|SLD)'")
EXEC_SHARES_RE = re.compile(r"shares=(?P<value>-?\d+(?:\.\d+)?)")
EXEC_PRICE_RE = re.compile(r"price=(?P<value>-?\d+(?:\.\d+)?)")
EXEC_CLIENT_ID_RE = re.compile(r"clientId=(?P<value>-?\d+)")
EXEC_ORDER_ID_RE = re.compile(r"orderId=(?P<value>-?\d+)")
EXEC_AVG_PRICE_RE = re.compile(r"avgPrice=(?P<value>-?\d+(?:\.\d+)?)")
EXEC_ORDER_REF_RE = re.compile(r"orderRef='(?P<value>[^']*)'")
ORDER_REF_SYMBOL_RE = re.compile(r"scanner-(?P<symbol>[A-Z.]+)-(BUY|SELL)-")

MONTHS = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}


@dataclass
class LifecycleRow:
    trade_key: str
    symbol: str
    mode: str
    side: str
    direction: str
    status: str
    entry_time: datetime | None
    entry_price: float | None
    exit_time: datetime | None
    exit_price: float | None
    stop_price: float | None
    target_price: float | None
    exit_reason: str | None
    shares: float | None
    realized_pnl: float | None
    realized_pnl_percent: float | None
    signal_timestamp: datetime | None
    signal_entry: float | None
    signal_stop: float | None
    signal_target: float | None
    signal_confidence: float | None
    broker: str
    order_id: str
    parent_order_id: str
    exit_order_id: str | None


@dataclass
class ExecutionRecord:
    exec_id: str
    symbol: str
    timestamp: datetime
    side: str
    shares: float
    price: float
    avg_price: float
    client_id: int
    order_id: str
    order_ref: str
    realized_pnl: float | None = None


@dataclass
class PortfolioSnapshot:
    symbol: str
    timestamp: datetime
    position: float
    realized_pnl: float


@dataclass
class RepairCandidate:
    trade_key: str
    symbol: str
    exit_order_id: str | None
    exit_time: datetime
    exit_price: float
    exit_reason: str
    realized_pnl: float | None
    realized_pnl_percent: float | None
    duration_minutes: float | None
    source: str


def _to_float(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _normalize_text(value) -> str:
    return str(value or "").strip()


def _normalize_upper(value) -> str:
    return _normalize_text(value).upper()


def _line_timestamp(line: str, year: int) -> datetime | None:
    match = LINE_TS_RE.search(line)
    if not match:
        return None
    month = MONTHS.get(match.group("month"))
    if month is None:
        return None
    return datetime(
        year,
        month,
        int(match.group("day")),
        int(match.group("hour")),
        int(match.group("minute")),
        int(match.group("second")),
        tzinfo=UTC,
    )


def _parse_execution_line(line: str) -> ExecutionRecord | None:
    if "execDetails Execution(" not in line:
        return None
    exec_id_match = EXEC_ID_RE.search(line)
    time_match = EXEC_TIME_RE.search(line)
    side_match = EXEC_SIDE_RE.search(line)
    shares_match = EXEC_SHARES_RE.search(line)
    price_match = EXEC_PRICE_RE.search(line)
    client_id_match = EXEC_CLIENT_ID_RE.search(line)
    order_id_match = EXEC_ORDER_ID_RE.search(line)
    avg_price_match = EXEC_AVG_PRICE_RE.search(line)
    order_ref_match = EXEC_ORDER_REF_RE.search(line)
    symbol_match = SYMBOL_RE.search(line)
    order_ref_symbol_match = ORDER_REF_SYMBOL_RE.search(line)
    if not all(
        [
            exec_id_match,
            time_match,
            side_match,
            shares_match,
            price_match,
            client_id_match,
            order_id_match,
            avg_price_match,
            order_ref_match,
        ]
    ):
        return None
    symbol = ""
    if symbol_match is not None:
        symbol = symbol_match.group("symbol").upper()
    elif order_ref_symbol_match is not None:
        symbol = order_ref_symbol_match.group("symbol").upper()
    if not symbol:
        return None
    timestamp = datetime(
        int(time_match.group("year")),
        int(time_match.group("month")),
        int(time_match.group("day")),
        int(time_match.group("hour")),
        int(time_match.group("minute")),
        int(time_match.group("second")),
        tzinfo=UTC,
    )
    return ExecutionRecord(
        exec_id=exec_id_match.group("value"),
        symbol=symbol,
        timestamp=timestamp,
        side=side_match.group("value").upper(),
        shares=float(shares_match.group("value")),
        price=float(price_match.group("value")),
        avg_price=float(avg_price_match.group("value")),
        client_id=int(client_id_match.group("value")),
        order_id=_normalize_text(order_id_match.group("value")),
        order_ref=_normalize_text(order_ref_match.group("value")),
    )


def _parse_commission_line(line: str) -> tuple[str, float] | None:
    match = COMMISSION_RE.search(line)
    if not match:
        return None
    return match.group("exec_id"), float(match.group("realized_pnl"))


def _parse_portfolio_line(line: str, year: int) -> PortfolioSnapshot | None:
    match = PORTFOLIO_RE.search(line)
    if not match:
        return None
    timestamp = _line_timestamp(line, year)
    if timestamp is None:
        return None
    return PortfolioSnapshot(
        symbol=match.group("symbol").upper(),
        timestamp=timestamp,
        position=float(match.group("position")),
        realized_pnl=float(match.group("realized_pnl")),
    )


def parse_vm_journal(lines: Iterable[str], *, year: int) -> tuple[list[ExecutionRecord], list[PortfolioSnapshot]]:
    executions: list[ExecutionRecord] = []
    executions_by_exec_id: dict[str, ExecutionRecord] = {}
    portfolios: list[PortfolioSnapshot] = []

    for raw_line in lines:
        line = raw_line.rstrip("\n")
        execution = _parse_execution_line(line)
        if execution is not None:
            executions.append(execution)
            executions_by_exec_id[execution.exec_id] = execution
            continue

        commission = _parse_commission_line(line)
        if commission is not None:
            exec_id, realized_pnl = commission
            execution_match = executions_by_exec_id.get(exec_id)
            if execution_match is not None:
                execution_match.realized_pnl = realized_pnl
            continue

        portfolio = _parse_portfolio_line(line, year)
        if portfolio is not None:
            portfolios.append(portfolio)

    return executions, portfolios


def _expected_exit_side(row: LifecycleRow) -> str:
    if row.side == "BUY" or row.direction == "LONG":
        return "SLD"
    return "BOT"


def _fetch_stale_ibkr_rows(symbols: list[str] | None = None) -> list[LifecycleRow]:
    where = [
        "UPPER(COALESCE(broker, '')) = 'IBKR'",
        "UPPER(COALESCE(status, '')) = 'CLOSED'",
        "COALESCE(exit_reason, '') = 'STALE_OPEN_RECONCILED'",
    ]
    params: dict[str, object] = {}
    if symbols:
        where.append("symbol = ANY(%(symbols)s)")
        params["symbols"] = [symbol.upper() for symbol in symbols]
    rows = fetch_all(
        f"""
        SELECT *
        FROM trade_lifecycles
        WHERE {' AND '.join(where)}
        ORDER BY entry_time ASC, id ASC
        """,
        params,
    )
    result: list[LifecycleRow] = []
    for row in rows:
        result.append(
            LifecycleRow(
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
        )
    return result


def _entry_execution_for_row(row: LifecycleRow, executions: list[ExecutionRecord]) -> ExecutionRecord | None:
    for execution in executions:
        if execution.symbol != row.symbol:
            continue
        if execution.order_id == row.parent_order_id:
            return execution
    return None


def _execution_pair_candidate(row: LifecycleRow, executions: list[ExecutionRecord]) -> RepairCandidate | None:
    entry_execution = _entry_execution_for_row(row, executions)
    if entry_execution is None:
        return None

    exit_side = _expected_exit_side(row)
    related = [
        execution
        for execution in executions
        if execution.symbol == row.symbol
        and execution.order_ref
        and execution.order_ref == entry_execution.order_ref
        and execution.side == exit_side
        and execution.timestamp >= entry_execution.timestamp
        and execution.order_id != entry_execution.order_id
    ]
    if not related:
        return None

    exit_execution = sorted(related, key=lambda item: item.timestamp)[-1]
    realized_pnl = exit_execution.realized_pnl
    if realized_pnl is None:
        realized_pnl = compute_realized_pnl(row.entry_price, exit_execution.price, row.shares, row.direction)

    realized_pnl_percent = None
    if row.entry_price not in (None, 0) and row.shares not in (None, 0) and realized_pnl is not None:
        realized_pnl_percent = round((realized_pnl / (row.entry_price * row.shares)) * 100.0, 6)
    elif realized_pnl is None:
        realized_pnl_percent = compute_realized_pnl_percent(row.entry_price, exit_execution.price, row.direction)

    return RepairCandidate(
        trade_key=row.trade_key,
        symbol=row.symbol,
        exit_order_id=exit_execution.order_id or None,
        exit_time=exit_execution.timestamp,
        exit_price=round(exit_execution.price, 6),
        exit_reason="BROKER_FILLED_EXIT_REPAIRED",
        realized_pnl=realized_pnl,
        realized_pnl_percent=realized_pnl_percent,
        duration_minutes=compute_duration_minutes(row.entry_time, exit_execution.timestamp),
        source="execution_pair",
    )


def _portfolio_snapshot_candidate(row: LifecycleRow, portfolios: list[PortfolioSnapshot]) -> RepairCandidate | None:
    exit_snapshots = [
        snapshot
        for snapshot in portfolios
        if snapshot.symbol == row.symbol
        and snapshot.position == 0
        and snapshot.timestamp >= (row.entry_time or snapshot.timestamp)
        and snapshot.realized_pnl != 0
    ]
    if not exit_snapshots or row.entry_price in (None, 0) or row.shares in (None, 0):
        return None

    snapshot = sorted(exit_snapshots, key=lambda item: item.timestamp)[0]
    if row.direction == "LONG":
        exit_price = row.entry_price + (snapshot.realized_pnl / row.shares)
    else:
        exit_price = row.entry_price - (snapshot.realized_pnl / row.shares)

    return RepairCandidate(
        trade_key=row.trade_key,
        symbol=row.symbol,
        exit_order_id=row.exit_order_id or row.parent_order_id or row.order_id or None,
        exit_time=row.exit_time or snapshot.timestamp,
        exit_price=round(exit_price, 6),
        exit_reason="BROKER_PORTFOLIO_SNAPSHOT_REPAIRED",
        realized_pnl=round(snapshot.realized_pnl, 6),
        realized_pnl_percent=round((snapshot.realized_pnl / (row.entry_price * row.shares)) * 100.0, 6),
        duration_minutes=compute_duration_minutes(row.entry_time, row.exit_time or snapshot.timestamp),
        source="portfolio_snapshot",
    )


def build_repair_candidates(rows: list[LifecycleRow], executions: list[ExecutionRecord], portfolios: list[PortfolioSnapshot]) -> list[RepairCandidate]:
    candidates: list[RepairCandidate] = []
    unresolved_by_symbol: dict[str, list[LifecycleRow]] = {}

    for row in rows:
        candidate = _execution_pair_candidate(row, executions)
        if candidate is not None:
            candidates.append(candidate)
            continue
        unresolved_by_symbol.setdefault(row.symbol, []).append(row)

    for symbol_rows in unresolved_by_symbol.values():
        # A portfolio snapshot is symbol-level aggregate P/L, not per-order fill data.
        # Only use it when there is exactly one unresolved stale row for that symbol.
        if len(symbol_rows) != 1:
            continue
        fallback_candidate = _portfolio_snapshot_candidate(symbol_rows[0], portfolios)
        if fallback_candidate is not None:
            candidates.append(fallback_candidate)
    return candidates


def _fetch_vm_journal_lines(*, project: str, zone: str, instance: str, since: str, until: str) -> list[str]:
    command = [
        "gcloud",
        "compute",
        "ssh",
        instance,
        f"--zone={zone}",
        f"--project={project}",
        "--command",
        f"sudo journalctl -u ibkr-bridge --since \"{since}\" --until \"{until}\" --no-pager",
    ]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return result.stdout.splitlines()


def apply_repair(rows_by_trade_key: dict[str, LifecycleRow], candidates: list[RepairCandidate], *, dry_run: bool) -> list[dict[str, object]]:
    repaired: list[dict[str, object]] = []
    for candidate in candidates:
        row = rows_by_trade_key.get(candidate.trade_key)
        if row is None:
            continue

        repaired.append(
            {
                "trade_key": candidate.trade_key,
                "symbol": candidate.symbol,
                "exit_order_id": candidate.exit_order_id,
                "exit_time": candidate.exit_time.isoformat(),
                "exit_price": candidate.exit_price,
                "exit_reason": candidate.exit_reason,
                "realized_pnl": candidate.realized_pnl,
                "source": candidate.source,
            }
        )
        if dry_run:
            continue

        upsert_trade_lifecycle(
            trade_key=row.trade_key,
            symbol=row.symbol,
            mode=row.mode,
            side=row.side,
            direction=row.direction,
            status="CLOSED",
            entry_time=row.entry_time,
            entry_price=row.entry_price,
            exit_time=candidate.exit_time,
            exit_price=candidate.exit_price,
            stop_price=row.stop_price,
            target_price=row.target_price,
            exit_reason=candidate.exit_reason,
            shares=row.shares,
            realized_pnl=candidate.realized_pnl,
            realized_pnl_percent=candidate.realized_pnl_percent,
            duration_minutes=candidate.duration_minutes,
            signal_timestamp=row.signal_timestamp,
            signal_entry=row.signal_entry,
            signal_stop=row.signal_stop,
            signal_target=row.signal_target,
            signal_confidence=row.signal_confidence,
            broker=row.broker,
            order_id=row.order_id,
            parent_order_id=row.parent_order_id,
            exit_order_id=candidate.exit_order_id or row.exit_order_id or row.parent_order_id,
        )

        execute(
            """
            UPDATE trade_events
            SET event_time = %(event_time)s,
                price = %(price)s,
                order_id = %(order_id)s,
                status = 'CLOSED'
            WHERE UPPER(COALESCE(broker, '')) = 'IBKR'
              AND COALESCE(parent_order_id, '') = %(parent_order_id)s
              AND UPPER(COALESCE(event_type, '')) IN ('MANUAL_CLOSE', 'EOD_CLOSE', 'STOP_HIT', 'TARGET_HIT')
            """,
            {
                "event_time": candidate.exit_time,
                "price": candidate.exit_price,
                "order_id": candidate.exit_order_id or row.parent_order_id,
                "parent_order_id": row.parent_order_id,
            },
        )

        if candidate.exit_order_id:
            insert_broker_order(
                order_id=candidate.exit_order_id,
                broker="IBKR",
                symbol=row.symbol,
                side="SELL" if row.side == "BUY" else "BUY",
                order_type="exit",
                status="Filled",
                qty=row.shares,
                filled_qty=row.shares,
                avg_fill_price=candidate.exit_price,
                submitted_at=candidate.exit_time,
                filled_at=candidate.exit_time,
            )

    return repaired


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair stale IBKR closed trades from retained VM bridge journal logs.")
    parser.add_argument("--project", default="stock-scanner-490821")
    parser.add_argument("--zone", default="europe-west1-b")
    parser.add_argument("--instance", default="ibkr-bridge-vm")
    parser.add_argument("--since", default="2026-04-10 13:00:00")
    parser.add_argument("--until", default="2026-04-10 18:00:00")
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--symbol", action="append", dest="symbols")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    rows = _fetch_stale_ibkr_rows(symbols=args.symbols)
    rows_by_trade_key = {row.trade_key: row for row in rows}
    journal_lines = _fetch_vm_journal_lines(
        project=args.project,
        zone=args.zone,
        instance=args.instance,
        since=args.since,
        until=args.until,
    )
    executions, portfolios = parse_vm_journal(journal_lines, year=args.year)
    candidates = build_repair_candidates(rows, executions, portfolios)
    repaired = apply_repair(rows_by_trade_key, candidates, dry_run=not args.apply)

    print(
        {
            "ok": True,
            "dry_run": not args.apply,
            "stale_row_count": len(rows),
            "candidate_count": len(candidates),
            "repaired": repaired,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
