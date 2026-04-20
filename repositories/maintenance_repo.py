from __future__ import annotations

from typing import Any

from core.db import fetch_one, get_db_cursor


_RETENTION_TABLES: dict[str, tuple[str, str]] = {
    "broker_api_logs": ("logged_at", "broker_api_logs"),
    "signal_logs": ("timestamp_utc", "signal_logs"),
    "scan_runs": ("scan_time", "scan_runs"),
    "paper_trade_attempts": ("timestamp_utc", "paper_trade_attempts"),
    "broker_orders": ("created_at", "broker_orders"),
    "reconciliation_details": ("created_at", "reconciliation_details"),
    "reconciliation_runs": ("run_time", "reconciliation_runs"),
}


def prune_operational_data(retention_days_by_table: dict[str, int]) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}

    for table_name, retention_days in retention_days_by_table.items():
        if table_name not in _RETENTION_TABLES:
            raise ValueError(f"Unsupported maintenance table: {table_name}")

        timestamp_column, resolved_table_name = _RETENTION_TABLES[table_name]
        resolved_retention_days = max(1, int(retention_days))
        row = fetch_one(
            f"""
            WITH deleted AS (
                DELETE FROM {resolved_table_name}
                WHERE {timestamp_column} < NOW() - (%(retention_days)s::text || ' days')::interval
                RETURNING 1
            )
            SELECT COUNT(*)::INT AS deleted_count FROM deleted
            """,
            {"retention_days": resolved_retention_days},
        )
        deleted_count = int(row["deleted_count"]) if row and row.get("deleted_count") is not None else 0
        results[table_name] = {
            "retention_days": resolved_retention_days,
            "deleted_count": deleted_count,
        }

    return results


def purge_legacy_broker_data() -> dict[str, Any]:
    statements: tuple[tuple[str, str], ...] = (
        (
            "paper_trade_attempts",
            """
            WITH deleted AS (
                DELETE FROM paper_trade_attempts
                WHERE UPPER(COALESCE(NULLIF(broker, ''), 'LEGACY')) <> 'IBKR'
                RETURNING 1
            )
            SELECT COUNT(*)::INT AS deleted_count FROM deleted
            """,
        ),
        (
            "trade_events",
            """
            WITH deleted AS (
                DELETE FROM trade_events
                WHERE UPPER(COALESCE(NULLIF(broker, ''), 'LEGACY')) <> 'IBKR'
                RETURNING 1
            )
            SELECT COUNT(*)::INT AS deleted_count FROM deleted
            """,
        ),
        (
            "trade_lifecycles",
            """
            WITH deleted AS (
                DELETE FROM trade_lifecycles
                WHERE UPPER(COALESCE(NULLIF(broker, ''), 'LEGACY')) <> 'IBKR'
                RETURNING 1
            )
            SELECT COUNT(*)::INT AS deleted_count FROM deleted
            """,
        ),
        (
            "broker_orders",
            """
            WITH deleted AS (
                DELETE FROM broker_orders
                WHERE UPPER(COALESCE(NULLIF(broker, ''), 'LEGACY')) <> 'IBKR'
                RETURNING 1
            )
            SELECT COUNT(*)::INT AS deleted_count FROM deleted
            """,
        ),
        (
            "broker_api_logs",
            """
            WITH deleted AS (
                DELETE FROM broker_api_logs
                RETURNING 1
            )
            SELECT COUNT(*)::INT AS deleted_count FROM deleted
            """,
        ),
    )

    deleted_counts: dict[str, int] = {}
    total_deleted = 0

    with get_db_cursor(commit=True) as cur:
        for table_name, statement in statements:
            cur.execute(statement, {})
            row = cur.fetchone()
            deleted_count = int(row["deleted_count"]) if row and row.get("deleted_count") is not None else 0
            deleted_counts[table_name] = deleted_count
            total_deleted += deleted_count

    return {
        "ok": True,
        "deleted_counts": deleted_counts,
        "total_deleted": total_deleted,
    }


def purge_all_test_data() -> dict[str, Any]:
    tables: tuple[str, ...] = (
        "paper_trade_attempts",
        "trade_events",
        "trade_lifecycles",
        "broker_orders",
        "signal_logs",
        "scan_runs",
        "reconciliation_details",
        "reconciliation_runs",
        "broker_api_logs",
        "mode_rankings",
    )

    deleted_counts: dict[str, int] = {}
    total_deleted = 0

    with get_db_cursor(commit=True) as cur:
        for table_name in tables:
            cur.execute(f"SELECT COUNT(*)::INT AS row_count FROM {table_name}")
            row = cur.fetchone()
            row_count = int(row["row_count"]) if row and row.get("row_count") is not None else 0
            deleted_counts[table_name] = row_count
            total_deleted += row_count

        table_list = ", ".join(tables)
        cur.execute(f"TRUNCATE TABLE {table_list} RESTART IDENTITY CASCADE")

    return {
        "ok": True,
        "tables_truncated": list(tables),
        "deleted_counts": deleted_counts,
        "total_deleted": total_deleted,
    }
