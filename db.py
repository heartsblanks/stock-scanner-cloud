import os
from contextlib import contextmanager
from functools import lru_cache
from typing import Any, Iterator, Optional

import psycopg
from psycopg.rows import dict_row


DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "")
DB_USER = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SSLMODE = os.getenv("DB_SSLMODE", "require")
DB_CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", "10"))
DB_APPLICATION_NAME = os.getenv("DB_APPLICATION_NAME", "stock-scanner")
DB_URL = os.getenv("DATABASE_URL", "").strip()
DB_SOCKET_DIR = os.getenv("DB_SOCKET_DIR", "").strip()
DB_SOCKET_INSTANCE = os.getenv("DB_SOCKET_INSTANCE", "").strip()


class DatabaseConfigurationError(RuntimeError):
    """Raised when the database connection is not configured correctly."""


@lru_cache(maxsize=1)
def _build_connection_string() -> str:
    if DB_URL:
        return DB_URL

    missing = [
        name
        for name, value in {
            "DB_NAME": DB_NAME,
            "DB_USER": DB_USER,
            "DB_PASSWORD": DB_PASSWORD,
        }.items()
        if not value
    ]
    if missing:
        raise DatabaseConfigurationError(
            f"Missing required database settings: {', '.join(missing)}"
        )

    host = DB_HOST
    if DB_SOCKET_DIR and DB_SOCKET_INSTANCE:
        host = f"{DB_SOCKET_DIR}/{DB_SOCKET_INSTANCE}"

    if not host:
        raise DatabaseConfigurationError(
            "Missing required database setting: DB_HOST or DB_SOCKET_DIR + DB_SOCKET_INSTANCE"
        )

    return (
        f"host={host} "
        f"port={DB_PORT} "
        f"dbname={DB_NAME} "
        f"user={DB_USER} "
        f"password={DB_PASSWORD} "
        f"sslmode={DB_SSLMODE} "
        f"connect_timeout={DB_CONNECT_TIMEOUT} "
        f"application_name={DB_APPLICATION_NAME}"
    )


def get_connection() -> psycopg.Connection:
    return psycopg.connect(
        _build_connection_string(),
        autocommit=False,
        row_factory=dict_row,
    )


@contextmanager
def get_db_cursor(commit: bool = False) -> Iterator[psycopg.Cursor]:
    conn: Optional[psycopg.Connection] = None
    cur: Optional[psycopg.Cursor] = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        yield cur
        if commit:
            conn.commit()
        else:
            conn.rollback()
    except Exception:
        if conn is not None:
            conn.rollback()
        raise
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def fetch_all(query: str, params: Optional[tuple[Any, ...] | dict[str, Any]] = None) -> list[dict[str, Any]]:
    with get_db_cursor(commit=False) as cur:
        cur.execute(query, params)
        return list(cur.fetchall())


def fetch_one(query: str, params: Optional[tuple[Any, ...] | dict[str, Any]] = None) -> Optional[dict[str, Any]]:
    with get_db_cursor(commit=False) as cur:
        cur.execute(query, params)
        return cur.fetchone()


def execute(query: str, params: Optional[tuple[Any, ...] | dict[str, Any]] = None) -> None:
    with get_db_cursor(commit=True) as cur:
        cur.execute(query, params)


def execute_many(query: str, params_seq: list[tuple[Any, ...]] | list[dict[str, Any]]) -> None:
    if not params_seq:
        return
    with get_db_cursor(commit=True) as cur:
        cur.executemany(query, params_seq)


def healthcheck() -> dict[str, Any]:
    row = fetch_one("SELECT 1 AS ok")
    return {
        "ok": bool(row and row.get("ok") == 1),
        "database": DB_NAME or "database_url",
        "host": DB_HOST or (f"{DB_SOCKET_DIR}/{DB_SOCKET_INSTANCE}" if DB_SOCKET_DIR and DB_SOCKET_INSTANCE else "database_url"),
    }


def fetch_recent_closed_trades_for_symbol(symbol: str, limit: int = 5) -> list[dict[str, Any]]:
    """
    Fetch recent CLOSED trades for a given symbol from trade_lifecycles.
    Returns latest trades ordered by exit_time DESC.
    """
    if not symbol:
        return []

    query = """
        SELECT
            symbol,
            exit_time,
            realized_pnl
        FROM trade_lifecycles
        WHERE symbol = %s
          AND status = 'CLOSED'
          AND exit_time IS NOT NULL
        ORDER BY exit_time DESC
        LIMIT %s
    """

    try:
        return fetch_all(query, (symbol, limit))
    except Exception:
        return []