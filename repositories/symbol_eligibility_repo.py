from __future__ import annotations

from typing import Any

from core.db import fetch_all, get_db_cursor
from repositories.common import normalize_text, to_optional_float


_SCHEMA_READY = False


def ensure_symbol_eligibility_schema() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return

    with get_db_cursor(commit=True) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS symbol_session_eligibility (
                id SERIAL PRIMARY KEY,
                session_date DATE NOT NULL,
                mode TEXT NOT NULL,
                symbol TEXT NOT NULL,
                display_name TEXT,
                currency TEXT,
                last_price NUMERIC,
                max_notional NUMERIC,
                eligible BOOLEAN NOT NULL DEFAULT FALSE,
                ineligible_reason TEXT,
                source TEXT,
                price_timestamp TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_symbol_session_eligibility_session_mode_symbol
            ON symbol_session_eligibility(session_date, mode, symbol)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_symbol_session_eligibility_mode_session
            ON symbol_session_eligibility(mode, session_date DESC)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_symbol_session_eligibility_symbol
            ON symbol_session_eligibility(symbol)
            """
        )

    _SCHEMA_READY = True


def replace_symbol_session_eligibility_rows(
    *,
    session_date: str,
    mode: str,
    rows: list[dict[str, Any]],
) -> int:
    ensure_symbol_eligibility_schema()

    normalized_mode = normalize_text(mode).lower()
    if not normalized_mode:
        raise ValueError("mode is required")
    if not normalize_text(session_date):
        raise ValueError("session_date is required")

    with get_db_cursor(commit=True) as cur:
        cur.execute(
            """
            DELETE FROM symbol_session_eligibility
            WHERE session_date = %(session_date)s::date
              AND mode = %(mode)s
            """,
            {"session_date": session_date, "mode": normalized_mode},
        )
        if rows:
            cur.executemany(
                """
                INSERT INTO symbol_session_eligibility (
                    session_date,
                    mode,
                    symbol,
                    display_name,
                    currency,
                    last_price,
                    max_notional,
                    eligible,
                    ineligible_reason,
                    source,
                    price_timestamp
                )
                VALUES (
                    %(session_date)s::date,
                    %(mode)s,
                    %(symbol)s,
                    %(display_name)s,
                    %(currency)s,
                    %(last_price)s,
                    %(max_notional)s,
                    %(eligible)s,
                    %(ineligible_reason)s,
                    %(source)s,
                    %(price_timestamp)s
                )
                """,
                [
                    {
                        "session_date": session_date,
                        "mode": normalized_mode,
                        "symbol": normalize_text(row.get("symbol")).upper(),
                        "display_name": normalize_text(row.get("display_name")) or None,
                        "currency": normalize_text(row.get("currency")).upper() or None,
                        "last_price": to_optional_float(row.get("last_price")),
                        "max_notional": to_optional_float(row.get("max_notional")),
                        "eligible": bool(row.get("eligible", False)),
                        "ineligible_reason": normalize_text(row.get("ineligible_reason")) or None,
                        "source": normalize_text(row.get("source")) or None,
                        "price_timestamp": row.get("price_timestamp"),
                    }
                    for row in rows
                    if normalize_text(row.get("symbol"))
                ],
            )
    return len(rows)


def get_symbol_session_eligibility_rows(*, session_date: str, mode: str) -> list[dict[str, Any]]:
    ensure_symbol_eligibility_schema()
    return fetch_all(
        """
        SELECT
            session_date::text AS session_date,
            mode,
            symbol,
            display_name,
            currency,
            last_price,
            max_notional,
            eligible,
            ineligible_reason,
            source,
            price_timestamp
        FROM symbol_session_eligibility
        WHERE session_date = %(session_date)s::date
          AND mode = %(mode)s
        ORDER BY symbol ASC
        """,
        {
            "session_date": session_date,
            "mode": normalize_text(mode).lower(),
        },
    )


def get_latest_symbol_session_eligibility_rows(
    *,
    mode: str,
    on_or_before_date: str,
) -> list[dict[str, Any]]:
    ensure_symbol_eligibility_schema()
    return fetch_all(
        """
        WITH latest AS (
            SELECT MAX(session_date) AS latest_session_date
            FROM symbol_session_eligibility
            WHERE mode = %(mode)s
              AND session_date <= %(on_or_before_date)s::date
        )
        SELECT
            s.session_date::text AS session_date,
            s.mode,
            s.symbol,
            s.display_name,
            s.currency,
            s.last_price,
            s.max_notional,
            s.eligible,
            s.ineligible_reason,
            s.source,
            s.price_timestamp
        FROM symbol_session_eligibility s
        JOIN latest l ON s.session_date = l.latest_session_date
        WHERE s.mode = %(mode)s
        ORDER BY s.symbol ASC
        """,
        {
            "mode": normalize_text(mode).lower(),
            "on_or_before_date": on_or_before_date,
        },
    )
