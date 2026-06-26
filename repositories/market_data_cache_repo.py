from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from core.db import execute, fetch_all, fetch_one
from repositories.common import normalize_text


def ensure_market_data_candles_schema() -> None:
    execute(
        """
        CREATE TABLE IF NOT EXISTS market_data_candles (
            id SERIAL PRIMARY KEY,
            broker TEXT NOT NULL DEFAULT 'IBKR',
            symbol TEXT NOT NULL,
            interval TEXT NOT NULL DEFAULT '1min',
            source TEXT NOT NULL DEFAULT 'ibkr_intraday',
            candles JSONB NOT NULL,
            candle_count INTEGER NOT NULL DEFAULT 0,
            last_bar_datetime TEXT,
            ibkr_contract_id INTEGER,
            fetched_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    # Add ibkr_contract_id column to existing tables if missing
    execute(
        """
        ALTER TABLE market_data_candles
        ADD COLUMN IF NOT EXISTS ibkr_contract_id INTEGER
        """
    )
    execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_market_data_candles_broker_symbol_interval
        ON market_data_candles(broker, symbol, interval)
        """
    )
    execute(
        """
        CREATE INDEX IF NOT EXISTS idx_market_data_candles_fetched_at
        ON market_data_candles(fetched_at DESC)
        """
    )


def upsert_market_data_candles(
    *,
    broker: str,
    symbol: str,
    interval: str,
    candles: list[dict[str, Any]],
    fetched_at: datetime,
    source: str = "ibkr_intraday",
    ibkr_contract_id: int | None = None,
) -> None:
    ensure_market_data_candles_schema()
    normalized_candles = list(candles or [])
    last_bar_datetime = None
    if normalized_candles and isinstance(normalized_candles[-1], dict):
        last_bar_datetime = str(normalized_candles[-1].get("datetime") or "").strip() or None
    execute(
        """
        INSERT INTO market_data_candles (
            broker, symbol, interval, source, candles, candle_count, last_bar_datetime,
            ibkr_contract_id, fetched_at, updated_at
        ) VALUES (
            %(broker)s, %(symbol)s, %(interval)s, %(source)s, %(candles)s::jsonb,
            %(candle_count)s, %(last_bar_datetime)s,
            %(ibkr_contract_id)s, %(fetched_at)s, NOW()
        )
        ON CONFLICT (broker, symbol, interval)
        DO UPDATE SET
            source = EXCLUDED.source,
            candles = EXCLUDED.candles,
            candle_count = EXCLUDED.candle_count,
            last_bar_datetime = EXCLUDED.last_bar_datetime,
            ibkr_contract_id = COALESCE(EXCLUDED.ibkr_contract_id, market_data_candles.ibkr_contract_id),
            fetched_at = EXCLUDED.fetched_at,
            updated_at = NOW()
        """,
        {
            "broker": normalize_text(broker).upper() or "IBKR",
            "symbol": normalize_text(symbol).upper(),
            "interval": normalize_text(interval).lower() or "1min",
            "source": normalize_text(source).lower() or "ibkr_intraday",
            "candles": json.dumps(normalized_candles, default=str),
            "candle_count": len(normalized_candles),
            "last_bar_datetime": last_bar_datetime,
            "ibkr_contract_id": int(ibkr_contract_id) if ibkr_contract_id else None,
            "fetched_at": fetched_at,
        },
    )


def get_market_data_candles(
    *,
    broker: str,
    symbol: str,
    interval: str = "1min",
) -> dict[str, Any] | None:
    ensure_market_data_candles_schema()
    row = fetch_one(
        """
        SELECT broker, symbol, interval, source, candles, candle_count, last_bar_datetime, fetched_at, updated_at
        FROM market_data_candles
        WHERE broker = %(broker)s
          AND symbol = %(symbol)s
          AND interval = %(interval)s
        """,
        {
            "broker": normalize_text(broker).upper() or "IBKR",
            "symbol": normalize_text(symbol).upper(),
            "interval": normalize_text(interval).lower() or "1min",
        },
    )
    return dict(row) if row else None


def get_known_contract_ids(*, broker: str = "IBKR") -> dict[str, int]:
    """Returns {symbol: ibkr_contract_id} for all symbols with a stored contract_id."""
    ensure_market_data_candles_schema()
    rows = fetch_all(
        """
        SELECT symbol, ibkr_contract_id
        FROM market_data_candles
        WHERE broker = %(broker)s
          AND ibkr_contract_id IS NOT NULL
        """,
        {"broker": normalize_text(broker).upper() or "IBKR"},
    )
    return {row["symbol"]: int(row["ibkr_contract_id"]) for row in rows}


def get_market_data_cache_summary(*, broker: str = "IBKR", limit: int = 20) -> list[dict[str, Any]]:
    ensure_market_data_candles_schema()
    return fetch_all(
        """
        SELECT broker, symbol, interval, candle_count, last_bar_datetime, fetched_at, updated_at
        FROM market_data_candles
        WHERE broker = %(broker)s
        ORDER BY fetched_at DESC, symbol
        LIMIT %(limit)s
        """,
        {
            "broker": normalize_text(broker).upper() or "IBKR",
            "limit": max(1, min(int(limit), 500)),
        },
    )
