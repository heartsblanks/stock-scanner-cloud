from __future__ import annotations

import copy
import os
import threading
import time

from core.db import fetch_all, fetch_one, get_db_cursor
from core.logging_utils import log_warning


REQUIRED_MODES = (
    "primary",
    "secondary",
    "third",
    "fourth",
    "fifth",
    "sixth",
    "us_test",
    "core_one",
    "core_two",
    "core_three",
)

_DEFAULT_INSTRUMENT_GROUPS: dict[str, dict[str, dict[str, object]]] = {
    "primary": {
        "Rivian": {"symbol": "RIVN", "type": "stock", "priority": 10, "market": "NASDAQ"},
        "SoFi": {"symbol": "SOFI", "type": "stock", "priority": 9, "market": "NASDAQ"},
        "Snap": {"symbol": "SNAP", "type": "stock", "priority": 8, "market": "NASDAQ"},
        "NIO": {"symbol": "NIO", "type": "stock", "priority": 8, "market": "NASDAQ"},
        "Hims & Hers": {"symbol": "HIMS", "type": "stock", "priority": 8, "market": "NASDAQ"},
        "Joby Aviation": {"symbol": "JOBY", "type": "stock", "priority": 8, "market": "NASDAQ"},
    },
    "secondary": {
        "Lucid": {"symbol": "LCID", "type": "stock", "priority": 7, "market": "NASDAQ"},
        "Archer Aviation": {"symbol": "ACHR", "type": "stock", "priority": 7, "market": "NASDAQ"},
        "Opendoor": {"symbol": "OPEN", "type": "stock", "priority": 6, "market": "NASDAQ"},
        "QuantumScape": {"symbol": "QS", "type": "stock", "priority": 6, "market": "NASDAQ"},
        "Plug Power": {"symbol": "PLUG", "type": "stock", "priority": 6, "market": "NASDAQ"},
        "Grab": {"symbol": "GRAB", "type": "stock", "priority": 6, "market": "NASDAQ"},
    },
    "third": {
        "Bitfarms": {"symbol": "BITF", "type": "stock", "priority": 7, "market": "NASDAQ"},
        "Clover Health": {"symbol": "CLOV", "type": "stock", "priority": 6, "market": "NASDAQ"},
        "TAL Education": {"symbol": "TAL", "type": "stock", "priority": 6, "market": "NASDAQ"},
        "Telefonica Brasil": {"symbol": "VIV", "type": "stock", "priority": 5, "market": "NASDAQ"},
        "Rocket Lab": {"symbol": "RKLB", "type": "stock", "priority": 7, "market": "NASDAQ"},
        "BigBear.ai": {"symbol": "BBAI", "type": "stock", "priority": 6, "market": "NASDAQ"},
    },
    "fourth": {
        "SoundHound AI": {"symbol": "SOUN", "type": "stock", "priority": 7, "market": "NASDAQ"},
        "C3.ai": {"symbol": "AI", "type": "stock", "priority": 7, "market": "NASDAQ"},
        "D-Wave Quantum": {"symbol": "QBTS", "type": "stock", "priority": 7, "market": "NASDAQ"},
        "IonQ": {"symbol": "IONQ", "type": "stock", "priority": 7, "market": "NASDAQ"},
        "Nu Holdings": {"symbol": "NU", "type": "stock", "priority": 6, "market": "NASDAQ"},
        "Tempus AI": {"symbol": "TEM", "type": "stock", "priority": 6, "market": "NASDAQ"},
    },
    "fifth": {
        "Affirm": {"symbol": "AFRM", "type": "stock", "priority": 8, "market": "NASDAQ"},
        "Robinhood": {"symbol": "HOOD", "type": "stock", "priority": 8, "market": "NASDAQ"},
        "Peloton": {"symbol": "PTON", "type": "stock", "priority": 6, "market": "NASDAQ"},
        "fuboTV": {"symbol": "FUBO", "type": "stock", "priority": 6, "market": "NASDAQ"},
    },
    "sixth": {
        "MARA Holdings": {"symbol": "MARA", "type": "stock", "priority": 7, "market": "NASDAQ"},
        "TeraWulf": {"symbol": "WULF", "type": "stock", "priority": 6, "market": "NASDAQ"},
        "Recursion": {"symbol": "RXRX", "type": "stock", "priority": 6, "market": "NASDAQ"},
        "Cerence": {"symbol": "CRNC", "type": "stock", "priority": 6, "market": "NASDAQ"},
    },
    "us_test": {
        "Qualcomm": {
            "symbol": "QCOM",
            "type": "stock",
            "priority": 10,
            "market": "NASDAQ",
            "exchange": "SMART",
            "primary_exchange": "NMS",
            "currency": "USD",
        },
        "Cisco": {
            "symbol": "CSCO",
            "type": "stock",
            "priority": 9,
            "market": "NASDAQ",
            "exchange": "SMART",
            "primary_exchange": "NMS",
            "currency": "USD",
        },
        "Applied Materials": {
            "symbol": "AMAT",
            "type": "stock",
            "priority": 9,
            "market": "NASDAQ",
            "exchange": "SMART",
            "primary_exchange": "NMS",
            "currency": "USD",
        },
        "Texas Instruments": {
            "symbol": "TXN",
            "type": "stock",
            "priority": 8,
            "market": "NASDAQ",
            "exchange": "SMART",
            "primary_exchange": "NMS",
            "currency": "USD",
        },
        "Automatic Data Processing": {
            "symbol": "ADP",
            "type": "stock",
            "priority": 8,
            "market": "NASDAQ",
            "exchange": "SMART",
            "primary_exchange": "NMS",
            "currency": "USD",
        },
        "Palo Alto Networks": {
            "symbol": "PANW",
            "type": "stock",
            "priority": 8,
            "market": "NASDAQ",
            "exchange": "SMART",
            "primary_exchange": "NMS",
            "currency": "USD",
        },
    },
    "core_one": {
        "NVIDIA": {"symbol": "NVDA", "type": "stock", "priority": 10, "market": "NASDAQ"},
        "Tesla": {"symbol": "TSLA", "type": "stock", "priority": 10, "market": "NASDAQ"},
        "Apple": {"symbol": "AAPL", "type": "stock", "priority": 10, "market": "NASDAQ"},
        "AMD": {"symbol": "AMD", "type": "stock", "priority": 9, "market": "NASDAQ"},
        "Palantir": {"symbol": "PLTR", "type": "stock", "priority": 9, "market": "NASDAQ"},
        "Microsoft": {"symbol": "MSFT", "type": "stock", "priority": 9, "market": "NASDAQ"},
    },
    "core_two": {
        "Meta": {"symbol": "META", "type": "stock", "priority": 9, "market": "NASDAQ"},
        "Amazon": {"symbol": "AMZN", "type": "stock", "priority": 9, "market": "NASDAQ"},
        "Alphabet": {"symbol": "GOOGL", "type": "stock", "priority": 9, "market": "NASDAQ"},
        "Netflix": {"symbol": "NFLX", "type": "stock", "priority": 8, "market": "NASDAQ"},
        "Broadcom": {"symbol": "AVGO", "type": "stock", "priority": 8, "market": "NASDAQ"},
        "Micron": {"symbol": "MU", "type": "stock", "priority": 8, "market": "NASDAQ"},
    },
    "core_three": {
        "Arm": {"symbol": "ARM", "type": "stock", "priority": 9, "market": "NASDAQ"},
        "Super Micro Computer": {"symbol": "SMCI", "type": "stock", "priority": 9, "market": "NASDAQ"},
        "Adobe": {"symbol": "ADBE", "type": "stock", "priority": 8, "market": "NASDAQ"},
        "Intel": {"symbol": "INTC", "type": "stock", "priority": 8, "market": "NASDAQ"},
    },
}

_SCHEMA_READY = False
_CACHE_LOCK = threading.Lock()
_CACHE_DATA: dict[str, dict[str, dict[str, object]]] | None = None
_CACHE_LOADED_AT = 0.0
_CACHE_TTL_SECONDS = max(0, int(os.getenv("INSTRUMENT_CATALOG_CACHE_TTL_SECONDS", "30")))


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def ensure_instrument_catalog_schema() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return

    with get_db_cursor(commit=True) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS instrument_catalog (
                id SERIAL PRIMARY KEY,
                mode TEXT NOT NULL,
                display_name TEXT NOT NULL,
                symbol TEXT NOT NULL,
                instrument_type TEXT NOT NULL,
                priority INT NOT NULL,
                market TEXT NOT NULL,
                exchange TEXT,
                primary_exchange TEXT,
                currency TEXT,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_instrument_catalog_mode_symbol
            ON instrument_catalog(mode, symbol)
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_instrument_catalog_symbol
            ON instrument_catalog(symbol)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_instrument_catalog_mode_active
            ON instrument_catalog(mode, active)
            """
        )

    _SCHEMA_READY = True


def _seed_defaults_if_empty() -> None:
    count_row = fetch_one("SELECT COUNT(*)::int AS count FROM instrument_catalog WHERE active = TRUE")
    if int((count_row or {}).get("count") or 0) > 0:
        return

    seed_rows: list[dict[str, object]] = []
    for mode, instruments in _DEFAULT_INSTRUMENT_GROUPS.items():
        for display_name, info in instruments.items():
            seed_rows.append(
                {
                    "mode": mode,
                    "display_name": display_name,
                    "symbol": _normalize_text(info.get("symbol")).upper(),
                    "instrument_type": _normalize_text(info.get("type")).lower(),
                    "priority": int(info.get("priority") or 0),
                    "market": _normalize_text(info.get("market")).upper(),
                    "exchange": _normalize_text(info.get("exchange")).upper() or None,
                    "primary_exchange": _normalize_text(info.get("primary_exchange")).upper() or None,
                    "currency": _normalize_text(info.get("currency")).upper() or None,
                    "active": True,
                }
            )

    with get_db_cursor(commit=True) as cur:
        cur.executemany(
            """
            INSERT INTO instrument_catalog (
                mode,
                display_name,
                symbol,
                instrument_type,
                priority,
                market,
                exchange,
                primary_exchange,
                currency,
                active
            )
            VALUES (
                %(mode)s,
                %(display_name)s,
                %(symbol)s,
                %(instrument_type)s,
                %(priority)s,
                %(market)s,
                %(exchange)s,
                %(primary_exchange)s,
                %(currency)s,
                %(active)s
            )
            ON CONFLICT (symbol) DO NOTHING
            """,
            seed_rows,
        )


def _rows_to_groups(rows: list[dict[str, object]]) -> dict[str, dict[str, dict[str, object]]]:
    groups: dict[str, dict[str, dict[str, object]]] = {mode: {} for mode in REQUIRED_MODES}
    symbol_to_mode: dict[str, str] = {}

    for row in rows:
        mode = _normalize_text(row.get("mode")).lower()
        if mode not in groups:
            continue

        display_name = _normalize_text(row.get("display_name"))
        symbol = _normalize_text(row.get("symbol")).upper()
        instrument_type = _normalize_text(row.get("instrument_type")).lower()
        market = _normalize_text(row.get("market")).upper()
        exchange = _normalize_text(row.get("exchange")).upper() or None
        primary_exchange = _normalize_text(row.get("primary_exchange")).upper() or None
        currency = _normalize_text(row.get("currency")).upper() or None

        try:
            priority = int(row.get("priority") or 0)
        except Exception as exc:  # pragma: no cover - defensive guard
            raise ValueError(f"Invalid priority for {display_name!r} in {mode}") from exc

        if not display_name or not symbol or not instrument_type or not market or priority <= 0:
            raise ValueError(f"Invalid instrument entry for {display_name!r} in {mode}")

        if symbol in symbol_to_mode:
            raise ValueError(
                f"Duplicate symbol {symbol!r} found in both {symbol_to_mode[symbol]!r} and {mode!r}"
            )

        symbol_to_mode[symbol] = mode
        groups[mode][display_name] = {
            "symbol": symbol,
            "type": instrument_type,
            "priority": priority,
            "market": market,
            "exchange": exchange,
            "primary_exchange": primary_exchange,
            "currency": currency,
        }

    for mode in REQUIRED_MODES:
        if not groups[mode]:
            raise ValueError(f"Missing instrument group: {mode}")

    return groups


def _load_groups_from_db() -> dict[str, dict[str, dict[str, object]]]:
    ensure_instrument_catalog_schema()
    _seed_defaults_if_empty()
    rows = fetch_all(
        """
        SELECT
            mode,
            display_name,
            symbol,
            instrument_type,
            priority,
            market,
            exchange,
            primary_exchange,
            currency
        FROM instrument_catalog
        WHERE active = TRUE
        ORDER BY mode ASC, priority DESC, display_name ASC
        """
    )
    return _rows_to_groups(rows)


def get_instrument_groups(*, force_refresh: bool = False) -> dict[str, dict[str, dict[str, object]]]:
    global _CACHE_DATA, _CACHE_LOADED_AT

    with _CACHE_LOCK:
        now = time.time()
        if (
            not force_refresh
            and _CACHE_DATA is not None
            and (_CACHE_TTL_SECONDS == 0 or (now - _CACHE_LOADED_AT) <= _CACHE_TTL_SECONDS)
        ):
            return copy.deepcopy(_CACHE_DATA)

        try:
            loaded = _load_groups_from_db()
            _CACHE_DATA = loaded
            _CACHE_LOADED_AT = now
            return copy.deepcopy(loaded)
        except Exception as exc:
            if _CACHE_DATA is not None:
                log_warning(
                    "Failed to refresh instrument catalog from DB, using cached values",
                    component="instrument_catalog",
                    error=str(exc),
                )
                return copy.deepcopy(_CACHE_DATA)

            raise RuntimeError("instrument catalog unavailable from DB") from exc


def get_mode_instruments(mode: str, *, force_refresh: bool = False) -> dict[str, dict[str, object]]:
    normalized_mode = _normalize_text(mode).lower()
    if normalized_mode not in REQUIRED_MODES:
        return {}
    groups = get_instrument_groups(force_refresh=force_refresh)
    return copy.deepcopy(groups.get(normalized_mode, {}))
