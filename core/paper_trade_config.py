import os


DEFAULT_PAPER_TRADE_MAX_CAPITAL_ALLOCATION_PCT = 1.0
DEFAULT_PAPER_TRADE_MAX_POSITIONS = 10
DEFAULT_PAPER_TRADE_ENFORCE_MAX_POSITIONS = False


def _env_flag(name: str, default: bool) -> bool:
    value = str(os.getenv(name, "true" if default else "false")).strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    return max(0.0, value)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return max(1, value)


def get_paper_trade_limits() -> dict[str, float | int | bool]:
    max_capital_allocation_pct = min(
        1.0,
        _env_float(
            "PAPER_TRADE_MAX_CAPITAL_ALLOCATION_PCT",
            DEFAULT_PAPER_TRADE_MAX_CAPITAL_ALLOCATION_PCT,
        ),
    )
    max_positions = _env_int("PAPER_TRADE_MAX_POSITIONS", DEFAULT_PAPER_TRADE_MAX_POSITIONS)
    enforce_max_positions = _env_flag(
        "PAPER_TRADE_ENFORCE_MAX_POSITIONS",
        DEFAULT_PAPER_TRADE_ENFORCE_MAX_POSITIONS,
    )

    return {
        "max_capital_allocation_pct": max_capital_allocation_pct,
        "max_positions": max_positions,
        "position_limit_enforced": enforce_max_positions,
    }
