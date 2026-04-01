from __future__ import annotations

from typing import Any, Optional


def normalize_text(value: Optional[str]) -> str:
    return str(value or "").strip()


def to_optional_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
