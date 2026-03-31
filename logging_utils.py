from __future__ import annotations

import json
import logging
import os
import traceback
from typing import Any


_CONFIGURED = False


def _configure_logging() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return

    log_level_name = str(os.getenv("LOG_LEVEL", "INFO")).strip().upper() or "INFO"
    log_level = getattr(logging, log_level_name, logging.INFO)
    logging.basicConfig(level=log_level, format="%(message)s")
    _CONFIGURED = True


def _emit(level: int, message: str, **fields: Any) -> None:
    _configure_logging()
    payload = {
        "severity": logging.getLevelName(level),
        "message": message,
        **fields,
    }
    logging.getLogger("stock-scanner").log(
        level,
        json.dumps(payload, default=str, ensure_ascii=True),
    )


def log_info(message: str, **fields: Any) -> None:
    _emit(logging.INFO, message, **fields)


def log_warning(message: str, **fields: Any) -> None:
    _emit(logging.WARNING, message, **fields)


def log_error(message: str, **fields: Any) -> None:
    _emit(logging.ERROR, message, **fields)


def log_exception(message: str, error: BaseException, **fields: Any) -> None:
    _emit(
        logging.ERROR,
        message,
        error_type=type(error).__name__,
        error=str(error),
        traceback="".join(traceback.format_exception(type(error), error, error.__traceback__)),
        **fields,
    )
