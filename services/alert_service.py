from __future__ import annotations

import os
import time
from typing import Any

import requests

from core.logging_utils import log_exception
from core.logging_utils import log_info


_ALERT_CACHE: dict[str, float] = {}


def signal_alert_webhook_enabled() -> bool:
    return bool(str(os.getenv("SIGNAL_ALERT_WEBHOOK_URL", "")).strip())


def send_signal_alert(*, alert_key: str, message: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    webhook_url = str(os.getenv("SIGNAL_ALERT_WEBHOOK_URL", "")).strip()
    if not webhook_url:
        return {
            "ok": False,
            "sent": False,
            "reason": "signal_webhook_not_configured",
        }

    dedupe_minutes = max(1, int(os.getenv("SIGNAL_ALERT_DEDUP_MINUTES", "30")))
    now = time.time()
    last_sent_at = _ALERT_CACHE.get(alert_key, 0.0)
    if last_sent_at and (now - last_sent_at) < (dedupe_minutes * 60):
        return {
            "ok": True,
            "sent": False,
            "reason": "deduped",
            "dedupe_minutes": dedupe_minutes,
        }

    body = {
        "message": message,
        "text": message,
        "payload": payload or {},
        "alert_key": alert_key,
    }

    try:
        response = requests.post(webhook_url, json=body, timeout=10)
        response.raise_for_status()
    except Exception as exc:
        log_exception(
            "Signal alert send failed",
            exc,
            component="alert_service",
            operation="send_signal_alert",
            alert_key=alert_key,
        )
        return {
            "ok": False,
            "sent": False,
            "reason": str(exc),
        }

    _ALERT_CACHE[alert_key] = now
    log_info(
        "Signal alert delivered",
        component="alert_service",
        operation="send_signal_alert",
        alert_key=alert_key,
    )
    return {
        "ok": True,
        "sent": True,
        "reason": "delivered",
        "status_code": response.status_code,
    }
