from __future__ import annotations

import os
import time
from typing import Any

from core.logging_utils import log_exception
from core.logging_utils import log_info


_ALERT_CACHE: dict[str, float] = {}


def telegram_alerts_enabled() -> bool:
    bot_token = str(os.getenv("TELEGRAM_BOT_TOKEN", "")).strip()
    chat_id = str(os.getenv("TELEGRAM_CHAT_ID", "")).strip()
    return bool(bot_token and chat_id)


def send_telegram_alert(*, alert_key: str, message: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    bot_token = str(os.getenv("TELEGRAM_BOT_TOKEN", "")).strip()
    chat_id = str(os.getenv("TELEGRAM_CHAT_ID", "")).strip()
    if not bot_token or not chat_id:
        return {
            "ok": False,
            "sent": False,
            "reason": "telegram_not_configured",
        }

    dedupe_minutes = max(1, int(os.getenv("TELEGRAM_ALERT_DEDUP_MINUTES", "30")))
    now = time.time()
    last_sent_at = _ALERT_CACHE.get(alert_key, 0.0)
    if last_sent_at and (now - last_sent_at) < (dedupe_minutes * 60):
        return {
            "ok": True,
            "sent": False,
            "reason": "deduped",
            "dedupe_minutes": dedupe_minutes,
        }

    lines = [message.strip()]
    if payload:
        for key, value in payload.items():
            lines.append(f"{key}: {value}")
    text = "\n".join(line for line in lines if line)

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    body = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }

    try:
        import requests

        response = requests.post(url, json=body, timeout=10)
        response.raise_for_status()
    except Exception as exc:
        log_exception(
            "Telegram alert send failed",
            exc,
            component="alert_service",
            operation="send_telegram_alert",
            alert_key=alert_key,
        )
        return {
            "ok": False,
            "sent": False,
            "reason": str(exc),
        }

    _ALERT_CACHE[alert_key] = now
    log_info(
        "Telegram alert delivered",
        component="alert_service",
        operation="send_telegram_alert",
        alert_key=alert_key,
    )
    return {
        "ok": True,
        "sent": True,
        "reason": "delivered",
        "status_code": response.status_code,
    }
