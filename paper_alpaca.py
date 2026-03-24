import os
from typing import Any

import requests

ALPACA_API_KEY_ID = os.getenv("APCA_API_KEY_ID", "")
ALPACA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY", "")
ALPACA_TRADING_BASE_URL = os.getenv("APCA_BASE_URL", "https://paper-api.alpaca.markets").rstrip("/")
ALPACA_MAX_NOTIONAL = float(os.getenv("ALPACA_MAX_NOTIONAL", "5000"))


def _auth_headers() -> dict[str, str]:
    if not ALPACA_API_KEY_ID or not ALPACA_API_SECRET_KEY:
        raise RuntimeError("Missing Alpaca API credentials")
    return {
        "accept": "application/json",
        "content-type": "application/json",
        "APCA-API-KEY-ID": ALPACA_API_KEY_ID,
        "APCA-API-SECRET-KEY": ALPACA_API_SECRET_KEY,
    }


def get_account() -> dict[str, Any]:
    response = requests.get(
        f"{ALPACA_TRADING_BASE_URL}/v2/account",
        headers=_auth_headers(),
        timeout=20,
    )
    response.raise_for_status()
    return response.json()


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def place_paper_bracket_order_from_trade(trade: dict, max_notional: float | None = None) -> dict[str, Any]:
    max_notional = ALPACA_MAX_NOTIONAL if max_notional is None else float(max_notional)
    metrics = trade.get("metrics", {})
    symbol = str(metrics.get("symbol", "")).strip().upper()
    entry = _to_float(metrics.get("entry"))
    stop = _to_float(metrics.get("stop"))
    target = _to_float(metrics.get("target"))
    scanner_shares = int(_to_float(metrics.get("shares"), 0))

    if not symbol:
        return {"attempted": False, "placed": False, "reason": "missing_symbol"}
    if entry <= 0:
        return {"attempted": False, "placed": False, "symbol": symbol, "reason": "invalid_entry_price"}
    if stop <= 0 or target <= 0:
        return {"attempted": False, "placed": False, "symbol": symbol, "reason": "invalid_stop_or_target"}
    if stop >= entry:
        return {"attempted": False, "placed": False, "symbol": symbol, "reason": "stop_must_be_below_entry"}
    if target <= entry:
        return {"attempted": False, "placed": False, "symbol": symbol, "reason": "target_must_be_above_entry"}
    if scanner_shares <= 0:
        return {"attempted": False, "placed": False, "symbol": symbol, "reason": "invalid_scanner_share_size"}

    capped_shares = int(max_notional / entry)
    final_shares = min(scanner_shares, capped_shares)
    if final_shares <= 0:
        return {
            "attempted": False,
            "placed": False,
            "symbol": symbol,
            "reason": "max_notional_too_small_for_symbol",
            "max_notional": round(max_notional, 2),
            "entry": round(entry, 4),
        }

    estimated_notional = round(final_shares * entry, 2)
    account = get_account()
    account_status = str(account.get("status", "")).strip().upper()
    buying_power = _to_float(account.get("buying_power"))
    cash = _to_float(account.get("cash"))
    available_funds = buying_power if buying_power > 0 else cash

    if account_status and account_status != "ACTIVE":
        return {
            "attempted": True,
            "placed": False,
            "symbol": symbol,
            "shares": final_shares,
            "estimated_notional": estimated_notional,
            "reason": "account_not_active",
            "account_status": account_status,
        }
    if estimated_notional > available_funds:
        return {
            "attempted": True,
            "placed": False,
            "symbol": symbol,
            "shares": final_shares,
            "estimated_notional": estimated_notional,
            "reason": "insufficient_buying_power",
            "available_funds": round(available_funds, 2),
        }

    client_order_id = f"scanner-{symbol}-{int(entry * 10000)}-{final_shares}"
    payload = {
        "symbol": symbol,
        "qty": str(final_shares),
        "side": "buy",
        "type": "market",
        "time_in_force": "day",
        "order_class": "bracket",
        "take_profit": {"limit_price": f"{target:.2f}"},
        "stop_loss": {"stop_price": f"{stop:.2f}"},
        "client_order_id": client_order_id,
    }

    response = requests.post(
        f"{ALPACA_TRADING_BASE_URL}/v2/orders",
        headers=_auth_headers(),
        json=payload,
        timeout=20,
    )
    if response.status_code >= 400:
        return {
            "attempted": True,
            "placed": False,
            "symbol": symbol,
            "shares": final_shares,
            "estimated_notional": estimated_notional,
            "reason": "alpaca_order_rejected",
            "http_status": response.status_code,
            "details": response.text.strip(),
        }

    order = response.json()
    legs = order.get("legs") or []
    take_profit_leg_id = ""
    stop_loss_leg_id = ""
    for leg in legs:
        leg_type = str(leg.get("type", "")).strip().lower()
        if leg_type == "limit":
            take_profit_leg_id = str(leg.get("id", ""))
        elif leg_type == "stop":
            stop_loss_leg_id = str(leg.get("id", ""))

    return {
        "attempted": True,
        "placed": True,
        "symbol": symbol,
        "shares": final_shares,
        "estimated_notional": estimated_notional,
        "entry": round(entry, 4),
        "stop": round(stop, 4),
        "target": round(target, 4),
        "max_notional": round(max_notional, 2),
        "client_order_id": client_order_id,
        "alpaca_order_id": str(order.get("id", "")),
        "alpaca_order_status": str(order.get("status", "")),
        "alpaca_order_class": str(order.get("order_class", "")),
        "alpaca_take_profit_order_id": take_profit_leg_id,
        "alpaca_stop_loss_order_id": stop_loss_leg_id,
        "account_buying_power": round(buying_power, 2),
        "account_cash": round(cash, 2),
    }