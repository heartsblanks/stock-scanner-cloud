import os
from typing import Any

from brokers import get_paper_broker, get_paper_broker_config
from brokers.alpaca_adapter import AlpacaPaperBroker
from brokers.ibkr_adapter import IbkrPaperBroker
from brokers.ibkr_bridge_client import ibkr_bridge_enabled, ibkr_bridge_get
from core.logging_utils import log_exception, log_info, log_warning
from orchestration.paper_trade_context import (
    get_latest_open_paper_trade_for_symbol as context_get_latest_open_paper_trade_for_symbol,
)
from orchestration.scan_context import (
    ALPACA_SCHEDULED_MODE_ORDER,
    IBKR_SCHEDULED_MODE_ORDER,
    to_float_or_none,
)
from storage import get_latest_mode_ranking_order, refresh_mode_rankings


PAPER_TRADE_MIN_CONFIDENCE = int(os.getenv("PAPER_TRADE_MIN_CONFIDENCE", "70"))
IBKR_PAPER_TRADE_MIN_CONFIDENCE = int(
    os.getenv("IBKR_PAPER_TRADE_MIN_CONFIDENCE", str(PAPER_TRADE_MIN_CONFIDENCE))
)
ALPACA_MODE_RANKING_WINDOW_DAYS = max(1, int(os.getenv("ALPACA_MODE_RANKING_WINDOW_DAYS", "5")))
ALPACA_MODE_RANKING_MIN_CLOSED_TRADES = max(1, int(os.getenv("ALPACA_MODE_RANKING_MIN_CLOSED_TRADES", "2")))

PAPER_BROKER = get_paper_broker()
PAPER_BROKER_CONFIG = get_paper_broker_config()
ALPACA_PAPER_BROKER = AlpacaPaperBroker()
IBKR_PAPER_BROKER = IbkrPaperBroker()

place_paper_bracket_order_from_trade = PAPER_BROKER.place_paper_bracket_order_from_trade
get_open_positions = PAPER_BROKER.get_open_positions
close_position = PAPER_BROKER.close_position
cancel_open_orders_for_symbol = PAPER_BROKER.cancel_open_orders_for_symbol
sync_order_by_id = PAPER_BROKER.sync_order_by_id
get_order_by_id = PAPER_BROKER.get_order_by_id


def _broker_instance_by_name(broker_name: str):
    normalized = str(broker_name or "").strip().upper()
    if normalized == "IBKR":
        return IBKR_PAPER_BROKER
    return ALPACA_PAPER_BROKER


def sync_order_by_id_for_broker(broker_name: str, order_id: str) -> dict[str, Any]:
    broker = _broker_instance_by_name(broker_name)
    return broker.sync_order_by_id(order_id)


def get_open_positions_for_broker_name(broker_name: str) -> list[dict[str, Any]]:
    broker = _broker_instance_by_name(broker_name)
    return broker.get_open_positions()


def get_open_orders_for_broker_name(broker_name: str) -> list[dict[str, Any]]:
    broker = _broker_instance_by_name(broker_name)
    return broker.get_open_orders()


def get_open_state_for_broker_name(broker_name: str) -> dict[str, Any]:
    broker = _broker_instance_by_name(broker_name)
    get_open_state = getattr(broker, "get_open_state", None)
    if callable(get_open_state):
        return get_open_state() or {}
    return {
        "positions": broker.get_open_positions(),
        "orders": broker.get_open_orders(),
    }


def close_position_for_broker_name(broker_name: str, symbol: str):
    broker = _broker_instance_by_name(broker_name)
    return broker.close_position(symbol)


def place_paper_orders_from_trade(trade: dict[str, Any], max_notional: float | None = None) -> list[dict]:
    results: list[dict] = []

    primary_result = ALPACA_PAPER_BROKER.place_paper_bracket_order_from_trade(trade, max_notional=max_notional)
    if isinstance(primary_result, dict):
        primary_result.setdefault("broker", "ALPACA")
        results.append(primary_result)

    if PAPER_BROKER_CONFIG.shadow_mode_enabled and ibkr_bridge_enabled():
        try:
            ibkr_result = IBKR_PAPER_BROKER.place_paper_bracket_order_from_trade(trade, max_notional=max_notional)
        except Exception as exc:
            ibkr_result = {
                "attempted": True,
                "placed": False,
                "broker": "IBKR",
                "reason": "ibkr_shadow_exception",
                "details": str(exc),
            }
        if isinstance(ibkr_result, dict):
            ibkr_result.setdefault("broker", "IBKR")
            results.append(ibkr_result)

    return results


def place_alpaca_paper_orders_from_trade(trade: dict[str, Any], max_notional: float | None = None) -> list[dict]:
    result = ALPACA_PAPER_BROKER.place_paper_bracket_order_from_trade(trade, max_notional=max_notional)
    if isinstance(result, dict):
        result.setdefault("broker", "ALPACA")
        return [result]
    return []


def place_ibkr_paper_orders_from_trade(trade: dict[str, Any], max_notional: float | None = None) -> list[dict]:
    try:
        result = IBKR_PAPER_BROKER.place_paper_bracket_order_from_trade(trade, max_notional=max_notional)
    except Exception as exc:
        result = {
            "attempted": True,
            "placed": False,
            "broker": "IBKR",
            "reason": "ibkr_shadow_exception",
            "details": str(exc),
        }
    if isinstance(result, dict):
        result.setdefault("broker", "IBKR")
        return [result]
    return []


def _account_equity_from_broker_account(account: dict[str, Any] | None) -> float:
    if not isinstance(account, dict):
        return 0.0
    try:
        return float(account.get("equity") or 0.0)
    except Exception:
        return 0.0


def resolve_alpaca_account_size(payload: dict[str, Any]) -> float:
    del payload
    account = ALPACA_PAPER_BROKER.get_account()
    equity = _account_equity_from_broker_account(account)
    if equity > 0:
        return equity
    raise ValueError("Unable to resolve Alpaca account equity")


def resolve_ibkr_account_size(payload: dict[str, Any]) -> float:
    fallback = to_float_or_none(
        payload.get("ibkr_account_size")
        or payload.get("shadow_account_size")
        or os.getenv("IBKR_SHADOW_ACCOUNT_SIZE_FALLBACK")
        or "1000000"
    )
    try:
        account = IBKR_PAPER_BROKER.get_account()
        equity = _account_equity_from_broker_account(account)
        if equity > 0:
            return equity
    except Exception as exc:
        log_exception(
            "Failed to resolve IBKR account equity; using fallback",
            exc,
            component="runtime_context",
            operation="resolve_ibkr_account_size",
        )
    if fallback is not None and fallback > 0:
        return float(fallback)
    raise ValueError("Unable to resolve IBKR account equity")


def resolve_ibkr_shadow_account_size(payload: dict[str, Any]) -> float:
    fallback = to_float_or_none(
        payload.get("ibkr_account_size")
        or payload.get("shadow_account_size")
        or os.getenv("IBKR_SHADOW_ACCOUNT_SIZE_FALLBACK")
        or "1000000"
    )
    if fallback is not None and fallback > 0:
        return float(fallback)
    return 1000000.0


def resolve_alpaca_scheduled_mode_order() -> list[str]:
    try:
        ranked_modes = get_latest_mode_ranking_order(
            broker="ALPACA",
            expected_modes=ALPACA_SCHEDULED_MODE_ORDER,
            window_days=ALPACA_MODE_RANKING_WINDOW_DAYS,
        )
        if ranked_modes:
            return ranked_modes
    except Exception as exc:
        log_exception(
            "Failed to resolve latest Alpaca mode ranking order; falling back to static order",
            exc,
            component="runtime_context",
            operation="resolve_alpaca_scheduled_mode_order",
        )
    return list(ALPACA_SCHEDULED_MODE_ORDER)


def refresh_alpaca_mode_rankings(*, ranking_date: str | None = None) -> dict[str, Any]:
    result = refresh_mode_rankings(
        broker="ALPACA",
        expected_modes=ALPACA_SCHEDULED_MODE_ORDER,
        window_days=ALPACA_MODE_RANKING_WINDOW_DAYS,
        as_of_date=ranking_date,
        min_closed_trade_count=ALPACA_MODE_RANKING_MIN_CLOSED_TRADES,
    )
    return {
        "ok": True,
        "message": "alpaca mode rankings refreshed",
        **result,
    }


def get_current_open_position_state_for_broker(broker) -> tuple[int, float]:
    try:
        positions = broker.get_open_positions() or []
    except Exception:
        return 0, 0.0

    open_count = 0
    open_exposure = 0.0
    for position in positions:
        symbol = str(position.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        open_count += 1
        market_value = to_float_or_none(position.get("market_value"))
        if market_value is not None:
            open_exposure += abs(market_value)
            continue
        qty = to_float_or_none(position.get("qty"))
        current_price = to_float_or_none(position.get("current_price"))
        if qty is not None and current_price is not None:
            open_exposure += abs(qty * current_price)
    return open_count, open_exposure


def get_risk_exposure_summary_for_broker(broker) -> dict[str, Any]:
    account_size = 0.0
    try:
        account_size = _account_equity_from_broker_account(broker.get_account())
    except Exception as exc:
        log_exception(
            "Failed to resolve broker account for risk summary",
            exc,
            component="runtime_context",
            operation="get_risk_exposure_summary_for_broker",
        )
    open_count, open_exposure = get_current_open_position_state_for_broker(broker)
    return {
        "account_size": account_size,
        "open_position_count": open_count,
        "total_open_exposure": open_exposure,
        "daily_realized_pnl": 0.0,
        "daily_unrealized_pnl": 0.0,
    }


def get_ibkr_shadow_risk_exposure_summary(payload: dict[str, Any]) -> dict[str, Any]:
    open_count, open_exposure = get_current_open_position_state_for_broker(IBKR_PAPER_BROKER)
    return {
        "account_size": resolve_ibkr_shadow_account_size(payload),
        "open_position_count": open_count,
        "total_open_exposure": open_exposure,
        "daily_realized_pnl": 0.0,
        "daily_unrealized_pnl": 0.0,
    }


def get_latest_open_paper_trade_for_symbol_for_broker(symbol: str, broker_name: str) -> dict | None:
    return context_get_latest_open_paper_trade_for_symbol(symbol, broker=broker_name)


def fetch_ibkr_intraday(symbol: str, interval: str = "1min", outputsize: int | None = None) -> list[dict]:
    params: dict[str, Any] = {"symbol": symbol, "interval": interval}
    if outputsize is not None:
        params["outputsize"] = int(outputsize)
    timeout_seconds = int(os.getenv("IBKR_BRIDGE_MARKET_DATA_TIMEOUT_SECONDS", "12"))
    log_info(
        "IBKR intraday fetch requested",
        component="runtime_context",
        operation="fetch_ibkr_intraday",
        broker="IBKR",
        symbol=symbol,
        interval=interval,
        outputsize=outputsize,
        timeout=timeout_seconds,
    )
    try:
        candles = ibkr_bridge_get(
            "/market-data/intraday",
            params=params,
            timeout=timeout_seconds,
        ) or []
        if not candles:
            log_warning(
                "IBKR intraday fetch returned no candles",
                component="runtime_context",
                operation="fetch_ibkr_intraday",
                broker="IBKR",
                symbol=symbol,
                interval=interval,
                outputsize=outputsize,
                timeout=timeout_seconds,
                duration="2 D",
                bar_size=("1 min" if str(interval).strip().lower() == "1min" else "5 mins" if str(interval).strip().lower() == "5min" else None),
                what_to_show="TRADES",
                use_rth=True,
            )
        log_info(
            "IBKR intraday fetch completed",
            component="runtime_context",
            operation="fetch_ibkr_intraday",
            broker="IBKR",
            symbol=symbol,
            interval=interval,
            outputsize=outputsize,
            candle_count=len(candles),
            last_bar_datetime=(candles[-1].get("datetime") if candles else None),
        )
        return candles
    except Exception as exc:
        log_exception(
            "IBKR intraday fetch failed",
            exc,
            component="runtime_context",
            operation="fetch_ibkr_intraday",
            broker="IBKR",
            symbol=symbol,
            interval=interval,
            outputsize=outputsize,
            timeout=timeout_seconds,
        )
        raise
