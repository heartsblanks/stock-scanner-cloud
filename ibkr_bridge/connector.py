from __future__ import annotations

import asyncio
import math
import os
from dataclasses import dataclass
from typing import Any

from core.logging_utils import log_exception, log_info


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class IbkrConnectionConfig:
    host: str
    port: int
    client_id: int
    account_id: str
    readonly: bool
    timeout_seconds: int


def get_ibkr_connection_config() -> IbkrConnectionConfig:
    return IbkrConnectionConfig(
        host=str(os.getenv("IBKR_HOST", "127.0.0.1")).strip() or "127.0.0.1",
        port=int(os.getenv("IBKR_PORT", "4002")),
        client_id=int(os.getenv("IBKR_CLIENT_ID", "101")),
        account_id=str(os.getenv("IBKR_ACCOUNT_ID", "")).strip(),
        readonly=str(os.getenv("IBKR_READONLY", "false")).strip().lower() == "true",
        timeout_seconds=int(os.getenv("IBKR_TIMEOUT_SECONDS", "10")),
    )


class IbkrGatewayClient:
    def __init__(self, config: IbkrConnectionConfig | None = None) -> None:
        self.config = config or get_ibkr_connection_config()
        self._ib = None

    def _ensure_event_loop(self) -> None:
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())

    def _load_ib_class(self):
        self._ensure_event_loop()
        try:
            from ib_insync import IB
        except ImportError as exc:
            raise RuntimeError(
                "ib_insync is not installed on the IBKR bridge host. "
                "Install requirements after adding the IBKR bridge dependency."
            ) from exc
        return IB

    def _load_order_classes(self):
        self._ensure_event_loop()
        try:
            from ib_insync import LimitOrder, MarketOrder, StopOrder, Stock
        except ImportError as exc:
            raise RuntimeError(
                "ib_insync is not installed on the IBKR bridge host. "
                "Install requirements after adding the IBKR bridge dependency."
            ) from exc
        return LimitOrder, MarketOrder, StopOrder, Stock

    def _connect(self):
        self._ensure_event_loop()
        if self._ib is not None and self._ib.isConnected():
            return self._ib

        ib_class = self._load_ib_class()
        ib = ib_class()
        ib.connect(
            self.config.host,
            self.config.port,
            clientId=self.config.client_id,
            readonly=self.config.readonly,
            timeout=self.config.timeout_seconds,
        )
        self._ib = ib
        return ib

    def _disconnect(self) -> None:
        ib = self._ib
        self._ib = None
        if ib is None:
            return
        try:
            if ib.isConnected():
                ib.disconnect()
        except Exception:
            pass

    def _reset_connection(self):
        self._disconnect()
        return self._connect()

    def _resolve_account_id(self, ib) -> str:
        if self.config.account_id:
            return self.config.account_id

        managed_accounts = list(ib.managedAccounts() or [])
        if managed_accounts:
            return str(managed_accounts[0]).strip()

        summary_rows = ib.accountSummary()
        for row in summary_rows or []:
            account = str(getattr(row, "account", "")).strip()
            if account:
                return account

        return ""

    def _summary_map(self) -> tuple[Any, str, dict[str, str]]:
        ib = self._connect()
        account_id = self._resolve_account_id(ib)
        rows = ib.accountSummary()
        summary: dict[str, str] = {}
        for row in rows or []:
            row_account = str(getattr(row, "account", "")).strip()
            if account_id and row_account and row_account != account_id:
                continue
            tag = str(getattr(row, "tag", "")).strip()
            if not tag:
                continue
            summary[tag] = str(getattr(row, "value", "")).strip()
        return ib, account_id, summary

    def health_snapshot(self) -> dict[str, Any]:
        snapshot = {
            "configured": True,
            "host": self.config.host,
            "port": self.config.port,
            "client_id": self.config.client_id,
            "readonly": self.config.readonly,
        }
        try:
            self._load_ib_class()
            snapshot["dependency_ready"] = True
        except RuntimeError as exc:
            snapshot["dependency_ready"] = False
            snapshot["dependency_error"] = str(exc)
        return snapshot

    def get_account(self) -> dict[str, Any]:
        _ib, account_id, summary = self._summary_map()
        equity = _to_float(summary.get("NetLiquidation"))
        cash = _to_float(summary.get("TotalCashValue"))
        buying_power = _to_float(summary.get("BuyingPower"))
        available_funds = _to_float(summary.get("AvailableFunds"))
        regt_buying_power = _to_float(summary.get("RegTEquity"))
        return {
            "account_id": account_id,
            "status": "ACTIVE",
            "equity": equity,
            "cash": cash,
            "buying_power": buying_power if buying_power > 0 else available_funds,
            "regt_buying_power": regt_buying_power,
            "available_funds": available_funds,
            "raw_summary": summary,
        }

    def get_positions(self) -> list[dict[str, Any]]:
        ib = self._connect()
        account_id = self._resolve_account_id(ib)
        positions = [
            row
            for row in (ib.positions() or [])
            if not account_id or str(getattr(row, "account", "")).strip() == account_id
        ]
        contracts = [row.contract for row in positions if getattr(row, "contract", None) is not None]
        tickers = ib.reqTickers(*contracts) if contracts else []
        ticker_by_conid = {
            int(getattr(ticker.contract, "conId", 0)): ticker
            for ticker in tickers or []
            if getattr(getattr(ticker, "contract", None), "conId", None) is not None
        }

        normalized: list[dict[str, Any]] = []
        for row in positions:
            contract = row.contract
            qty = _to_float(getattr(row, "position", 0.0))
            avg_entry_price = _to_float(getattr(row, "avgCost", 0.0))
            ticker = ticker_by_conid.get(int(getattr(contract, "conId", 0)))
            current_price = None
            if ticker is not None:
                try:
                    current_price = ticker.marketPrice()
                except Exception:
                    current_price = None
            current_price_value = _to_float(current_price, avg_entry_price)
            market_value = qty * current_price_value
            unrealized_pl = (current_price_value - avg_entry_price) * qty

            normalized.append({
                "symbol": str(getattr(contract, "symbol", "")).strip().upper(),
                "exchange": str(getattr(contract, "exchange", "")).strip(),
                "currency": str(getattr(contract, "currency", "")).strip(),
                "qty": qty,
                "side": "long" if qty >= 0 else "short",
                "avg_entry_price": round(avg_entry_price, 4),
                "current_price": round(current_price_value, 4),
                "market_value": round(market_value, 2),
                "unrealized_pl": round(unrealized_pl, 2),
                "account_id": str(getattr(row, "account", "")).strip(),
            })

        return normalized

    def get_intraday_candles(self, symbol: str, interval: str = "1min", outputsize: int | None = None) -> list[dict[str, Any]]:
        normalized_symbol = str(symbol).strip().upper()
        if not normalized_symbol:
            raise RuntimeError("symbol is required")

        bar_size_by_interval = {
            "1min": "1 min",
            "5min": "5 mins",
        }
        bar_size = bar_size_by_interval.get(str(interval).strip().lower())
        if not bar_size:
            raise RuntimeError("unsupported interval")

        ib = self._reset_connection()
        _LimitOrder, _MarketOrder, _StopOrder, Stock = self._load_order_classes()
        contract = Stock(normalized_symbol, "SMART", "USD")
        log_info(
            "IBKR bridge intraday request started",
            component="ibkr_bridge",
            operation="get_intraday_candles",
            symbol=normalized_symbol,
            interval=interval,
            outputsize=outputsize,
            timeout=self.config.timeout_seconds,
        )
        try:
            ib.qualifyContracts(contract)
            log_info(
                "IBKR bridge contract qualified",
                component="ibkr_bridge",
                operation="get_intraday_candles",
                symbol=normalized_symbol,
                con_id=getattr(contract, "conId", None),
                exchange=getattr(contract, "exchange", None),
                primary_exchange=getattr(contract, "primaryExchange", None),
            )

            bars = ib.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr="2 D",
                barSizeSetting=bar_size,
                whatToShow="TRADES",
                useRTH=True,
                formatDate=1,
                timeout=float(self.config.timeout_seconds),
            )
        except Exception as exc:
            self._disconnect()
            log_exception(
                "IBKR bridge intraday request failed",
                exc,
                component="ibkr_bridge",
                operation="get_intraday_candles",
                symbol=normalized_symbol,
                interval=interval,
                outputsize=outputsize,
                timeout=self.config.timeout_seconds,
            )
            raise

        normalized: list[dict[str, Any]] = []
        trimmed_bars = list(bars or [])
        if outputsize and outputsize > 0:
            trimmed_bars = trimmed_bars[-int(outputsize):]

        for bar in trimmed_bars:
            bar_dt = getattr(bar, "date", None)
            if hasattr(bar_dt, "strftime"):
                dt_str = bar_dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                dt_str = str(bar_dt)
            normalized.append({
                "datetime": dt_str,
                "open": _to_float(getattr(bar, "open", 0.0)),
                "high": _to_float(getattr(bar, "high", 0.0)),
                "low": _to_float(getattr(bar, "low", 0.0)),
                "close": _to_float(getattr(bar, "close", 0.0)),
                "volume": _to_float(getattr(bar, "volume", 0.0)),
            })
        log_info(
            "IBKR bridge intraday request completed",
            component="ibkr_bridge",
            operation="get_intraday_candles",
            symbol=normalized_symbol,
            interval=interval,
            outputsize=outputsize,
            count=len(normalized),
        )
        self._disconnect()
        return normalized

    def _normalize_trade(self, trade: Any) -> dict[str, Any]:
        contract = getattr(trade, "contract", None)
        order = getattr(trade, "order", None)
        order_status = getattr(trade, "orderStatus", None)
        return {
            "id": str(getattr(order, "orderId", "")).strip(),
            "client_order_id": str(getattr(order, "orderRef", "")).strip(),
            "symbol": str(getattr(contract, "symbol", "")).strip().upper(),
            "side": str(getattr(order, "action", "")).strip().lower(),
            "qty": _to_float(getattr(order, "totalQuantity", 0.0)),
            "filled_qty": _to_float(getattr(order_status, "filled", 0.0)),
            "remaining_qty": _to_float(getattr(order_status, "remaining", 0.0)),
            "type": str(getattr(order, "orderType", "")).strip().lower(),
            "status": str(getattr(order_status, "status", "")).strip(),
            "parent_id": str(getattr(order, "parentId", "")).strip(),
            "limit_price": _to_float(getattr(order, "lmtPrice", 0.0)),
            "stop_price": _to_float(getattr(order, "auxPrice", 0.0)),
        }

    def get_open_orders(self) -> list[dict[str, Any]]:
        ib = self._connect()
        return [self._normalize_trade(trade) for trade in (ib.openTrades() or [])]

    def get_order(self, order_id: str) -> dict[str, Any] | None:
        normalized_order_id = str(order_id).strip()
        if not normalized_order_id:
            return None

        ib = self._connect()
        for trade in ib.openTrades() or []:
            trade_order_id = str(getattr(getattr(trade, "order", None), "orderId", "")).strip()
            if trade_order_id == normalized_order_id:
                return self._normalize_trade(trade)
        return None

    def place_paper_bracket_order(self, trade: dict[str, Any], max_notional: float | None = None) -> dict[str, Any]:
        metrics = trade.get("metrics", {}) if isinstance(trade, dict) else {}
        symbol = str(metrics.get("symbol", "")).strip().upper()
        direction = str(metrics.get("direction", "BUY")).strip().upper() or "BUY"
        entry = _to_float(metrics.get("entry"))
        stop = _to_float(metrics.get("stop"))
        target = _to_float(metrics.get("target"))
        scanner_shares = int(_to_float(metrics.get("shares"), 0))
        per_trade_notional = _to_float(metrics.get("per_trade_notional"), 0.0)
        remaining_allocatable_capital = _to_float(metrics.get("remaining_allocatable_capital"), 0.0)

        if not symbol:
            return {"attempted": False, "placed": False, "broker": "IBKR", "reason": "missing_symbol"}
        if entry <= 0 or stop <= 0 or target <= 0:
            return {"attempted": False, "placed": False, "broker": "IBKR", "symbol": symbol, "reason": "invalid_price_inputs"}
        if direction not in {"BUY", "SELL"}:
            return {"attempted": False, "placed": False, "broker": "IBKR", "symbol": symbol, "reason": "invalid_direction"}
        if direction == "BUY" and not (stop < entry < target):
            return {"attempted": False, "placed": False, "broker": "IBKR", "symbol": symbol, "reason": "invalid_long_bracket"}
        if direction == "SELL" and not (target < entry < stop):
            return {"attempted": False, "placed": False, "broker": "IBKR", "symbol": symbol, "reason": "invalid_short_bracket"}

        notional_cap_candidates = [value for value in (max_notional, per_trade_notional, remaining_allocatable_capital) if _to_float(value) > 0]
        notional_cap = min(notional_cap_candidates) if notional_cap_candidates else 0.0
        capped_shares = int(math.floor(notional_cap / entry)) if notional_cap > 0 else 0
        final_shares = min(scanner_shares, capped_shares) if scanner_shares > 0 and capped_shares > 0 else max(scanner_shares, capped_shares)
        if final_shares <= 0:
            return {"attempted": False, "placed": False, "broker": "IBKR", "symbol": symbol, "reason": "position_size_too_small"}

        ib = self._connect()
        LimitOrder, MarketOrder, StopOrder, Stock = self._load_order_classes()
        contract = Stock(symbol, "SMART", "USD")
        ib.qualifyContracts(contract)

        action = "BUY" if direction == "BUY" else "SELL"
        exit_action = "SELL" if action == "BUY" else "BUY"
        client_order_id = f"scanner-{symbol}-{direction}-{int(round(entry * 10000))}-{final_shares}"

        base_order_id = ib.client.getReqId()
        parent = MarketOrder(action, final_shares, transmit=False)
        parent.orderId = base_order_id
        parent.orderRef = client_order_id
        parent.tif = "DAY"

        take_profit = LimitOrder(exit_action, final_shares, round(target, 2), transmit=False)
        take_profit.orderId = base_order_id + 1
        take_profit.parentId = base_order_id
        take_profit.orderRef = client_order_id
        take_profit.tif = "GTC"

        stop_loss = StopOrder(exit_action, final_shares, round(stop, 2), transmit=True)
        stop_loss.orderId = base_order_id + 2
        stop_loss.parentId = base_order_id
        stop_loss.orderRef = client_order_id
        stop_loss.tif = "GTC"

        try:
            parent_trade = ib.placeOrder(contract, parent)
            take_profit_trade = ib.placeOrder(contract, take_profit)
            stop_loss_trade = ib.placeOrder(contract, stop_loss)
        except Exception as exc:
            return {
                "attempted": True,
                "placed": False,
                "broker": "IBKR",
                "symbol": symbol,
                "reason": "ibkr_order_rejected",
                "details": str(exc),
            }

        estimated_notional = round(final_shares * entry, 2)
        parent_status = str(getattr(getattr(parent_trade, "orderStatus", None), "status", "")).strip()

        return {
            "attempted": True,
            "placed": True,
            "broker": "IBKR",
            "symbol": symbol,
            "shares": final_shares,
            "estimated_notional": estimated_notional,
            "client_order_id": client_order_id,
            "broker_order_id": str(base_order_id),
            "broker_parent_order_id": str(base_order_id),
            "broker_order_status": parent_status,
            "take_profit_order_id": str(base_order_id + 1),
            "stop_loss_order_id": str(base_order_id + 2),
            "order_id": str(base_order_id),
            "parent_order_id": str(base_order_id),
            "order_status": parent_status,
        }

    def cancel_orders_by_symbol(self, symbol: str) -> list[str]:
        normalized_symbol = str(symbol).strip().upper()
        if not normalized_symbol:
            raise RuntimeError("symbol is required")

        ib = self._connect()
        canceled_order_ids: list[str] = []
        for trade in ib.openTrades() or []:
            contract = getattr(trade, "contract", None)
            if str(getattr(contract, "symbol", "")).strip().upper() != normalized_symbol:
                continue

            order = getattr(trade, "order", None)
            order_id = str(getattr(order, "orderId", "")).strip()
            ib.cancelOrder(order)
            if order_id:
                canceled_order_ids.append(order_id)

        return canceled_order_ids

    def close_position(self, symbol: str) -> dict[str, Any]:
        normalized_symbol = str(symbol).strip().upper()
        if not normalized_symbol:
            raise RuntimeError("symbol is required")

        ib = self._connect()
        account_id = self._resolve_account_id(ib)
        target_position = None
        for row in ib.positions() or []:
            if account_id and str(getattr(row, "account", "")).strip() != account_id:
                continue
            contract = getattr(row, "contract", None)
            if str(getattr(contract, "symbol", "")).strip().upper() == normalized_symbol:
                target_position = row
                break

        if target_position is None:
            return {
                "attempted": False,
                "placed": False,
                "symbol": normalized_symbol,
                "reason": "no_open_position",
            }

        qty = _to_float(getattr(target_position, "position", 0.0))
        if qty == 0:
            return {
                "attempted": False,
                "placed": False,
                "symbol": normalized_symbol,
                "reason": "no_open_position",
            }

        MarketOrder, Stock = self._load_order_classes()
        contract = getattr(target_position, "contract", None)
        if contract is None:
            contract = Stock(normalized_symbol, "SMART", "USD")
            ib.qualifyContracts(contract)

        action = "SELL" if qty > 0 else "BUY"
        order = MarketOrder(action, abs(qty))
        order.orderRef = f"scanner-close-{normalized_symbol}"
        trade = ib.placeOrder(contract, order)

        normalized_trade = self._normalize_trade(trade)
        return {
            "attempted": True,
            "placed": True,
            "symbol": normalized_symbol,
            "action": action.lower(),
            "qty": abs(qty),
            "order_id": normalized_trade.get("id", ""),
            "status": normalized_trade.get("status", ""),
        }


_client: IbkrGatewayClient | None = None


def get_ibkr_client() -> IbkrGatewayClient:
    global _client
    if _client is None:
        _client = IbkrGatewayClient()
    return _client
