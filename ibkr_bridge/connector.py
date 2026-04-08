from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import Any


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
            from ib_insync import MarketOrder, Stock
        except ImportError as exc:
            raise RuntimeError(
                "ib_insync is not installed on the IBKR bridge host. "
                "Install requirements after adding the IBKR bridge dependency."
            ) from exc
        return MarketOrder, Stock

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
