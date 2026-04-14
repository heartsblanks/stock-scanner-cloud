from __future__ import annotations

import asyncio
import math
import os
import time
from dataclasses import dataclass
from typing import Any

from core.logging_utils import log_exception, log_info, log_warning


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _truthy_env(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, str(default))).strip().lower()
    return raw in {"1", "true", "yes", "on"}


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

    def _is_open_order_status(self, status: str) -> bool:
        normalized = str(status or "").strip().lower()
        return normalized not in {"", "filled", "cancelled", "apicancelled", "inactive"}

    def _find_position_row(self, ib, symbol: str):
        account_id = self._resolve_account_id(ib)
        normalized_symbol = str(symbol).strip().upper()
        for row in ib.positions() or []:
            if account_id and str(getattr(row, "account", "")).strip() != account_id:
                continue
            contract = getattr(row, "contract", None)
            if str(getattr(contract, "symbol", "")).strip().upper() == normalized_symbol:
                return row
        return None

    def _position_is_open(self, ib, symbol: str) -> bool:
        row = self._find_position_row(ib, symbol)
        if row is None:
            return False
        return _to_float(getattr(row, "position", 0.0)) != 0.0

    def _close_poll_config(self) -> tuple[int, float]:
        try:
            attempts = max(1, int(os.getenv("IBKR_CLOSE_POLL_ATTEMPTS", "12")))
        except Exception:
            attempts = 12
        try:
            interval_seconds = max(0.25, float(os.getenv("IBKR_CLOSE_POLL_INTERVAL_SECONDS", "1.0")))
        except Exception:
            interval_seconds = 1.0
        return attempts, interval_seconds

    def _order_status_snapshot(self, trade: Any) -> dict[str, Any]:
        order = getattr(trade, "order", None)
        order_status = getattr(trade, "orderStatus", None)
        fills = list(getattr(trade, "fills", []) or [])
        latest_fill = fills[-1] if fills else None
        execution = getattr(latest_fill, "execution", None)
        execution_time = getattr(execution, "time", None)
        if hasattr(execution_time, "isoformat"):
            filled_at = execution_time.isoformat()
        elif execution_time is not None:
            filled_at = str(execution_time)
        else:
            filled_at = ""
        return {
            "order_id": str(getattr(order, "orderId", "")).strip(),
            "status": str(getattr(order_status, "status", "")).strip(),
            "filled_qty": _to_float(getattr(order_status, "filled", 0.0)),
            "remaining_qty": _to_float(getattr(order_status, "remaining", 0.0)),
            "avg_fill_price": _to_float(getattr(order_status, "avgFillPrice", 0.0)),
            "last_fill_price": _to_float(getattr(order_status, "lastFillPrice", 0.0)),
            "why_held": str(getattr(order_status, "whyHeld", "")).strip(),
            "filled_at": filled_at,
        }

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

    def _position_market_data_enrichment_enabled(self) -> bool:
        return _truthy_env("IBKR_ENRICH_POSITIONS_WITH_TICKERS", False)

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
        tickers = []
        if self._position_market_data_enrichment_enabled():
            try:
                tickers = ib.reqTickers(*contracts) if contracts else []
            except Exception as exc:
                log_exception(
                    "IBKR bridge position ticker enrichment failed; using avg cost fallback",
                    exc,
                    component="ibkr_bridge",
                    operation="get_positions",
                    contract_count=len(contracts),
                )
                tickers = []
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
        if not normalized:
            log_warning(
                "IBKR bridge intraday request returned no bars",
                component="ibkr_bridge",
                operation="get_intraday_candles",
                symbol=normalized_symbol,
                interval=interval,
                outputsize=outputsize,
                duration="2 D",
                bar_size=bar_size,
                what_to_show="TRADES",
                use_rth=True,
                con_id=getattr(contract, "conId", None),
                exchange=getattr(contract, "exchange", None),
                primary_exchange=getattr(contract, "primaryExchange", None),
                timeout=self.config.timeout_seconds,
            )
        log_info(
            "IBKR bridge intraday request completed",
            component="ibkr_bridge",
            operation="get_intraday_candles",
            symbol=normalized_symbol,
            interval=interval,
            outputsize=outputsize,
            count=len(normalized),
            duration="2 D",
            bar_size=bar_size,
            what_to_show="TRADES",
            use_rth=True,
            last_bar_datetime=(normalized[-1]["datetime"] if normalized else None),
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

    def _fetch_recent_fills(self, ib) -> list[Any]:
        try:
            fills = list(ib.reqExecutions() or [])
            if fills:
                return fills
        except Exception as exc:
            log_exception(
                "IBKR bridge execution fetch failed",
                exc,
                component="ibkr_bridge",
                operation="fetch_recent_fills",
            )
        return list(getattr(ib, "fills", lambda: [])() or [])

    def _fetch_completed_trades(self, ib) -> list[Any]:
        fetch_fn = getattr(ib, "reqCompletedOrders", None)
        if fetch_fn is None:
            return []
        try:
            completed = list(fetch_fn(False) or [])
            if completed:
                return completed
        except TypeError:
            try:
                completed = list(fetch_fn() or [])
                if completed:
                    return completed
            except Exception as exc:
                log_exception(
                    "IBKR bridge completed-order fetch failed",
                    exc,
                    component="ibkr_bridge",
                    operation="fetch_completed_trades",
                )
        except Exception as exc:
            log_exception(
                "IBKR bridge completed-order fetch failed",
                exc,
                component="ibkr_bridge",
                operation="fetch_completed_trades",
            )
        return []

    def _execution_time_value(self, fill: Any) -> str:
        execution = getattr(fill, "execution", None)
        execution_time = getattr(execution, "time", None)
        if hasattr(execution_time, "isoformat"):
            return execution_time.isoformat()
        if execution_time is None:
            return ""
        return str(execution_time)

    def _sync_order_from_fills(self, ib, order_id: str) -> dict[str, Any]:
        fills = self._fetch_recent_fills(ib)
        if not fills:
            return {
                "id": str(order_id).strip(),
                "status": "unknown",
                "message": "Order was not found in current IBKR fills.",
            }

        order_id_text = str(order_id).strip()
        entry_fills = [
            fill
            for fill in fills
            if str(getattr(getattr(fill, "execution", None), "orderId", "")).strip() == order_id_text
        ]
        if not entry_fills:
            return {
                "id": order_id_text,
                "status": "unknown",
                "message": "Order was not found in current IBKR fills.",
            }

        entry_fill = entry_fills[-1]
        entry_execution = getattr(entry_fill, "execution", None)
        order_ref = str(getattr(entry_execution, "orderRef", "")).strip()
        related_fills = fills
        if order_ref:
            related_fills = [
                fill
                for fill in fills
                if str(getattr(getattr(fill, "execution", None), "orderRef", "")).strip() == order_ref
            ] or entry_fills

        related_fills = sorted(related_fills, key=self._execution_time_value)
        latest_fill = related_fills[-1]
        latest_execution = getattr(latest_fill, "execution", None)
        latest_order_id = str(getattr(latest_execution, "orderId", "")).strip()
        latest_price = _to_float(getattr(latest_execution, "price", 0.0))
        latest_qty = _to_float(getattr(latest_execution, "shares", 0.0))
        latest_avg_price = _to_float(getattr(latest_execution, "avgPrice", 0.0), latest_price)
        latest_side = str(getattr(latest_execution, "side", "")).strip().upper()

        result = {
            "id": order_id_text,
            "status": "filled",
            "parent_order_id": order_id_text,
            "parent_status": "Filled",
            "entry_filled": True,
            "entry_filled_qty": _to_float(getattr(entry_execution, "shares", 0.0)),
            "entry_filled_avg_price": _to_float(getattr(entry_execution, "avgPrice", 0.0), _to_float(getattr(entry_execution, "price", 0.0))),
            "client_order_id": order_ref,
            "symbol": str(getattr(getattr(entry_fill, "contract", None), "symbol", "")).strip().upper(),
        }

        if latest_order_id and latest_order_id != order_id_text:
            result.update(
                {
                    "status": "closed",
                    "exit_event": "MANUAL_CLOSE",
                    "exit_order_id": latest_order_id,
                    "exit_status": "Filled",
                    "exit_price": round(latest_price, 4) if latest_price > 0 else "",
                    "exit_filled_qty": round(latest_qty, 4) if latest_qty > 0 else "",
                    "exit_filled_avg_price": round(latest_avg_price, 4) if latest_avg_price > 0 else "",
                    "exit_reason": "BROKER_FILLED_EXIT",
                    "exit_side": latest_side,
                    "exit_filled_at": self._execution_time_value(latest_fill),
                }
            )

        return result

    def _trade_fill_snapshot(self, trade: Any) -> dict[str, Any]:
        fills = list(getattr(trade, "fills", []) or [])
        latest_fill = fills[-1] if fills else None
        execution = getattr(latest_fill, "execution", None)
        order_status = getattr(trade, "orderStatus", None)
        last_price = _to_float(getattr(execution, "price", 0.0))
        avg_price = _to_float(getattr(execution, "avgPrice", 0.0), last_price)
        filled_qty = _to_float(getattr(execution, "shares", 0.0), _to_float(getattr(order_status, "filled", 0.0)))
        order_status_avg_fill_price = _to_float(getattr(order_status, "avgFillPrice", None), avg_price)
        order_status_last_fill_price = _to_float(getattr(order_status, "lastFillPrice", None), last_price)
        return {
            "filled_qty": filled_qty,
            "avg_fill_price": order_status_avg_fill_price if order_status_avg_fill_price > 0 else avg_price,
            "last_fill_price": order_status_last_fill_price if order_status_last_fill_price > 0 else last_price,
            "filled_at": self._execution_time_value(latest_fill) if latest_fill is not None else "",
        }

    def _sync_order_from_completed_trades(self, ib, order_id: str) -> dict[str, Any]:
        trades = self._fetch_completed_trades(ib)
        if not trades:
            return {
                "id": str(order_id).strip(),
                "status": "unknown",
                "message": "Order was not found in completed IBKR trades.",
            }

        order_id_text = str(order_id).strip()
        entry_trades = [
            trade
            for trade in trades
            if str(getattr(getattr(trade, "order", None), "orderId", "")).strip() == order_id_text
        ]
        if not entry_trades:
            return {
                "id": order_id_text,
                "status": "unknown",
                "message": "Order was not found in completed IBKR trades.",
            }

        entry_trade = entry_trades[-1]
        entry_order = getattr(entry_trade, "order", None)
        entry_contract = getattr(entry_trade, "contract", None)
        entry_status = getattr(entry_trade, "orderStatus", None)
        entry_order_ref = str(getattr(entry_order, "orderRef", "")).strip()
        related_trades = trades
        if entry_order_ref:
            related_trades = [
                trade
                for trade in trades
                if str(getattr(getattr(trade, "order", None), "orderRef", "")).strip() == entry_order_ref
            ] or entry_trades

        def _trade_sort_key(trade: Any) -> tuple[str, str]:
            fill_snapshot = self._trade_fill_snapshot(trade)
            order = getattr(trade, "order", None)
            return (
                str(fill_snapshot.get("filled_at", "") or ""),
                str(getattr(order, "orderId", "") or ""),
            )

        related_trades = sorted(related_trades, key=_trade_sort_key)
        latest_trade = related_trades[-1]
        latest_order = getattr(latest_trade, "order", None)
        latest_status = getattr(latest_trade, "orderStatus", None)
        latest_snapshot = self._trade_fill_snapshot(latest_trade)
        latest_order_id = str(getattr(latest_order, "orderId", "")).strip()
        latest_action = str(getattr(latest_order, "action", "")).strip().upper()

        entry_snapshot = self._trade_fill_snapshot(entry_trade)
        result = {
            "id": order_id_text,
            "status": str(getattr(entry_status, "status", "") or "filled"),
            "parent_order_id": order_id_text,
            "parent_status": str(getattr(entry_status, "status", "") or "Filled"),
            "entry_filled": True,
            "entry_filled_qty": entry_snapshot["filled_qty"],
            "entry_filled_avg_price": entry_snapshot["avg_fill_price"] or entry_snapshot["last_fill_price"],
            "client_order_id": entry_order_ref,
            "symbol": str(getattr(entry_contract, "symbol", "")).strip().upper(),
        }

        if latest_order_id and latest_order_id != order_id_text and latest_action:
            result.update(
                {
                    "status": "closed",
                    "exit_event": "MANUAL_CLOSE",
                    "exit_order_id": latest_order_id,
                    "exit_status": str(getattr(latest_status, "status", "") or "Filled"),
                    "exit_price": round(latest_snapshot["last_fill_price"], 4) if latest_snapshot["last_fill_price"] > 0 else "",
                    "exit_filled_qty": round(latest_snapshot["filled_qty"], 4) if latest_snapshot["filled_qty"] > 0 else "",
                    "exit_filled_avg_price": round(latest_snapshot["avg_fill_price"], 4) if latest_snapshot["avg_fill_price"] > 0 else "",
                    "exit_reason": "BROKER_FILLED_EXIT",
                    "exit_side": latest_action,
                    "exit_filled_at": str(latest_snapshot["filled_at"] or ""),
                }
            )

        return result

    def _fetch_open_trades(self, ib) -> list[Any]:
        trades = list(ib.openTrades() or [])
        if trades:
            return trades

        for fetch_name in ("reqAllOpenOrders", "reqOpenOrders"):
            fetch_fn = getattr(ib, fetch_name, None)
            if fetch_fn is None:
                continue
            try:
                fetched = list(fetch_fn() or [])
            except Exception as exc:
                log_exception(
                    "IBKR bridge open-order fetch failed",
                    exc,
                    component="ibkr_bridge",
                    operation="fetch_open_trades",
                    method=fetch_name,
                )
                continue
            if fetched:
                log_info(
                    "IBKR bridge open-order fetch returned trades",
                    component="ibkr_bridge",
                    operation="fetch_open_trades",
                    method=fetch_name,
                    count=len(fetched),
                )
                return fetched

        fallback_trades = list(getattr(ib, "trades", lambda: [])() or [])
        if fallback_trades:
            log_info(
                "IBKR bridge open-order fetch used trade fallback",
                component="ibkr_bridge",
                operation="fetch_open_trades",
                method="trades",
                count=len(fallback_trades),
            )
        return fallback_trades

    def get_open_orders(self) -> list[dict[str, Any]]:
        ib = self._connect()
        trades = self._fetch_open_trades(ib)
        normalized_trades = [self._normalize_trade(trade) for trade in trades]
        return [
            trade
            for trade in normalized_trades
            if self._is_open_order_status(trade.get("status", "")) and _to_float(trade.get("remaining_qty", 0.0)) > 0
        ]

    def get_order(self, order_id: str) -> dict[str, Any] | None:
        normalized_order_id = str(order_id).strip()
        if not normalized_order_id:
            return None

        ib = self._connect()
        for trade in self._fetch_open_trades(ib):
            trade_order_id = str(getattr(getattr(trade, "order", None), "orderId", "")).strip()
            if trade_order_id == normalized_order_id:
                return self._normalize_trade(trade)
        return None

    def sync_order(self, order_id: str) -> dict[str, Any]:
        normalized_order_id = str(order_id).strip()
        if not normalized_order_id:
            return {"id": "", "status": "unknown", "message": "order_id is required"}

        ib = self._connect()
        fills_result = self._sync_order_from_fills(ib, normalized_order_id)
        if str(fills_result.get("status", "")).strip().lower() != "unknown":
            return fills_result

        completed_result = self._sync_order_from_completed_trades(ib, normalized_order_id)
        if str(completed_result.get("status", "")).strip().lower() != "unknown":
            return completed_result

        for trade in self._fetch_open_trades(ib):
            trade_order_id = str(getattr(getattr(trade, "order", None), "orderId", "")).strip()
            if trade_order_id == normalized_order_id:
                normalized_trade = self._normalize_trade(trade)
                return {
                    "id": normalized_order_id,
                    "status": str(normalized_trade.get("status", "") or "open"),
                    "parent_order_id": normalized_order_id,
                    "parent_status": str(normalized_trade.get("status", "") or ""),
                    "symbol": normalized_trade.get("symbol", ""),
                    "client_order_id": normalized_trade.get("client_order_id", ""),
                }

        return completed_result

    def _open_orders_for_symbol(self, ib, symbol: str) -> list[Any]:
        normalized_symbol = str(symbol or "").strip().upper()
        if not normalized_symbol:
            return []

        trades: list[Any] = []
        for trade in self._fetch_open_trades(ib):
            contract = getattr(trade, "contract", None)
            if str(getattr(contract, "symbol", "")).strip().upper() != normalized_symbol:
                continue

            order_status = str(getattr(getattr(trade, "orderStatus", None), "status", "")).strip()
            remaining_qty = _to_float(getattr(getattr(trade, "orderStatus", None), "remaining", 0.0))
            if self._is_open_order_status(order_status) and remaining_qty > 0:
                trades.append(trade)
        return trades

    def _cancel_settle_config(self) -> tuple[int, float]:
        try:
            attempts = max(1, int(os.getenv("IBKR_CANCEL_SETTLE_POLL_ATTEMPTS", "8")))
        except Exception:
            attempts = 8
        try:
            interval_seconds = max(0.25, float(os.getenv("IBKR_CANCEL_SETTLE_POLL_INTERVAL_SECONDS", "0.5")))
        except Exception:
            interval_seconds = 0.5
        return attempts, interval_seconds

    def _wait_for_symbol_open_orders_to_clear(self, ib, symbol: str) -> list[dict[str, Any]]:
        normalized_symbol = str(symbol or "").strip().upper()
        poll_attempts, poll_interval_seconds = self._cancel_settle_config()
        snapshots: list[dict[str, Any]] = []

        for attempt in range(0, poll_attempts + 1):
            open_trades = self._open_orders_for_symbol(ib, normalized_symbol)
            snapshot = {
                "attempt": attempt,
                "open_order_count": len(open_trades),
                "order_ids": [
                    str(getattr(getattr(trade, "order", None), "orderId", "")).strip()
                    for trade in open_trades
                ],
            }
            snapshots.append(snapshot)
            log_info(
                "IBKR bridge cancel-settle snapshot",
                component="ibkr_bridge",
                operation="close_position",
                symbol=normalized_symbol,
                **snapshot,
            )
            if not open_trades:
                return snapshots
            if attempt == poll_attempts:
                break
            try:
                ib.sleep(poll_interval_seconds)
            except Exception:
                time.sleep(poll_interval_seconds)

        return snapshots

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

        log_info(
            "IBKR bridge paper bracket placement started",
            component="ibkr_bridge",
            operation="place_paper_bracket_order",
            symbol=symbol,
            direction=direction,
            entry=entry,
            stop=stop,
            target=target,
            scanner_shares=scanner_shares,
            final_shares=final_shares,
            notional_cap=round(notional_cap, 2),
        )

        ib = self._connect()
        LimitOrder, MarketOrder, StopOrder, Stock = self._load_order_classes()
        contract = Stock(symbol, "SMART", "USD")
        try:
            ib.qualifyContracts(contract)
            log_info(
                "IBKR bridge paper bracket contract qualified",
                component="ibkr_bridge",
                operation="place_paper_bracket_order",
                symbol=symbol,
                con_id=int(getattr(contract, "conId", 0) or 0),
                exchange=str(getattr(contract, "exchange", "")).strip(),
                primary_exchange=str(getattr(contract, "primaryExchange", "")).strip(),
            )
        except Exception as exc:
            log_exception(
                "IBKR bridge paper bracket contract qualification failed",
                exc,
                component="ibkr_bridge",
                operation="place_paper_bracket_order",
                symbol=symbol,
            )
            return {
                "attempted": True,
                "placed": False,
                "broker": "IBKR",
                "symbol": symbol,
                "reason": "ibkr_contract_qualification_failed",
                "details": str(exc),
            }

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

        log_info(
            "IBKR bridge paper bracket orders prepared",
            component="ibkr_bridge",
            operation="place_paper_bracket_order",
            symbol=symbol,
            client_order_id=client_order_id,
            parent_order_id=base_order_id,
            take_profit_order_id=base_order_id + 1,
            stop_loss_order_id=base_order_id + 2,
        )

        try:
            parent_trade = ib.placeOrder(contract, parent)
            log_info(
                "IBKR bridge parent order submitted",
                component="ibkr_bridge",
                operation="place_paper_bracket_order",
                symbol=symbol,
                order_id=base_order_id,
            )
            take_profit_trade = ib.placeOrder(contract, take_profit)
            log_info(
                "IBKR bridge take profit order submitted",
                component="ibkr_bridge",
                operation="place_paper_bracket_order",
                symbol=symbol,
                order_id=base_order_id + 1,
            )
            stop_loss_trade = ib.placeOrder(contract, stop_loss)
            log_info(
                "IBKR bridge stop loss order submitted",
                component="ibkr_bridge",
                operation="place_paper_bracket_order",
                symbol=symbol,
                order_id=base_order_id + 2,
            )
        except Exception as exc:
            log_exception(
                "IBKR bridge paper bracket placement failed",
                exc,
                component="ibkr_bridge",
                operation="place_paper_bracket_order",
                symbol=symbol,
                client_order_id=client_order_id,
            )
            self._disconnect()
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
        take_profit_status = str(getattr(getattr(take_profit_trade, "orderStatus", None), "status", "")).strip()
        stop_loss_status = str(getattr(getattr(stop_loss_trade, "orderStatus", None), "status", "")).strip()

        log_info(
            "IBKR bridge paper bracket placement completed",
            component="ibkr_bridge",
            operation="place_paper_bracket_order",
            symbol=symbol,
            client_order_id=client_order_id,
            parent_order_id=base_order_id,
            parent_status=parent_status,
            take_profit_status=take_profit_status,
            stop_loss_status=stop_loss_status,
            estimated_notional=estimated_notional,
        )
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
        for trade in self._open_orders_for_symbol(ib, normalized_symbol):
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
        target_position = self._find_position_row(ib, normalized_symbol)

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

        _LimitOrder, MarketOrder, _StopOrder, Stock = self._load_order_classes()
        contract = getattr(target_position, "contract", None)
        if contract is None:
            contract = Stock(normalized_symbol, "SMART", "USD")
            ib.qualifyContracts(contract)

        action = "SELL" if qty > 0 else "BUY"
        canceled_order_ids: list[str] = []
        for trade in self._open_orders_for_symbol(ib, normalized_symbol):
            order = getattr(trade, "order", None)
            order_id = str(getattr(order, "orderId", "")).strip()
            ib.cancelOrder(order)
            if order_id:
                canceled_order_ids.append(order_id)
        cancel_settle_transitions = self._wait_for_symbol_open_orders_to_clear(ib, normalized_symbol)
        order = MarketOrder(action, abs(qty))
        order.orderRef = f"scanner-close-{normalized_symbol}"
        trade = ib.placeOrder(contract, order)

        normalized_trade = self._normalize_trade(trade)
        status_transitions: list[dict[str, Any]] = []
        poll_attempts, poll_interval_seconds = self._close_poll_config()
        last_snapshot = self._order_status_snapshot(trade)
        last_status = str(last_snapshot.get("status", "")).strip()
        status_transitions.append({
            "attempt": 0,
            **last_snapshot,
        })
        log_info(
            "IBKR bridge close-position transition",
            component="ibkr_bridge",
            operation="close_position",
            symbol=normalized_symbol,
            attempt=0,
            **last_snapshot,
        )

        final_snapshot = dict(last_snapshot)
        broker_position_open = self._position_is_open(ib, normalized_symbol)
        terminal_statuses = {"Filled", "Cancelled", "ApiCancelled", "Inactive"}

        for attempt in range(1, poll_attempts + 1):
            if last_status in terminal_statuses and not broker_position_open:
                break
            try:
                ib.sleep(poll_interval_seconds)
            except Exception:
                time.sleep(poll_interval_seconds)

            current_snapshot = self._order_status_snapshot(trade)
            current_status = str(current_snapshot.get("status", "")).strip()
            broker_position_open = self._position_is_open(ib, normalized_symbol)
            if current_status != last_status or attempt == poll_attempts or not broker_position_open:
                transition = {
                    "attempt": attempt,
                    **current_snapshot,
                    "broker_position_open": broker_position_open,
                }
                status_transitions.append(transition)
                log_info(
                    "IBKR bridge close-position transition",
                    component="ibkr_bridge",
                    operation="close_position",
                    symbol=normalized_symbol,
                    **transition,
                )
            final_snapshot = dict(current_snapshot)
            last_status = current_status

        final_status = str(final_snapshot.get("status", "")).strip()
        close_filled = final_status.lower() == "filled" or not broker_position_open
        close_failed = broker_position_open
        result_reason = ""
        if close_failed:
            result_reason = "broker_close_not_confirmed"

        return {
            "attempted": True,
            "placed": True,
            "symbol": normalized_symbol,
            "action": action.lower(),
            "qty": abs(qty),
            "order_id": normalized_trade.get("id", ""),
            "status": final_status or normalized_trade.get("status", ""),
            "filled_qty": final_snapshot.get("filled_qty", 0.0),
            "filled_avg_price": final_snapshot.get("avg_fill_price", 0.0),
            "filled_at": final_snapshot.get("filled_at", ""),
            "position_closed": not broker_position_open,
            "close_failed": close_failed,
            "reason": result_reason,
            "canceled_order_ids": canceled_order_ids,
            "cancel_settle_transitions": cancel_settle_transitions,
            "status_transitions": status_transitions,
        }


_client: IbkrGatewayClient | None = None


def get_ibkr_client() -> IbkrGatewayClient:
    global _client
    if _client is None:
        _client = IbkrGatewayClient()
    return _client
