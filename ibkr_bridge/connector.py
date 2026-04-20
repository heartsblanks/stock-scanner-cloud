from __future__ import annotations

import asyncio
import copy
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


def _to_float_or_none(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _truthy_env(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, str(default))).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _fractional_shares_enabled() -> bool:
    return _truthy_env("ENABLE_FRACTIONAL_SHARES", False)


DEFAULT_PAPER_MAX_NOTIONAL = 250.0


def _configured_hard_notional_cap() -> float:
    configured = _to_float(os.getenv("PAPER_MAX_NOTIONAL"), DEFAULT_PAPER_MAX_NOTIONAL)
    return configured if configured > 0 else DEFAULT_PAPER_MAX_NOTIONAL


def _configured_entry_order_type() -> str:
    raw_value = str(os.getenv("IBKR_ENTRY_ORDER_TYPE", "MARKET")).strip().upper()
    if raw_value in {"LMT", "LIMIT"}:
        return "LMT"
    return "MKT"


def _fractional_share_decimals() -> int:
    try:
        return max(0, min(6, int(os.getenv("FRACTIONAL_SHARE_DECIMALS", "4"))))
    except Exception:
        return 4


def _normalize_order_quantity(quantity: float, *, allow_fractional: bool | None = None) -> float:
    if quantity <= 0:
        return 0.0

    fractional_allowed = _fractional_shares_enabled() if allow_fractional is None else bool(allow_fractional)
    if fractional_allowed:
        factor = 10 ** _fractional_share_decimals()
        return math.floor(quantity * factor) / factor

    return float(int(quantity))


def _is_effectively_whole_quantity(quantity: float) -> bool:
    return abs(quantity - round(quantity)) < 1e-9


def _order_quantity_token(quantity: float) -> str:
    if _is_effectively_whole_quantity(quantity):
        return str(int(round(quantity)))
    return str(quantity).replace(".", "p")


def _fractional_rejection_hint(*error_sets: list[dict[str, Any]], detail: str | None = None) -> bool:
    search_space: list[str] = []
    if detail:
        search_space.append(str(detail))

    for error_set in error_sets:
        for entry in error_set or []:
            if not isinstance(entry, dict):
                continue
            search_space.append(str(entry.get("error", "")))
            search_space.append(str(entry.get("message", "")))
            search_space.append(str(entry.get("status", "")))

    combined = " ".join(search_space).lower()
    if not combined:
        return False

    return any(
        token in combined
        for token in (
            "fraction",
            "fractional",
            "minimum increment",
            "size increment",
            "invalid size",
            "invalid quantity",
            "quantity does not conform",
            "outside regular",
        )
    )


@dataclass(frozen=True)
class IbkrConnectionConfig:
    host: str
    port: int
    client_id: int
    inspection_client_id: int
    account_id: str
    readonly: bool
    timeout_seconds: int


def get_ibkr_connection_config() -> IbkrConnectionConfig:
    client_id = int(os.getenv("IBKR_CLIENT_ID", "101"))
    return IbkrConnectionConfig(
        host=str(os.getenv("IBKR_HOST", "127.0.0.1")).strip() or "127.0.0.1",
        port=int(os.getenv("IBKR_PORT", "4002")),
        client_id=client_id,
        inspection_client_id=int(os.getenv("IBKR_INSPECTION_CLIENT_ID", str(client_id + 1000))),
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
            from ib_insync import LimitOrder, MarketOrder, Order, StopOrder, Stock
        except ImportError as exc:
            raise RuntimeError(
                "ib_insync is not installed on the IBKR bridge host. "
                "Install requirements after adding the IBKR bridge dependency."
            ) from exc
        return LimitOrder, MarketOrder, StopOrder, Order, Stock

    def _connect(self):
        self._ensure_event_loop()
        cached_ib = getattr(self, "_ib", None)
        if cached_ib is not None and cached_ib.isConnected():
            return cached_ib

        return self._connect_with_client_id(self.config.client_id, use_cache=True)

    def _connect_with_client_id(self, client_id: int, *, use_cache: bool):
        ib_class = self._load_ib_class()
        ib = ib_class()
        request_timeout = float(max(1, int(self.config.timeout_seconds)))
        setattr(ib, "RequestTimeout", request_timeout)
        ib.connect(
            self.config.host,
            self.config.port,
            clientId=client_id,
            readonly=self.config.readonly,
            timeout=self.config.timeout_seconds,
        )
        if use_cache:
            self._ib = ib
        return ib

    def _connect_inspection(self):
        self._ensure_event_loop()
        inspection_client_id = int(getattr(self.config, "inspection_client_id", getattr(self.config, "client_id", 101) + 1000))
        return self._connect_with_client_id(inspection_client_id, use_cache=False)

    def _disconnect_ib(self, ib) -> None:
        if ib is None:
            return
        try:
            if ib.isConnected():
                ib.disconnect()
        except Exception:
            pass

    def _disconnect(self) -> None:
        ib = getattr(self, "_ib", None)
        self._ib = None
        self._disconnect_ib(ib)

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

    def _entry_poll_config(self) -> tuple[int, float]:
        try:
            attempts = max(1, int(os.getenv("IBKR_ENTRY_POLL_ATTEMPTS", "4")))
        except Exception:
            attempts = 4
        try:
            interval_seconds = max(0.1, float(os.getenv("IBKR_ENTRY_POLL_INTERVAL_SECONDS", "0.5")))
        except Exception:
            interval_seconds = 0.5
        return attempts, interval_seconds

    def _is_rejected_entry_status(self, status: str) -> bool:
        normalized = str(status or "").strip().lower()
        return normalized in {"cancelled", "apicancelled", "inactive", "rejected"}

    def _is_confirmed_entry_status(self, status: str) -> bool:
        normalized = str(status or "").strip().lower()
        return normalized in {"pendingsubmit", "presubmitted", "submitted", "filled", "pendingcancel"}

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
            "perm_id": _to_float(getattr(order_status, "permId", 0.0)),
            "filled_qty": _to_float(getattr(order_status, "filled", 0.0)),
            "remaining_qty": _to_float(getattr(order_status, "remaining", 0.0)),
            "avg_fill_price": _to_float(getattr(order_status, "avgFillPrice", 0.0)),
            "last_fill_price": _to_float(getattr(order_status, "lastFillPrice", 0.0)),
            "why_held": str(getattr(order_status, "whyHeld", "")).strip(),
            "filled_at": filled_at,
        }

    def _collect_trade_errors(self, trade: Any) -> list[dict[str, Any]]:
        logs = list(getattr(trade, "log", []) or [])
        errors: list[dict[str, Any]] = []
        for entry in logs:
            error_code = int(_to_float(getattr(entry, "errorCode", 0), 0))
            message = str(getattr(entry, "message", "")).strip()
            status = str(getattr(entry, "status", "")).strip()
            if error_code <= 0 and not message:
                continue
            event_time = getattr(entry, "time", None)
            if hasattr(event_time, "isoformat"):
                timestamp = event_time.isoformat()
            elif event_time is not None:
                timestamp = str(event_time)
            else:
                timestamp = ""
            errors.append(
                {
                    "timestamp": timestamp,
                    "error_code": error_code,
                    "message": message,
                    "status": status,
                }
            )
        return errors

    def _resolve_account_id(self, ib) -> str:
        configured_account_id = str(getattr(getattr(self, "config", None), "account_id", "")).strip()
        if configured_account_id:
            return configured_account_id

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
            "inspection_client_id": self.config.inspection_client_id,
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
        ib = self._connect_inspection()
        try:
            return self._get_positions_from_ib(ib)
        finally:
            self._disconnect_ib(ib)

    def _get_positions_from_ib(self, ib) -> list[dict[str, Any]]:
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

    def get_intraday_candles(
        self,
        symbol: str,
        interval: str = "1min",
        outputsize: int | None = None,
        *,
        exchange: str | None = None,
        primary_exchange: str | None = None,
        currency: str | None = None,
    ) -> list[dict[str, Any]]:
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
        _LimitOrder, _MarketOrder, _StopOrder, _Order, Stock = self._load_order_classes()
        route_exchange = str(exchange or "SMART").strip().upper() or "SMART"
        normalized_currency = str(currency or "USD").strip().upper() or "USD"
        normalized_primary_exchange = str(primary_exchange or "").strip().upper()
        try:
            contract = Stock(normalized_symbol, route_exchange, normalized_currency)
        except TypeError:
            # Test doubles may only accept `symbol`; set route fields afterward.
            contract = Stock(normalized_symbol)
            try:
                contract.exchange = route_exchange
            except Exception:
                pass
            try:
                contract.currency = normalized_currency
            except Exception:
                pass
        if normalized_primary_exchange and normalized_primary_exchange != route_exchange:
            try:
                contract.primaryExchange = normalized_primary_exchange
            except Exception:
                pass
        log_info(
            "IBKR bridge intraday request started",
            component="ibkr_bridge",
            operation="get_intraday_candles",
            symbol=normalized_symbol,
            interval=interval,
            outputsize=outputsize,
            exchange=route_exchange,
            primary_exchange=(normalized_primary_exchange or None),
            currency=normalized_currency,
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
            "trail_stop_price": _to_float(getattr(order, "trailStopPrice", 0.0)),
            "trailing_percent": _to_float(getattr(order, "trailingPercent", 0.0)),
            "limit_price": _to_float(getattr(order, "lmtPrice", 0.0)),
            "stop_price": _to_float(getattr(order, "trailStopPrice", getattr(order, "auxPrice", 0.0))),
            "trail_amount": _to_float(getattr(order, "auxPrice", 0.0)),
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

    def _unknown_sync_payload(self, order_id: str, message: str) -> dict[str, Any]:
        return {
            "id": str(order_id).strip(),
            "status": "unknown",
            "message": message,
        }

    def _fill_realized_pnl(self, fill: Any) -> float | None:
        commission_report = getattr(fill, "commissionReport", None)
        if commission_report is not None:
            value = _to_float_or_none(getattr(commission_report, "realizedPNL", None))
            if value is not None:
                return value
        execution = getattr(fill, "execution", None)
        return _to_float_or_none(getattr(execution, "realizedPNL", None))

    def _sync_order_from_fills(self, ib, order_id: str) -> dict[str, Any]:
        return self._sync_order_from_fills_snapshot(
            fills=self._fetch_recent_fills(ib),
            order_id=order_id,
        )

    def _sync_order_from_fills_snapshot(self, *, fills: list[Any], order_id: str) -> dict[str, Any]:
        if not fills:
            return self._unknown_sync_payload(order_id, "Order was not found in current IBKR fills.")

        order_id_text = str(order_id).strip()
        entry_fills = [
            fill
            for fill in fills
            if str(getattr(getattr(fill, "execution", None), "orderId", "")).strip() == order_id_text
        ]
        if not entry_fills:
            return self._unknown_sync_payload(order_id_text, "Order was not found in current IBKR fills.")

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
        latest_realized_pnl = self._fill_realized_pnl(latest_fill)

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
            if latest_realized_pnl is not None:
                result["exit_realized_pnl"] = round(latest_realized_pnl, 6)

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
            "realized_pnl": self._fill_realized_pnl(latest_fill) if latest_fill is not None else None,
        }

    def _sync_order_from_completed_trades(self, ib, order_id: str) -> dict[str, Any]:
        return self._sync_order_from_completed_trades_snapshot(
            trades=self._fetch_completed_trades(ib),
            order_id=order_id,
        )

    def _sync_order_from_completed_trades_snapshot(self, *, trades: list[Any], order_id: str) -> dict[str, Any]:
        if not trades:
            return self._unknown_sync_payload(order_id, "Order was not found in completed IBKR trades.")

        order_id_text = str(order_id).strip()
        entry_trades = [
            trade
            for trade in trades
            if str(getattr(getattr(trade, "order", None), "orderId", "")).strip() == order_id_text
        ]
        if not entry_trades:
            return self._unknown_sync_payload(order_id_text, "Order was not found in completed IBKR trades.")

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
            if latest_snapshot.get("realized_pnl") is not None:
                result["exit_realized_pnl"] = round(_to_float(latest_snapshot["realized_pnl"]), 6)

        return result

    def _sync_order_from_open_trades_snapshot(self, *, open_trades: list[Any], order_id: str) -> dict[str, Any]:
        normalized_order_id = str(order_id).strip()
        for trade in open_trades:
            trade_order_id = str(getattr(getattr(trade, "order", None), "orderId", "")).strip()
            if trade_order_id != normalized_order_id:
                continue

            normalized_trade = self._normalize_trade(trade)
            return {
                "id": normalized_order_id,
                "status": str(normalized_trade.get("status", "") or "open"),
                "parent_order_id": normalized_order_id,
                "parent_status": str(normalized_trade.get("status", "") or ""),
                "symbol": normalized_trade.get("symbol", ""),
                "client_order_id": normalized_trade.get("client_order_id", ""),
            }

        return self._unknown_sync_payload(normalized_order_id, "Order was not found in current open IBKR trades.")

    def _log_sync_batch_stage(
        self,
        *,
        stage: str,
        duration_ms: int,
        order_count: int,
        fills_count: int = 0,
        completed_count: int = 0,
        open_trade_count: int = 0,
    ) -> None:
        log_info(
            "IBKR bridge sync batch stage completed",
            component="ibkr_bridge",
            operation="sync_orders_batch",
            stage=stage,
            duration_ms=duration_ms,
            order_count=order_count,
            fills_count=fills_count,
            completed_count=completed_count,
            open_trade_count=open_trade_count,
        )

    def sync_orders(self, order_ids: list[str]) -> dict[str, Any]:
        normalized_order_ids: list[str] = []
        seen: set[str] = set()
        for raw_order_id in order_ids or []:
            order_id = str(raw_order_id).strip()
            if not order_id or order_id in seen:
                continue
            seen.add(order_id)
            normalized_order_ids.append(order_id)

        if not normalized_order_ids:
            return {
                "requested_count": 0,
                "synced_count": 0,
                "unknown_count": 0,
                "results": [],
                "durations_ms": {
                    "fills": 0,
                    "completed": 0,
                    "open_trades": 0,
                    "total": 0,
                },
            }

        # Use a fresh broker session for each sync batch so stale sockets
        # cannot block the request pipeline.
        ib = self._reset_connection()
        total_started_at = time.monotonic()

        resolved_by_order_id: dict[str, dict[str, Any]] = {}
        unknown_fallback_by_order_id: dict[str, dict[str, Any]] = {}

        fills_started_at = time.monotonic()
        fills = self._fetch_recent_fills(ib)
        fills_duration_ms = int((time.monotonic() - fills_started_at) * 1000)
        self._log_sync_batch_stage(
            stage="fills",
            duration_ms=fills_duration_ms,
            order_count=len(normalized_order_ids),
            fills_count=len(fills),
        )

        unresolved_after_fills: list[str] = []
        for order_id in normalized_order_ids:
            fills_result = self._sync_order_from_fills_snapshot(
                fills=fills,
                order_id=order_id,
            )
            if str(fills_result.get("status", "")).strip().lower() != "unknown":
                resolved_by_order_id[order_id] = fills_result
            else:
                unknown_fallback_by_order_id[order_id] = fills_result
                unresolved_after_fills.append(order_id)

        open_duration_ms = 0
        unresolved_after_open = list(unresolved_after_fills)
        if unresolved_after_fills:
            open_started_at = time.monotonic()
            open_trades = self._fetch_open_trades(ib)
            open_duration_ms = int((time.monotonic() - open_started_at) * 1000)
            self._log_sync_batch_stage(
                stage="open_trades",
                duration_ms=open_duration_ms,
                order_count=len(unresolved_after_fills),
                open_trade_count=len(open_trades),
            )

            unresolved_after_open = []
            for order_id in unresolved_after_fills:
                open_result = self._sync_order_from_open_trades_snapshot(
                    open_trades=open_trades,
                    order_id=order_id,
                )
                if str(open_result.get("status", "")).strip().lower() != "unknown":
                    resolved_by_order_id[order_id] = open_result
                else:
                    unknown_fallback_by_order_id[order_id] = open_result
                    unresolved_after_open.append(order_id)
        else:
            self._log_sync_batch_stage(
                stage="open_trades",
                duration_ms=0,
                order_count=0,
                open_trade_count=0,
            )

        completed_duration_ms = 0
        if unresolved_after_open:
            completed_started_at = time.monotonic()
            completed_trades = self._fetch_completed_trades(ib)
            completed_duration_ms = int((time.monotonic() - completed_started_at) * 1000)
            self._log_sync_batch_stage(
                stage="completed",
                duration_ms=completed_duration_ms,
                order_count=len(unresolved_after_open),
                completed_count=len(completed_trades),
            )

            for order_id in unresolved_after_open:
                completed_result = self._sync_order_from_completed_trades_snapshot(
                    trades=completed_trades,
                    order_id=order_id,
                )
                if str(completed_result.get("status", "")).strip().lower() != "unknown":
                    resolved_by_order_id[order_id] = completed_result
                else:
                    unknown_fallback_by_order_id[order_id] = completed_result
        else:
            self._log_sync_batch_stage(
                stage="completed",
                duration_ms=0,
                order_count=0,
                completed_count=0,
            )

        results: list[dict[str, Any]] = []
        synced_count = 0
        for order_id in normalized_order_ids:
            resolved = resolved_by_order_id.get(order_id)
            if isinstance(resolved, dict):
                synced_count += 1
                results.append(resolved)
                continue

            fallback_unknown = unknown_fallback_by_order_id.get(order_id)
            if isinstance(fallback_unknown, dict):
                results.append(fallback_unknown)
                continue
            results.append(self._unknown_sync_payload(order_id, "Order was not found in IBKR batch sync stages."))

        total_duration_ms = int((time.monotonic() - total_started_at) * 1000)
        unknown_count = max(len(normalized_order_ids) - synced_count, 0)
        log_info(
            "IBKR bridge sync batch completed",
            component="ibkr_bridge",
            operation="sync_orders_batch",
            order_count=len(normalized_order_ids),
            synced_count=synced_count,
            unknown_count=unknown_count,
            duration_ms=total_duration_ms,
            fills_duration_ms=fills_duration_ms,
            completed_duration_ms=completed_duration_ms,
            open_trades_duration_ms=open_duration_ms,
        )
        return {
            "requested_count": len(normalized_order_ids),
            "synced_count": synced_count,
            "unknown_count": unknown_count,
            "results": results,
            "durations_ms": {
                "fills": fills_duration_ms,
                "completed": completed_duration_ms,
                "open_trades": open_duration_ms,
                "total": total_duration_ms,
            },
        }

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
        ib = self._connect_inspection()
        try:
            return self._get_open_orders_from_ib(ib)
        finally:
            self._disconnect_ib(ib)

    def _get_open_orders_from_ib(self, ib) -> list[dict[str, Any]]:
        trades = self._fetch_open_trades(ib)
        normalized_trades = [self._normalize_trade(trade) for trade in trades]
        return [
            trade
            for trade in normalized_trades
            if self._is_open_order_status(trade.get("status", "")) and _to_float(trade.get("remaining_qty", 0.0)) > 0
        ]

    def get_open_state(self) -> dict[str, Any]:
        ib = self._connect_inspection()
        try:
            return {
                "positions": self._get_positions_from_ib(ib),
                "orders": self._get_open_orders_from_ib(ib),
            }
        finally:
            self._disconnect_ib(ib)

    def get_order(self, order_id: str) -> dict[str, Any] | None:
        normalized_order_id = str(order_id).strip()
        if not normalized_order_id:
            return None

        ib = self._connect_inspection()
        try:
            for trade in self._fetch_open_trades(ib):
                trade_order_id = str(getattr(getattr(trade, "order", None), "orderId", "")).strip()
                if trade_order_id == normalized_order_id:
                    return self._normalize_trade(trade)
            return None
        finally:
            self._disconnect_ib(ib)

    def sync_order(self, order_id: str) -> dict[str, Any]:
        normalized_order_id = str(order_id).strip()
        if not normalized_order_id:
            return {"id": "", "status": "unknown", "message": "order_id is required"}

        batch_result = self.sync_orders([normalized_order_id])
        rows = list(batch_result.get("results") or [])
        if rows:
            return rows[0]
        return self._unknown_sync_payload(normalized_order_id, "Order was not found in IBKR sync batch response.")

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

    def _is_scanner_close_order(self, order: Any, symbol: str) -> bool:
        normalized_symbol = str(symbol or "").strip().upper()
        if not normalized_symbol:
            return False
        order_ref = str(getattr(order, "orderRef", "")).strip().upper()
        if order_ref != f"SCANNER-CLOSE-{normalized_symbol}":
            return False
        order_type = str(getattr(order, "orderType", "")).strip().upper()
        return order_type in {"", "MKT"}

    def _find_existing_scanner_close_trade(
        self,
        ib,
        symbol: str,
        action: str,
        *,
        open_trades: list[Any] | None = None,
    ) -> Any | None:
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_action = str(action or "").strip().upper()
        trades = list(open_trades) if open_trades is not None else self._open_orders_for_symbol(ib, normalized_symbol)
        for trade in trades:
            order = getattr(trade, "order", None)
            if not self._is_scanner_close_order(order, normalized_symbol):
                continue
            trade_action = str(getattr(order, "action", "")).strip().upper()
            if normalized_action and trade_action and trade_action != normalized_action:
                continue
            return trade
        return None

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

    def _global_cancel_on_unresolved_order_id_enabled(self) -> bool:
        return _truthy_env("IBKR_GLOBAL_CANCEL_ON_UNRESOLVED_ORDER_ID", True)

    def _attempt_global_cancel(self, ib, *, symbol: str, operation: str) -> bool:
        if not self._global_cancel_on_unresolved_order_id_enabled():
            return False
        try:
            ib.reqGlobalCancel()
            log_warning(
                "IBKR bridge requested global cancel after unresolved open order ids",
                component="ibkr_bridge",
                operation=operation,
                symbol=symbol,
            )
            return True
        except Exception as exc:
            log_exception(
                "IBKR bridge global cancel fallback failed",
                exc,
                component="ibkr_bridge",
                operation=operation,
                symbol=symbol,
            )
            return False

    def place_paper_bracket_order(
        self,
        trade: dict[str, Any],
        max_notional: float | None = None,
        *,
        _fractional_retry_attempted: bool = False,
    ) -> dict[str, Any]:
        metrics = trade.get("metrics", {}) if isinstance(trade, dict) else {}
        symbol = str(metrics.get("symbol", "")).strip().upper()
        requested_market = str(metrics.get("market", "") or "").strip().upper()
        requested_exchange = str(metrics.get("exchange", "SMART") or "SMART").strip().upper() or "SMART"
        requested_primary_exchange = str(metrics.get("primary_exchange", "") or "").strip().upper()
        requested_currency = str(metrics.get("currency", "USD") or "USD").strip().upper() or "USD"
        direction = str(metrics.get("direction", "BUY")).strip().upper() or "BUY"
        entry = _to_float(metrics.get("entry"))
        stop = _to_float(metrics.get("stop"))
        target = _to_float(metrics.get("target"))
        allow_fractional = _fractional_shares_enabled() and not _fractional_retry_attempted
        scanner_shares = _normalize_order_quantity(_to_float(metrics.get("shares"), 0), allow_fractional=allow_fractional)
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

        notional_cap_candidates = [
            value
            for value in (max_notional, per_trade_notional, remaining_allocatable_capital)
            if _to_float(value) > 0
        ]
        # Defense-in-depth: when scanner sizing fields are present, enforce the
        # configured hard cap at the broker layer too.
        if notional_cap_candidates:
            configured_hard_cap = _configured_hard_notional_cap()
            if configured_hard_cap > 0:
                notional_cap_candidates.append(configured_hard_cap)
        notional_cap = min(notional_cap_candidates) if notional_cap_candidates else 0.0
        capped_shares = _normalize_order_quantity((notional_cap / entry), allow_fractional=allow_fractional) if notional_cap > 0 else 0.0
        if notional_cap > 0 and capped_shares <= 0:
            # Hard cap says this symbol cannot afford even one share.
            final_shares = 0.0
        elif scanner_shares > 0 and capped_shares > 0:
            final_shares = min(scanner_shares, capped_shares)
        else:
            final_shares = max(scanner_shares, capped_shares)
        if final_shares <= 0:
            return {"attempted": False, "placed": False, "broker": "IBKR", "symbol": symbol, "reason": "position_size_too_small"}

        has_fractional_qty = allow_fractional and not _is_effectively_whole_quantity(final_shares)
        whole_share_fallback_qty = _normalize_order_quantity(final_shares, allow_fractional=False)

        def _retry_with_whole_shares_if_supported(
            *,
            trigger_reason: str,
            details: str = "",
            ib_errors: list[dict[str, Any]] | None = None,
            trade_errs: list[dict[str, Any]] | None = None,
        ) -> dict[str, Any] | None:
            if _fractional_retry_attempted:
                return None
            if not has_fractional_qty:
                return None
            if whole_share_fallback_qty <= 0:
                return None
            if not _fractional_rejection_hint(ib_errors or [], trade_errs or [], detail=details):
                return None

            fallback_trade = copy.deepcopy(trade) if isinstance(trade, dict) else {}
            fallback_metrics = fallback_trade.setdefault("metrics", {})
            if isinstance(fallback_metrics, dict):
                fallback_metrics["shares"] = whole_share_fallback_qty

            log_warning(
                "IBKR bridge retrying bracket with whole-share fallback after fractional rejection",
                component="ibkr_bridge",
                operation="place_paper_bracket_order",
                symbol=symbol,
                trigger_reason=trigger_reason,
                original_shares=final_shares,
                fallback_shares=whole_share_fallback_qty,
            )

            fallback_result = self.place_paper_bracket_order(
                fallback_trade,
                max_notional=max_notional,
                _fractional_retry_attempted=True,
            )
            if isinstance(fallback_result, dict):
                fallback_result["fractional_fallback_used"] = True
                fallback_result["fractional_original_qty"] = final_shares
                fallback_result["fractional_fallback_qty"] = whole_share_fallback_qty
                fallback_result["fractional_fallback_trigger"] = trigger_reason
            return fallback_result

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
            allow_fractional=allow_fractional,
            fractional_retry_attempted=_fractional_retry_attempted,
            exchange=requested_exchange,
            primary_exchange=(requested_primary_exchange or None),
            currency=requested_currency,
        )

        # Use a fresh broker session for placement flow so stale sockets
        # cannot block qualification/placeOrder indefinitely.
        ib = self._reset_connection()
        LimitOrder, MarketOrder, StopOrder, Order, Stock = self._load_order_classes()
        try:
            contract = Stock(symbol, requested_exchange, requested_currency)
        except TypeError:
            # Test doubles may only accept `symbol`; set route fields afterward.
            contract = Stock(symbol)
            try:
                contract.exchange = requested_exchange
            except Exception:
                pass
            try:
                contract.currency = requested_currency
            except Exception:
                pass
        if requested_primary_exchange and requested_primary_exchange != requested_exchange:
            try:
                contract.primaryExchange = requested_primary_exchange
            except Exception:
                pass
        existing_position_row = self._find_position_row(ib, symbol)
        existing_position_qty = _to_float(getattr(existing_position_row, "position", 0.0)) if existing_position_row is not None else 0.0
        existing_open_trades = self._open_orders_for_symbol(ib, symbol)
        if existing_position_qty != 0.0 or existing_open_trades:
            open_order_ids = [
                str(getattr(getattr(open_trade, "order", None), "orderId", "")).strip()
                for open_trade in existing_open_trades
            ]
            return {
                "attempted": True,
                "placed": False,
                "broker": "IBKR",
                "symbol": symbol,
                "reason": "ibkr_symbol_already_open",
                "details": "Symbol already has open broker exposure (position and/or open orders).",
                "existing_position_qty": existing_position_qty,
                "existing_open_order_count": len(existing_open_trades),
                "existing_open_order_ids": open_order_ids,
            }
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

        qualified_exchange = str(getattr(contract, "exchange", "") or "").strip().upper()
        qualified_primary_exchange = str(getattr(contract, "primaryExchange", "") or "").strip().upper()
        qualified_currency = str(getattr(contract, "currency", "") or "").strip().upper()

        if requested_currency and qualified_currency and qualified_currency != requested_currency:
            return {
                "attempted": True,
                "placed": False,
                "broker": "IBKR",
                "symbol": symbol,
                "reason": "qualified_currency_mismatch",
                "details": (
                    f"Requested currency {requested_currency}, but qualified contract currency is {qualified_currency}."
                ),
                "requested_exchange": requested_exchange,
                "requested_primary_exchange": requested_primary_exchange,
                "qualified_exchange": qualified_exchange,
                "qualified_primary_exchange": qualified_primary_exchange,
                "requested_currency": requested_currency,
                "qualified_currency": qualified_currency,
            }

        if requested_primary_exchange and qualified_primary_exchange and qualified_primary_exchange != requested_primary_exchange:
            return {
                "attempted": True,
                "placed": False,
                "broker": "IBKR",
                "symbol": symbol,
                "reason": "qualified_primary_exchange_mismatch",
                "details": (
                    f"Requested primary exchange {requested_primary_exchange}, "
                    f"but qualified contract primary exchange is {qualified_primary_exchange}."
                ),
                "requested_exchange": requested_exchange,
                "requested_primary_exchange": requested_primary_exchange,
                "qualified_exchange": qualified_exchange,
                "qualified_primary_exchange": qualified_primary_exchange,
                "requested_currency": requested_currency,
                "qualified_currency": qualified_currency,
            }

        if requested_market == "EUROPE":
            us_venues = {"NYSE", "NASDAQ", "ARCA", "AMEX", "BATS", "IEX", "NMS"}
            if qualified_exchange in us_venues or qualified_primary_exchange in us_venues:
                return {
                    "attempted": True,
                    "placed": False,
                    "broker": "IBKR",
                    "symbol": symbol,
                    "reason": "qualified_us_venue_for_europe_symbol",
                    "details": (
                        "Qualified contract resolved to a US venue for a EUROPE mode symbol. "
                        "Order was blocked."
                    ),
                    "requested_exchange": requested_exchange,
                    "requested_primary_exchange": requested_primary_exchange,
                    "qualified_exchange": qualified_exchange,
                    "qualified_primary_exchange": qualified_primary_exchange,
                    "requested_currency": requested_currency,
                    "qualified_currency": qualified_currency,
                }
            if requested_currency == "EUR" and qualified_currency and qualified_currency != "EUR":
                return {
                    "attempted": True,
                    "placed": False,
                    "broker": "IBKR",
                    "symbol": symbol,
                    "reason": "qualified_non_eur_for_europe_symbol",
                    "details": (
                        f"Qualified contract currency {qualified_currency} is invalid for EUROPE mode."
                    ),
                    "requested_exchange": requested_exchange,
                    "requested_primary_exchange": requested_primary_exchange,
                    "qualified_exchange": qualified_exchange,
                    "qualified_primary_exchange": qualified_primary_exchange,
                    "requested_currency": requested_currency,
                    "qualified_currency": qualified_currency,
                }

        action = "BUY" if direction == "BUY" else "SELL"
        exit_action = "SELL" if action == "BUY" else "BUY"
        client_order_id = f"scanner-{symbol}-{direction}-{int(round(entry * 10000))}-{_order_quantity_token(final_shares)}"
        entry_order_type = _configured_entry_order_type()

        base_order_id = ib.client.getReqId()
        if entry_order_type == "MKT":
            parent = MarketOrder(action, final_shares, transmit=False)
        else:
            parent = LimitOrder(action, final_shares, round(entry, 2), transmit=False)
        parent.orderId = base_order_id
        parent.orderRef = client_order_id
        parent.tif = "DAY"

        take_profit = LimitOrder(exit_action, final_shares, round(target, 2), transmit=False)
        take_profit.orderId = base_order_id + 1
        take_profit.parentId = base_order_id
        take_profit.orderRef = client_order_id
        take_profit.tif = "GTC"

        trail_amount = round(abs(entry - stop), 2)
        trail_percent = None
        if trail_amount > 0 and entry > 0:
            trail_percent = round((trail_amount / entry) * 100.0, 4)

        trailing_stop = Order()
        trailing_stop.action = exit_action
        trailing_stop.totalQuantity = final_shares
        trailing_stop.orderType = "TRAIL"
        trailing_stop.transmit = True
        trailing_stop.orderId = base_order_id + 2
        trailing_stop.parentId = base_order_id
        trailing_stop.orderRef = client_order_id
        trailing_stop.tif = "GTC"
        trailing_stop.auxPrice = trail_amount
        trailing_stop.trailStopPrice = round(stop, 2)

        log_info(
            "IBKR bridge paper bracket orders prepared",
            component="ibkr_bridge",
            operation="place_paper_bracket_order",
            symbol=symbol,
            client_order_id=client_order_id,
            parent_order_id=base_order_id,
            take_profit_order_id=base_order_id + 1,
            trailing_stop_order_id=base_order_id + 2,
            trail_amount=trail_amount,
            trail_percent=trail_percent,
            entry_order_type=entry_order_type,
        )

        ib_api_errors: list[dict[str, Any]] = []
        parent_trade = None
        take_profit_trade = None
        trailing_stop_trade = None

        def _ib_error_callback(req_id, error_code, error_string, contract_obj):
            error_text = str(error_string or "").strip()
            if not error_text and int(_to_float(error_code, 0)) <= 0:
                return
            ib_api_errors.append(
                {
                    "req_id": int(_to_float(req_id, 0)),
                    "error_code": int(_to_float(error_code, 0)),
                    "error": error_text,
                    "symbol": str(getattr(contract_obj, "symbol", "")).strip(),
                }
            )

        error_event = getattr(ib, "errorEvent", None)
        callback_registered = False
        if error_event is not None:
            try:
                error_event += _ib_error_callback
                callback_registered = True
            except Exception:
                callback_registered = False

        try:
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
                trailing_stop_trade = ib.placeOrder(contract, trailing_stop)
                log_info(
                    "IBKR bridge trailing stop order submitted",
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
                fallback_result = _retry_with_whole_shares_if_supported(
                    trigger_reason="ibkr_order_rejected_exception",
                    details=str(exc),
                    ib_errors=ib_api_errors,
                    trade_errs=[],
                )
                if fallback_result is not None:
                    return fallback_result
                return {
                    "attempted": True,
                    "placed": False,
                    "broker": "IBKR",
                    "symbol": symbol,
                    "reason": "ibkr_order_rejected",
                    "details": str(exc),
                    "ib_api_errors": ib_api_errors,
                }

            estimated_notional = round(final_shares * entry, 2)
            parent_snapshots: list[dict[str, Any]] = []
            poll_attempts, poll_interval_seconds = self._entry_poll_config()
            parent_snapshot = self._order_status_snapshot(parent_trade)
            parent_snapshots.append(
                {
                    "attempt": 0,
                    **parent_snapshot,
                }
            )
            for attempt in range(1, poll_attempts + 1):
                status_now = str(parent_snapshot.get("status", "")).strip()
                perm_id_now = int(_to_float(parent_snapshot.get("perm_id"), 0))
                if self._is_rejected_entry_status(status_now):
                    break
                if self._is_confirmed_entry_status(status_now) and perm_id_now > 0:
                    break
                try:
                    ib.sleep(poll_interval_seconds)
                except Exception:
                    time.sleep(poll_interval_seconds)
                parent_snapshot = self._order_status_snapshot(parent_trade)
                parent_snapshots.append(
                    {
                        "attempt": attempt,
                        **parent_snapshot,
                    }
                )

            parent_status = str(parent_snapshot.get("status", "")).strip()
            parent_perm_id = int(_to_float(parent_snapshot.get("perm_id"), 0))
            take_profit_status = str(getattr(getattr(take_profit_trade, "orderStatus", None), "status", "")).strip()
            trailing_stop_status = str(getattr(getattr(trailing_stop_trade, "orderStatus", None), "status", "")).strip()
            trade_errors = self._collect_trade_errors(parent_trade) + self._collect_trade_errors(take_profit_trade) + self._collect_trade_errors(trailing_stop_trade)

            if self._is_rejected_entry_status(parent_status):
                rejection_details = f"Parent order status is terminal reject state: {parent_status or 'UNKNOWN'}"
                fallback_result = _retry_with_whole_shares_if_supported(
                    trigger_reason="ibkr_order_rejected_status",
                    details=rejection_details,
                    ib_errors=ib_api_errors,
                    trade_errs=trade_errors,
                )
                if fallback_result is not None:
                    return fallback_result
                return {
                    "attempted": True,
                    "placed": False,
                    "broker": "IBKR",
                    "symbol": symbol,
                    "reason": "ibkr_order_rejected_status",
                    "details": rejection_details,
                    "broker_order_id": str(base_order_id),
                    "broker_parent_order_id": str(base_order_id),
                    "broker_order_status": parent_status,
                    "broker_perm_id": parent_perm_id,
                    "order_status_transitions": parent_snapshots,
                    "ib_api_errors": ib_api_errors,
                    "trade_errors": trade_errors,
                }

            if not self._is_confirmed_entry_status(parent_status):
                unconfirmed_details = f"Parent order status was not confirmed after placement polling: {parent_status or 'UNKNOWN'}"
                fallback_result = _retry_with_whole_shares_if_supported(
                    trigger_reason="ibkr_order_unconfirmed_status",
                    details=unconfirmed_details,
                    ib_errors=ib_api_errors,
                    trade_errs=trade_errors,
                )
                if fallback_result is not None:
                    return fallback_result
                return {
                    "attempted": True,
                    "placed": False,
                    "broker": "IBKR",
                    "symbol": symbol,
                    "reason": "ibkr_order_unconfirmed_status",
                    "details": unconfirmed_details,
                    "broker_order_id": str(base_order_id),
                    "broker_parent_order_id": str(base_order_id),
                    "broker_order_status": parent_status,
                    "broker_perm_id": parent_perm_id,
                    "order_status_transitions": parent_snapshots,
                    "ib_api_errors": ib_api_errors,
                    "trade_errors": trade_errors,
                }

            if parent_perm_id <= 0:
                missing_perm_details = "Parent order did not receive broker acknowledgment (permId is missing)."
                fallback_result = _retry_with_whole_shares_if_supported(
                    trigger_reason="ibkr_order_not_acknowledged",
                    details=missing_perm_details,
                    ib_errors=ib_api_errors,
                    trade_errs=trade_errors,
                )
                if fallback_result is not None:
                    return fallback_result
                return {
                    "attempted": True,
                    "placed": False,
                    "broker": "IBKR",
                    "symbol": symbol,
                    "reason": "ibkr_order_not_acknowledged",
                    "details": missing_perm_details,
                    "broker_order_id": str(base_order_id),
                    "broker_parent_order_id": str(base_order_id),
                    "broker_order_status": parent_status,
                    "broker_perm_id": parent_perm_id,
                    "order_status_transitions": parent_snapshots,
                    "ib_api_errors": ib_api_errors,
                    "trade_errors": trade_errors,
                }

            log_info(
                "IBKR bridge paper bracket placement completed",
                component="ibkr_bridge",
                operation="place_paper_bracket_order",
                symbol=symbol,
                client_order_id=client_order_id,
                parent_order_id=base_order_id,
                parent_status=parent_status,
                parent_perm_id=parent_perm_id,
                take_profit_status=take_profit_status,
                trailing_stop_status=trailing_stop_status,
                estimated_notional=estimated_notional,
                parent_status_transitions=parent_snapshots,
                ib_api_errors=ib_api_errors,
                trade_errors=trade_errors,
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
                "broker_perm_id": parent_perm_id,
                "take_profit_order_id": str(base_order_id + 1),
                "trailing_stop_order_id": str(base_order_id + 2),
                "stop_loss_order_id": str(base_order_id + 2),
                "trail_amount": trail_amount,
                "trail_percent": trail_percent,
                "order_id": str(base_order_id),
                "parent_order_id": str(base_order_id),
                "order_status": parent_status,
                "entry_order_type": "market" if entry_order_type == "MKT" else "limit",
                "order_status_transitions": parent_snapshots,
                "ib_api_errors": ib_api_errors,
                "trade_errors": trade_errors,
            }
        finally:
            if callback_registered and error_event is not None:
                try:
                    error_event -= _ib_error_callback
                except Exception:
                    pass

    def cancel_orders_by_symbol(self, symbol: str) -> list[str]:
        normalized_symbol = str(symbol).strip().upper()
        if not normalized_symbol:
            raise RuntimeError("symbol is required")

        ib = self._connect()
        canceled_order_ids: list[str] = []
        unresolved_order_id_detected = False
        for trade in self._open_orders_for_symbol(ib, normalized_symbol):
            order = getattr(trade, "order", None)
            if self._is_scanner_close_order(order, normalized_symbol):
                log_info(
                    "IBKR bridge leaving scanner close order in place during cancel-by-symbol",
                    component="ibkr_bridge",
                    operation="cancel_orders_by_symbol",
                    symbol=normalized_symbol,
                    order_id=int(_to_float(getattr(order, "orderId", 0.0), 0)),
                )
                continue
            order_id = int(_to_float(getattr(order, "orderId", 0.0), 0))
            if order_id <= 0:
                unresolved_order_id_detected = True
                log_warning(
                    "IBKR bridge skipping cancel for unresolved open order id",
                    component="ibkr_bridge",
                    operation="cancel_orders_by_symbol",
                    symbol=normalized_symbol,
                    order_id=order_id,
                )
                continue
            ib.cancelOrder(order)
            canceled_order_ids.append(str(order_id))

        if unresolved_order_id_detected:
            self._attempt_global_cancel(
                ib,
                symbol=normalized_symbol,
                operation="cancel_orders_by_symbol",
            )

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

        _LimitOrder, MarketOrder, _StopOrder, _Order, Stock = self._load_order_classes()
        source_contract = getattr(target_position, "contract", None)
        source_contract_con_id = int(_to_float(getattr(source_contract, "conId", 0.0), 0))
        source_primary_exchange = str(getattr(source_contract, "primaryExchange", "")).strip().upper()
        source_exchange = str(getattr(source_contract, "exchange", "")).strip().upper()
        should_qualify_contract = source_contract_con_id <= 0

        if source_contract is not None and source_contract_con_id > 0:
            # Use the position contract identity (conId) and force SMART route.
            # This avoids close-order qualification drifting back to listing exchange.
            try:
                contract = copy.copy(source_contract)
            except Exception:
                contract = source_contract
            try:
                contract.symbol = normalized_symbol
            except Exception:
                pass
            try:
                contract.currency = str(getattr(contract, "currency", "")).strip() or "USD"
            except Exception:
                pass
        else:
            try:
                contract = Stock(normalized_symbol, "SMART", "USD")
            except TypeError:
                # Test doubles may only accept `symbol`; set route fields afterward.
                contract = Stock(normalized_symbol)
                try:
                    contract.exchange = "SMART"
                except Exception:
                    pass
                try:
                    contract.currency = "USD"
                except Exception:
                    pass
            # Keep SMART routing explicit for close orders. Only copy a listing venue
            # into `primaryExchange` when IB already reports one (never from `exchange`).
            preferred_primary_exchange = source_primary_exchange if source_primary_exchange not in {"", "SMART"} else ""
            if preferred_primary_exchange:
                contract.primaryExchange = preferred_primary_exchange
        try:
            contract.exchange = "SMART"
        except Exception:
            pass
        if should_qualify_contract:
            try:
                ib.qualifyContracts(contract)
            except Exception as exc:
                # Do not fall back to source position contract (often direct-routed listing venue).
                # Keep SMART contract and continue; this preserves route intent.
                log_warning(
                    "IBKR bridge close-position contract qualification failed; continuing with SMART contract",
                    component="ibkr_bridge",
                    operation="close_position",
                    symbol=normalized_symbol,
                    error=str(exc),
                )
        try:
            contract.exchange = "SMART"
        except Exception:
            pass
        # Defensively clear `primaryExchange` when it equals the routing exchange.
        # Some IB payload paths treat that as a direct-route hint.
        try:
            current_primary_exchange = str(getattr(contract, "primaryExchange", "")).strip().upper()
            current_exchange = str(getattr(contract, "exchange", "")).strip().upper()
            if current_primary_exchange and current_primary_exchange == current_exchange:
                contract.primaryExchange = ""
        except Exception:
            pass
        log_info(
            "IBKR bridge close-position contract prepared",
            component="ibkr_bridge",
            operation="close_position",
            symbol=normalized_symbol,
            con_id=int(_to_float(getattr(contract, "conId", 0.0), 0)),
            exchange=str(getattr(contract, "exchange", "")).strip(),
            primary_exchange=str(getattr(contract, "primaryExchange", "")).strip(),
            local_symbol=str(getattr(contract, "localSymbol", "")).strip(),
            trading_class=str(getattr(contract, "tradingClass", "")).strip(),
            source_exchange=source_exchange,
            source_primary_exchange=source_primary_exchange,
            qualification_skipped=not should_qualify_contract,
        )

        action = "SELL" if qty > 0 else "BUY"
        current_open_trades = self._open_orders_for_symbol(ib, normalized_symbol)
        existing_close_trade = self._find_existing_scanner_close_trade(
            ib,
            normalized_symbol,
            action,
            open_trades=current_open_trades,
        )
        if existing_close_trade is not None:
            existing_snapshot = self._order_status_snapshot(existing_close_trade)
            broker_position_open = self._position_is_open(ib, normalized_symbol)
            existing_order = getattr(existing_close_trade, "order", None)
            existing_order_id = str(getattr(existing_order, "orderId", "")).strip()
            existing_status = str(existing_snapshot.get("status", "")).strip()
            log_info(
                "IBKR bridge reusing existing scanner close order",
                component="ibkr_bridge",
                operation="close_position",
                symbol=normalized_symbol,
                order_id=existing_order_id,
                status=existing_status,
                broker_position_open=broker_position_open,
            )
            return {
                "attempted": True,
                "placed": True,
                "symbol": normalized_symbol,
                "action": action.lower(),
                "qty": abs(qty),
                "order_id": existing_order_id,
                "status": existing_status,
                "filled_qty": existing_snapshot.get("filled_qty", 0.0),
                "filled_avg_price": existing_snapshot.get("avg_fill_price", 0.0),
                "filled_at": existing_snapshot.get("filled_at", ""),
                "position_closed": not broker_position_open,
                "close_failed": False,
                "reason": "existing_close_order_pending",
                "canceled_order_ids": [],
                "cancel_settle_transitions": [],
                "status_transitions": [{
                    "attempt": 0,
                    **existing_snapshot,
                    "broker_position_open": broker_position_open,
                }],
            }
        canceled_order_ids: list[str] = []
        unresolved_order_id_detected = False
        for trade in current_open_trades:
            order = getattr(trade, "order", None)
            if self._is_scanner_close_order(order, normalized_symbol):
                continue
            order_id = int(_to_float(getattr(order, "orderId", 0.0), 0))
            if order_id <= 0:
                unresolved_order_id_detected = True
                log_warning(
                    "IBKR bridge skipping cancel for unresolved open order id",
                    component="ibkr_bridge",
                    operation="close_position",
                    symbol=normalized_symbol,
                    order_id=order_id,
                )
                continue
            ib.cancelOrder(order)
            canceled_order_ids.append(str(order_id))
        if unresolved_order_id_detected:
            self._attempt_global_cancel(
                ib,
                symbol=normalized_symbol,
                operation="close_position",
            )
        cancel_settle_transitions = self._wait_for_symbol_open_orders_to_clear(ib, normalized_symbol)
        order = MarketOrder(action, abs(qty))
        order.orderRef = f"scanner-close-{normalized_symbol}"
        try:
            order.overridePercentageConstraints = True
        except Exception:
            pass
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
