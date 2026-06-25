import { formatCurrency, formatNumber, formatTimestamp, formatValue, sortRowsByLatest } from "./tableFormatters";
import { useVirtualizedTableRows } from "./useVirtualizedTableRows";

function getStatusBadge(status) {
  if (status === "OPEN") {
    return "dashboard-badge dashboard-badge-ok";
  }
  if (status === "PENDING") {
    return "dashboard-badge dashboard-badge-warn";
  }
  return "dashboard-badge dashboard-badge-neutral";
}

function getBrokerBadge(broker) {
  const normalized = String(broker || "").trim().toUpperCase();
  if (normalized === "IBKR") {
    return "dashboard-badge dashboard-badge-broker-ibkr";
  }
  return "dashboard-badge dashboard-badge-neutral";
}

function getTradeAge(value) {
  if (!value) {
    return "-";
  }
  const timestamp = new Date(value).getTime();
  if (Number.isNaN(timestamp)) {
    return "-";
  }
  const minutes = Math.max(0, Math.round((Date.now() - timestamp) / 60000));
  if (minutes < 60) {
    return `${minutes}m`;
  }
  return `${Math.floor(minutes / 60)}h ${minutes % 60}m`;
}

function getCurrentPrice(trade) {
  return trade?.live_current_price ?? trade?.current_price ?? trade?.last_price ?? null;
}

function getOpenPnl(trade) {
  if (trade?.live_unrealized_pl !== null && trade?.live_unrealized_pl !== undefined) {
    return trade.live_unrealized_pl;
  }
  const currentPrice = Number(getCurrentPrice(trade));
  const entryPrice = Number(trade?.entry_price);
  const shares = Number(trade?.shares);
  if (!Number.isFinite(currentPrice) || !Number.isFinite(entryPrice) || !Number.isFinite(shares)) {
    return null;
  }
  const direction = String(trade?.direction || trade?.side || "").toUpperCase();
  const multiplier = direction === "SHORT" || direction === "SELL" ? -1 : 1;
  return (currentPrice - entryPrice) * shares * multiplier;
}

function getPnlClass(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric === 0) {
    return "";
  }
  return numeric > 0 ? "dashboard-cell-positive" : "dashboard-cell-negative";
}

export default function OpenTradesTable({ trades, compact = false }) {
  const sortedTrades = sortRowsByLatest(Array.isArray(trades) ? trades : [], ["entry_time", "timestamp_utc", "created_at"]);
  const columnCount = compact ? 9 : 12;
  const {
    containerRef,
    handleScroll,
    virtualizationEnabled,
    startIndex,
    endIndex,
    topPadding,
    bottomPadding,
  } = useVirtualizedTableRows({
    rowCount: sortedTrades.length,
    rowHeight: 76,
    overscan: 8,
    minRowsToVirtualize: 40,
  });
  const visibleTrades = sortedTrades.slice(startIndex, endIndex);

  if (!sortedTrades.length) {
    return <div className="dashboard-empty">No open trades.</div>;
  }

  return (
    <div
      ref={containerRef}
      onScroll={handleScroll}
      className={`dashboard-table-wrap ${virtualizationEnabled ? "dashboard-table-wrap-virtualized" : ""}`}
    >
      <table className="dashboard-table">
        <thead>
          {compact ? (
            <tr>
              <th>Symbol</th>
              <th>Side</th>
              <th className="dashboard-col-number">Shares</th>
              <th className="dashboard-col-number">Entry</th>
              <th className="dashboard-col-number">Current</th>
              <th className="dashboard-col-number">Stop</th>
              <th className="dashboard-col-number">Target</th>
              <th className="dashboard-col-number">Open P/L</th>
              <th>Age</th>
            </tr>
          ) : (
            <tr>
              <th>Symbol</th>
              <th>Broker</th>
              <th>Mode</th>
              <th>Status</th>
              <th className="dashboard-col-number">Shares</th>
              <th className="dashboard-col-number">Position Cost</th>
              <th className="dashboard-col-number">Per Trade Notional</th>
              <th className="dashboard-col-number">Entry Price</th>
              <th className="dashboard-col-number">Stop</th>
              <th className="dashboard-col-number">Target</th>
              <th className="dashboard-col-number">Take Profit</th>
              <th>Entry Time</th>
            </tr>
          )}
        </thead>
        <tbody>
          {topPadding > 0 ? (
            <tr className="dashboard-virtual-spacer" aria-hidden="true">
              <td colSpan={columnCount} style={{ height: `${topPadding}px` }} />
            </tr>
          ) : null}
          {visibleTrades.map((trade, index) => {
            const openPnl = getOpenPnl(trade);
            if (compact) {
              return (
                <tr key={`${trade.trade_key || trade.symbol || "trade"}-${startIndex + index}`}>
                  <td data-label="Symbol" className="dashboard-cell-strong">{formatValue(trade.symbol)}</td>
                  <td data-label="Side">{formatValue(trade.direction || trade.side)}</td>
                  <td data-label="Shares" className="dashboard-cell-number">{formatNumber(trade.shares, 2)}</td>
                  <td data-label="Entry" className="dashboard-cell-number">{formatCurrency(trade.entry_price)}</td>
                  <td data-label="Current" className="dashboard-cell-number">{formatCurrency(getCurrentPrice(trade))}</td>
                  <td data-label="Stop" className="dashboard-cell-number">{formatCurrency(trade.stop_price)}</td>
                  <td data-label="Target" className="dashboard-cell-number">{formatCurrency(trade.target_price)}</td>
                  <td data-label="Open P/L" className={`dashboard-cell-number ${getPnlClass(openPnl)}`}>{formatCurrency(openPnl)}</td>
                  <td data-label="Age" className="dashboard-cell-muted">{getTradeAge(trade.entry_time)}</td>
                </tr>
              );
            }
            return (
              <tr key={`${trade.trade_key || trade.symbol || "trade"}-${startIndex + index}`}>
                <td data-label="Symbol" className="dashboard-cell-strong">{formatValue(trade.symbol)}</td>
                <td data-label="Broker">
                  <span className={getBrokerBadge(trade.broker)}>{formatValue(trade.broker)}</span>
                </td>
                <td data-label="Mode">{formatValue(trade.mode)}</td>
                <td data-label="Status">
                  <span className={getStatusBadge(trade.status)}>{formatValue(trade.status)}</span>
                </td>
                <td data-label="Shares" className="dashboard-cell-number">{formatNumber(trade.shares, 0)}</td>
                <td data-label="Position Cost" className="dashboard-cell-number">{formatCurrency(trade.position_cost)}</td>
                <td data-label="Per Trade Notional" className="dashboard-cell-number">{formatCurrency(trade.per_trade_notional)}</td>
                <td data-label="Entry Price" className="dashboard-cell-number">{formatCurrency(trade.entry_price)}</td>
                <td data-label="Stop" className="dashboard-cell-number">{formatCurrency(trade.stop_price)}</td>
                <td data-label="Target" className="dashboard-cell-number">{formatCurrency(trade.target_price)}</td>
                <td data-label="Take Profit" className="dashboard-cell-number">{formatCurrency(trade.take_profit_dollars)}</td>
                <td data-label="Entry Time" className="dashboard-cell-muted">{formatTimestamp(trade.entry_time)}</td>
              </tr>
            );
          })}
          {bottomPadding > 0 ? (
            <tr className="dashboard-virtual-spacer" aria-hidden="true">
              <td colSpan={columnCount} style={{ height: `${bottomPadding}px` }} />
            </tr>
          ) : null}
        </tbody>
      </table>
    </div>
  );
}
