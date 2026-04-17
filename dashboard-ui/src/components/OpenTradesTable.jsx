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

export default function OpenTradesTable({ trades }) {
  const sortedTrades = sortRowsByLatest(Array.isArray(trades) ? trades : [], ["entry_time", "timestamp_utc", "created_at"]);
  const columnCount = 12;
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
        </thead>
        <tbody>
          {topPadding > 0 ? (
            <tr className="dashboard-virtual-spacer" aria-hidden="true">
              <td colSpan={columnCount} style={{ height: `${topPadding}px` }} />
            </tr>
          ) : null}
          {visibleTrades.map((trade, index) => (
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
          ))}
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
