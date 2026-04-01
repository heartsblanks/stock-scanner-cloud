import { formatCurrency, formatNumber, formatTimestamp, formatValue, sortRowsByLatest } from "./tableFormatters";

function getStatusBadge(status) {
  if (status === "OPEN") {
    return "dashboard-badge dashboard-badge-ok";
  }
  if (status === "PENDING") {
    return "dashboard-badge dashboard-badge-warn";
  }
  return "dashboard-badge dashboard-badge-neutral";
}

export default function OpenTradesTable({ trades }) {
  if (!trades || trades.length === 0) {
    return <div className="dashboard-empty">No open trades.</div>;
  }

  const sortedTrades = sortRowsByLatest(trades, ["entry_time", "timestamp_utc", "created_at"]);

  return (
    <div className="dashboard-table-wrap">
      <table className="dashboard-table">
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Mode</th>
            <th>Status</th>
            <th>Shares</th>
            <th>Position Cost</th>
            <th>Per Trade Notional</th>
            <th>Entry Price</th>
            <th>Stop</th>
            <th>Target</th>
            <th>Take Profit</th>
            <th>Entry Time</th>
          </tr>
        </thead>
        <tbody>
          {sortedTrades.map((trade, index) => (
            <tr key={`${trade.trade_key || trade.symbol || "trade"}-${index}`}>
              <td data-label="Symbol" className="dashboard-cell-strong">{formatValue(trade.symbol)}</td>
              <td data-label="Mode">{formatValue(trade.mode)}</td>
              <td data-label="Status">
                <span className={getStatusBadge(trade.status)}>{formatValue(trade.status)}</span>
              </td>
              <td data-label="Shares">{formatNumber(trade.shares, 0)}</td>
              <td data-label="Position Cost">{formatCurrency(trade.position_cost)}</td>
              <td data-label="Per Trade Notional">{formatCurrency(trade.per_trade_notional)}</td>
              <td data-label="Entry Price">{formatCurrency(trade.entry_price)}</td>
              <td data-label="Stop">{formatCurrency(trade.stop_price)}</td>
              <td data-label="Target">{formatCurrency(trade.target_price)}</td>
              <td data-label="Take Profit">{formatCurrency(trade.take_profit_dollars)}</td>
              <td data-label="Entry Time" className="dashboard-cell-muted">{formatTimestamp(trade.entry_time)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
