import { formatCurrency, formatNumber, formatTimestamp, formatValue } from "./tableFormatters";

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
          {trades.map((trade, index) => (
            <tr key={`${trade.trade_key || trade.symbol || "trade"}-${index}`}>
              <td className="dashboard-cell-strong">{formatValue(trade.symbol)}</td>
              <td>{formatValue(trade.mode)}</td>
              <td>
                <span className={getStatusBadge(trade.status)}>{formatValue(trade.status)}</span>
              </td>
              <td>{formatNumber(trade.shares, 0)}</td>
              <td>{formatCurrency(trade.position_cost)}</td>
              <td>{formatCurrency(trade.per_trade_notional)}</td>
              <td>{formatCurrency(trade.entry_price)}</td>
              <td>{formatCurrency(trade.stop_price)}</td>
              <td>{formatCurrency(trade.target_price)}</td>
              <td>{formatCurrency(trade.take_profit_dollars)}</td>
              <td className="dashboard-cell-muted">{formatTimestamp(trade.entry_time)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
