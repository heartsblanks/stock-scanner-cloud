import {
  formatCurrency,
  formatNumber,
  formatPercent,
  formatTimestamp,
  formatValue,
  signedTone,
  sortRowsByLatest,
} from "./tableFormatters";

function statusBadge(status) {
  if (status === "CLOSED") {
    return "dashboard-badge dashboard-badge-neutral";
  }
  if (status === "OPEN") {
    return "dashboard-badge dashboard-badge-ok";
  }
  return "dashboard-badge dashboard-badge-warn";
}

function directionBadge(direction) {
  if (direction === "LONG") {
    return "dashboard-badge dashboard-badge-ok";
  }
  if (direction === "SHORT") {
    return "dashboard-badge dashboard-badge-info";
  }
  return "dashboard-badge dashboard-badge-neutral";
}

function brokerBadge(broker) {
  const normalized = String(broker || "").trim().toUpperCase();
  if (normalized === "IBKR") {
    return "dashboard-badge dashboard-badge-broker-ibkr";
  }
  return "dashboard-badge dashboard-badge-neutral";
}

export default function TradeLifecycleTable({ rows }) {
  if (!rows || rows.length === 0) {
    return <div className="dashboard-empty">No trade lifecycle data.</div>;
  }

  const sortedRows = sortRowsByLatest(rows, ["entry_time", "exit_time", "created_at"]);

  return (
    <div className="dashboard-table-wrap">
      <table className="dashboard-table">
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Broker</th>
            <th>Mode</th>
            <th>Status</th>
            <th>Direction</th>
            <th className="dashboard-col-number">Shares</th>
            <th className="dashboard-col-number">Position Cost</th>
            <th className="dashboard-col-number">Per Trade Notional</th>
            <th className="dashboard-col-number">Remaining Slots</th>
            <th className="dashboard-col-number">Entry Price</th>
            <th className="dashboard-col-number">Exit Price</th>
            <th className="dashboard-col-number">P&amp;L</th>
            <th className="dashboard-col-number">Take Profit</th>
            <th className="dashboard-col-number">P&amp;L %</th>
            <th>Exit Reason</th>
            <th className="dashboard-col-number">Duration</th>
            <th>Entry Time</th>
            <th>Exit Time</th>
          </tr>
        </thead>
        <tbody>
          {sortedRows.map((row, index) => {
            const pnlTone = signedTone(row.realized_pnl);
            const rowTone =
              pnlTone === "dashboard-cell-positive"
                ? "dashboard-table-row-positive"
                : pnlTone === "dashboard-cell-negative"
                  ? "dashboard-table-row-negative"
                  : "";

            return (
              <tr key={`${row.trade_key || row.symbol || "lifecycle"}-${index}`} className={rowTone}>
                <td data-label="Symbol" className="dashboard-cell-strong">{formatValue(row.symbol)}</td>
                <td data-label="Broker">
                  <span className={brokerBadge(row.broker)}>{formatValue(row.broker)}</span>
                </td>
                <td data-label="Mode">{formatValue(row.mode)}</td>
                <td data-label="Status">
                  <span className={statusBadge(row.status)}>{formatValue(row.status)}</span>
                </td>
                <td data-label="Direction">
                  <span className={directionBadge(row.direction)}>{formatValue(row.direction)}</span>
                </td>
                <td data-label="Shares" className="dashboard-cell-number">{formatNumber(row.shares, 0)}</td>
                <td data-label="Position Cost" className="dashboard-cell-number">{formatCurrency(row.position_cost)}</td>
                <td data-label="Per Trade Notional" className="dashboard-cell-number">{formatCurrency(row.per_trade_notional)}</td>
                <td data-label="Remaining Slots" className="dashboard-cell-number">{formatNumber(row.remaining_slots, 0)}</td>
                <td data-label="Entry Price" className="dashboard-cell-number">{formatCurrency(row.entry_price)}</td>
                <td data-label="Exit Price" className="dashboard-cell-number">{formatCurrency(row.exit_price)}</td>
                <td data-label="P&L" className={`dashboard-cell-number ${pnlTone}`}>{formatCurrency(row.realized_pnl)}</td>
                <td data-label="Take Profit" className="dashboard-cell-number">{formatCurrency(row.take_profit_dollars)}</td>
                <td data-label="P&L %" className={`dashboard-cell-number ${signedTone(row.realized_pnl_percent)}`}>{formatPercent(row.realized_pnl_percent)}</td>
                <td data-label="Exit Reason">{formatValue(row.exit_reason)}</td>
                <td data-label="Duration" className="dashboard-cell-number">{formatNumber(row.duration_minutes, 1)} min</td>
                <td data-label="Entry Time" className="dashboard-cell-muted">{formatTimestamp(row.entry_time)}</td>
                <td data-label="Exit Time" className="dashboard-cell-muted">{formatTimestamp(row.exit_time)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
