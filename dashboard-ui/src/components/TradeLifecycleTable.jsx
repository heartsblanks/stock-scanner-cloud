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
            <th>Mode</th>
            <th>Status</th>
            <th>Direction</th>
            <th>Shares</th>
            <th>Position Cost</th>
            <th>Per Trade Notional</th>
            <th>Remaining Slots</th>
            <th>Entry Price</th>
            <th>Exit Price</th>
            <th>P&amp;L</th>
            <th>Take Profit</th>
            <th>P&amp;L %</th>
            <th>Exit Reason</th>
            <th>Duration</th>
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
                <td data-label="Mode">{formatValue(row.mode)}</td>
                <td data-label="Status">
                  <span className={statusBadge(row.status)}>{formatValue(row.status)}</span>
                </td>
                <td data-label="Direction">
                  <span className={directionBadge(row.direction)}>{formatValue(row.direction)}</span>
                </td>
                <td data-label="Shares">{formatNumber(row.shares, 0)}</td>
                <td data-label="Position Cost">{formatCurrency(row.position_cost)}</td>
                <td data-label="Per Trade Notional">{formatCurrency(row.per_trade_notional)}</td>
                <td data-label="Remaining Slots">{formatNumber(row.remaining_slots, 0)}</td>
                <td data-label="Entry Price">{formatCurrency(row.entry_price)}</td>
                <td data-label="Exit Price">{formatCurrency(row.exit_price)}</td>
                <td data-label="P&L" className={pnlTone}>{formatCurrency(row.realized_pnl)}</td>
                <td data-label="Take Profit">{formatCurrency(row.take_profit_dollars)}</td>
                <td data-label="P&L %" className={signedTone(row.realized_pnl_percent)}>{formatPercent(row.realized_pnl_percent)}</td>
                <td data-label="Exit Reason">{formatValue(row.exit_reason)}</td>
                <td data-label="Duration">{formatNumber(row.duration_minutes, 1)} min</td>
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
