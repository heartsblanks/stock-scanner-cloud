import { formatCurrency, formatNumber, formatTimestamp, formatValue } from "./tableFormatters";

function getStatusClass(status) {
  const normalized = String(status || "").trim().toLowerCase();

  if (normalized === "matched") {
    return "dashboard-badge dashboard-badge-ok";
  }

  if (
    normalized === "missing_in_alpaca" ||
    normalized === "missing_in_db" ||
    normalized === "exit_not_resolved"
  ) {
    return "dashboard-badge dashboard-badge-danger";
  }

  return "dashboard-badge dashboard-badge-warn";
}

function toneForMismatch(row) {
  const status = String(row?.match_status || "").trim().toLowerCase();
  if (status === "matched") {
    return "";
  }
  if (status === "missing_in_alpaca" || status === "missing_in_db" || status === "exit_not_resolved") {
    return "dashboard-table-row-negative";
  }
  return "";
}

export default function ReconciliationDetailsTable({ rows = [] }) {
  return (
    <div className="dashboard-table-wrap">
      {rows.length === 0 ? (
        <div className="dashboard-empty">No reconciliation detail rows available</div>
      ) : (
        <table className="dashboard-table" style={{ minWidth: 1200 }}>
          <thead>
            <tr>
              <th>Status</th>
              <th>Symbol</th>
              <th>Mode</th>
              <th>Parent Order ID</th>
              <th>Client Order ID</th>
              <th>Local Entry Time</th>
              <th>Local Exit Time</th>
              <th>Local Entry Price</th>
              <th>Alpaca Entry Price</th>
              <th>Entry Price Diff</th>
              <th>Local Exit Price</th>
              <th>Alpaca Exit Price</th>
              <th>Exit Price Diff</th>
              <th>Local Shares</th>
              <th>Alpaca Entry Qty</th>
              <th>Alpaca Exit Qty</th>
              <th>Local Exit Reason</th>
              <th>Alpaca Exit Reason</th>
              <th>Alpaca Exit Order ID</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, index) => (
              <tr key={`${row.broker_parent_order_id || "row"}-${index}`} className={toneForMismatch(row)}>
                <td>
                  <span className={getStatusClass(row.match_status)}>{formatValue(row.match_status)}</span>
                </td>
                <td className="dashboard-cell-strong">{formatValue(row.symbol)}</td>
                <td>{formatValue(row.mode)}</td>
                <td className="dashboard-cell-muted">{formatValue(row.broker_parent_order_id)}</td>
                <td className="dashboard-cell-muted">{formatValue(row.client_order_id)}</td>
                <td className="dashboard-cell-muted">{formatTimestamp(row.local_entry_timestamp_utc)}</td>
                <td className="dashboard-cell-muted">{formatTimestamp(row.local_exit_timestamp_utc)}</td>
                <td>{formatCurrency(row.local_entry_price)}</td>
                <td>{formatCurrency(row.alpaca_entry_price)}</td>
                <td>{formatCurrency(row.entry_price_diff)}</td>
                <td>{formatCurrency(row.local_exit_price)}</td>
                <td>{formatCurrency(row.alpaca_exit_price)}</td>
                <td>{formatCurrency(row.exit_price_diff)}</td>
                <td>{formatNumber(row.local_shares, 0)}</td>
                <td>{formatNumber(row.alpaca_entry_qty, 0)}</td>
                <td>{formatNumber(row.alpaca_exit_qty, 0)}</td>
                <td>{formatValue(row.local_exit_reason)}</td>
                <td>{formatValue(row.alpaca_exit_reason)}</td>
                <td className="dashboard-cell-muted">{formatValue(row.alpaca_exit_order_id)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
