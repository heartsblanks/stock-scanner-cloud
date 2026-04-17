import { formatCurrency, formatNumber, formatTimestamp, formatValue, sortRowsByLatest } from "./tableFormatters";

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
  const sortedRows = sortRowsByLatest(rows, [
    "local_entry_timestamp_utc",
    "local_exit_timestamp_utc",
    "created_at",
  ]);

  return (
    <div className="dashboard-table-wrap">
      {sortedRows.length === 0 ? (
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
              <th>Live Entry Price</th>
              <th>Entry Price Diff</th>
              <th>Local Exit Price</th>
              <th>Live Exit Price</th>
              <th>Exit Price Diff</th>
              <th>Local Shares</th>
              <th>Live Entry Qty</th>
              <th>Live Exit Qty</th>
              <th>Local Exit Reason</th>
              <th>Live Exit Reason</th>
              <th>Live Exit Order ID</th>
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((row, index) => (
              <tr key={`${row.broker_parent_order_id || "row"}-${index}`} className={toneForMismatch(row)}>
                <td data-label="Status">
                  <span className={getStatusClass(row.match_status)}>{formatValue(row.match_status)}</span>
                </td>
                <td data-label="Symbol" className="dashboard-cell-strong">{formatValue(row.symbol)}</td>
                <td data-label="Mode">{formatValue(row.mode)}</td>
                <td data-label="Parent Order ID" className="dashboard-cell-muted">{formatValue(row.broker_parent_order_id)}</td>
                <td data-label="Client Order ID" className="dashboard-cell-muted">{formatValue(row.client_order_id)}</td>
                <td data-label="Local Entry Time" className="dashboard-cell-muted">{formatTimestamp(row.local_entry_timestamp_utc)}</td>
                <td data-label="Local Exit Time" className="dashboard-cell-muted">{formatTimestamp(row.local_exit_timestamp_utc)}</td>
                <td data-label="Local Entry Price">{formatCurrency(row.local_entry_price)}</td>
                <td data-label="Live Entry Price">{formatCurrency(row.broker_entry_price)}</td>
                <td data-label="Entry Price Diff">{formatCurrency(row.entry_price_diff)}</td>
                <td data-label="Local Exit Price">{formatCurrency(row.local_exit_price)}</td>
                <td data-label="Live Exit Price">{formatCurrency(row.broker_exit_price)}</td>
                <td data-label="Exit Price Diff">{formatCurrency(row.exit_price_diff)}</td>
                <td data-label="Local Shares">{formatNumber(row.local_shares, 0)}</td>
                <td data-label="Live Entry Qty">{formatNumber(row.broker_entry_qty, 0)}</td>
                <td data-label="Live Exit Qty">{formatNumber(row.broker_exit_qty, 0)}</td>
                <td data-label="Local Exit Reason">{formatValue(row.local_exit_reason)}</td>
                <td data-label="Live Exit Reason">{formatValue(row.broker_exit_reason)}</td>
                <td data-label="Live Exit Order ID" className="dashboard-cell-muted">{formatValue(row.broker_exit_order_id)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
