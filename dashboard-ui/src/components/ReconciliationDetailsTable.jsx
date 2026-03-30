import React from "react";

function formatValue(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  return value;
}

const tableWrapStyle = {
  background: "#fff",
  borderRadius: 8,
  padding: 16,
  boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
  overflowX: "auto",
};

const tableStyle = {
  width: "100%",
  borderCollapse: "collapse",
  minWidth: 1200,
};

const thStyle = {
  textAlign: "left",
  padding: "10px 12px",
  borderBottom: "1px solid #e5e7eb",
  fontSize: 13,
  color: "#374151",
  background: "#f9fafb",
  whiteSpace: "nowrap",
};

const tdStyle = {
  padding: "10px 12px",
  borderBottom: "1px solid #f1f5f9",
  fontSize: 13,
  color: "#111827",
  verticalAlign: "top",
  whiteSpace: "nowrap",
};

function getStatusStyle(status) {
  const normalized = String(status || "").trim().toLowerCase();

  if (normalized === "matched") {
    return {
      background: "#dcfce7",
      color: "#166534",
    };
  }

  if (
    normalized === "missing_in_alpaca" ||
    normalized === "missing_in_db" ||
    normalized === "exit_not_resolved"
  ) {
    return {
      background: "#fee2e2",
      color: "#991b1b",
    };
  }

  return {
    background: "#fef3c7",
    color: "#92400e",
  };
}

function StatusBadge({ status }) {
  const style = getStatusStyle(status);

  return (
    <span
      style={{
        display: "inline-block",
        padding: "4px 8px",
        borderRadius: 999,
        fontSize: 12,
        fontWeight: 600,
        background: style.background,
        color: style.color,
      }}
    >
      {formatValue(status)}
    </span>
  );
}

export default function ReconciliationDetailsTable({ rows = [] }) {
  return (
    <div style={tableWrapStyle}>
      {rows.length === 0 ? (
        <div style={{ color: "#6b7280", fontSize: 14 }}>
          No reconciliation detail rows available
        </div>
      ) : (
        <table style={tableStyle}>
          <thead>
            <tr>
              <th style={thStyle}>Status</th>
              <th style={thStyle}>Symbol</th>
              <th style={thStyle}>Mode</th>
              <th style={thStyle}>Parent Order ID</th>
              <th style={thStyle}>Client Order ID</th>
              <th style={thStyle}>Local Entry Time</th>
              <th style={thStyle}>Local Exit Time</th>
              <th style={thStyle}>Local Entry Price</th>
              <th style={thStyle}>Alpaca Entry Price</th>
              <th style={thStyle}>Entry Price Diff</th>
              <th style={thStyle}>Local Exit Price</th>
              <th style={thStyle}>Alpaca Exit Price</th>
              <th style={thStyle}>Exit Price Diff</th>
              <th style={thStyle}>Local Shares</th>
              <th style={thStyle}>Alpaca Entry Qty</th>
              <th style={thStyle}>Alpaca Exit Qty</th>
              <th style={thStyle}>Local Exit Reason</th>
              <th style={thStyle}>Alpaca Exit Reason</th>
              <th style={thStyle}>Alpaca Exit Order ID</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, index) => (
              <tr key={`${row.broker_parent_order_id || "row"}-${index}`}>
                <td style={tdStyle}>
                  <StatusBadge status={row.match_status} />
                </td>
                <td style={tdStyle}>{formatValue(row.symbol)}</td>
                <td style={tdStyle}>{formatValue(row.mode)}</td>
                <td style={tdStyle}>{formatValue(row.broker_parent_order_id)}</td>
                <td style={tdStyle}>{formatValue(row.client_order_id)}</td>
                <td style={tdStyle}>{formatValue(row.local_entry_timestamp_utc)}</td>
                <td style={tdStyle}>{formatValue(row.local_exit_timestamp_utc)}</td>
                <td style={tdStyle}>{formatValue(row.local_entry_price)}</td>
                <td style={tdStyle}>{formatValue(row.alpaca_entry_price)}</td>
                <td style={tdStyle}>{formatValue(row.entry_price_diff)}</td>
                <td style={tdStyle}>{formatValue(row.local_exit_price)}</td>
                <td style={tdStyle}>{formatValue(row.alpaca_exit_price)}</td>
                <td style={tdStyle}>{formatValue(row.exit_price_diff)}</td>
                <td style={tdStyle}>{formatValue(row.local_shares)}</td>
                <td style={tdStyle}>{formatValue(row.alpaca_entry_qty)}</td>
                <td style={tdStyle}>{formatValue(row.alpaca_exit_qty)}</td>
                <td style={tdStyle}>{formatValue(row.local_exit_reason)}</td>
                <td style={tdStyle}>{formatValue(row.alpaca_exit_reason)}</td>
                <td style={tdStyle}>{formatValue(row.alpaca_exit_order_id)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
