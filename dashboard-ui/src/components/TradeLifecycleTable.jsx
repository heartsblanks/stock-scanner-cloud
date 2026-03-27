function formatValue(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  if (typeof value === "number") {
    return value.toFixed(2);
  }
  return String(value);
}

export default function TradeLifecycleTable({ rows }) {
  if (!rows || rows.length === 0) {
    return <div style={{ marginTop: 12 }}>No trade lifecycle data.</div>;
  }

  return (
    <div style={{ overflowX: "auto", marginTop: 12 }}>
      <table
        style={{
          width: "100%",
          borderCollapse: "collapse",
          background: "#fff",
        }}
      >
        <thead>
          <tr>
            <th style={thStyle}>Symbol</th>
            <th style={thStyle}>Mode</th>
            <th style={thStyle}>Status</th>
            <th style={thStyle}>Direction</th>
            <th style={thStyle}>Shares</th>
            <th style={thStyle}>Entry Price</th>
            <th style={thStyle}>Exit Price</th>
            <th style={thStyle}>P&amp;L</th>
            <th style={thStyle}>P&amp;L %</th>
            <th style={thStyle}>Exit Reason</th>
            <th style={thStyle}>Duration (min)</th>
            <th style={thStyle}>Entry Time</th>
            <th style={thStyle}>Exit Time</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={`${row.trade_key || row.symbol || "lifecycle"}-${index}`}>
              <td style={tdStyle}>{formatValue(row.symbol)}</td>
              <td style={tdStyle}>{formatValue(row.mode)}</td>
              <td style={tdStyle}>{formatValue(row.status)}</td>
              <td style={tdStyle}>{formatValue(row.direction)}</td>
              <td style={tdStyle}>{formatValue(row.shares)}</td>
              <td style={tdStyle}>{formatValue(row.entry_price)}</td>
              <td style={tdStyle}>{formatValue(row.exit_price)}</td>
              <td style={tdStyle}>{formatValue(row.realized_pnl)}</td>
              <td style={tdStyle}>{formatValue(row.realized_pnl_percent)}</td>
              <td style={tdStyle}>{formatValue(row.exit_reason)}</td>
              <td style={tdStyle}>{formatValue(row.duration_minutes)}</td>
              <td style={tdStyle}>{formatValue(row.entry_time)}</td>
              <td style={tdStyle}>{formatValue(row.exit_time)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

const thStyle = {
  textAlign: "left",
  padding: "12px 10px",
  borderBottom: "1px solid #ddd",
  background: "#f7f7f7",
  fontSize: 14,
  whiteSpace: "nowrap",
};

const tdStyle = {
  padding: "10px",
  borderBottom: "1px solid #eee",
  fontSize: 14,
  whiteSpace: "nowrap",
};
