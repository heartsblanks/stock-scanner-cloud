

function formatValue(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  if (typeof value === "number") {
    return value.toFixed(2);
  }
  return String(value);
}

export default function OpenTradesTable({ trades }) {
  if (!trades || trades.length === 0) {
    return <div style={{ marginTop: 12 }}>No open trades.</div>;
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
            <th style={thStyle}>Shares</th>
            <th style={thStyle}>Entry Price</th>
            <th style={thStyle}>Stop</th>
            <th style={thStyle}>Target</th>
            <th style={thStyle}>Entry Time</th>
          </tr>
        </thead>
        <tbody>
          {trades.map((trade, index) => (
            <tr key={`${trade.trade_key || trade.symbol || "trade"}-${index}`}>
              <td style={tdStyle}>{formatValue(trade.symbol)}</td>
              <td style={tdStyle}>{formatValue(trade.mode)}</td>
              <td style={tdStyle}>{formatValue(trade.status)}</td>
              <td style={tdStyle}>{formatValue(trade.shares)}</td>
              <td style={tdStyle}>{formatValue(trade.entry_price)}</td>
              <td style={tdStyle}>{formatValue(trade.stop_price)}</td>
              <td style={tdStyle}>{formatValue(trade.target_price)}</td>
              <td style={tdStyle}>{formatValue(trade.entry_time)}</td>
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
};

const tdStyle = {
  padding: "10px",
  borderBottom: "1px solid #eee",
  fontSize: 14,
};