

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
          borderRadius: 8,
          overflow: "hidden",
          boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
        }}
      >
        <thead>
          <tr>
            <th style={thStyle}>Symbol</th>
            <th style={thStyle}>Mode</th>
            <th style={thStyle}>Status</th>
            <th style={thStyle}>Shares</th>
            <th style={thStyle}>Position Cost</th>
            <th style={thStyle}>Per Trade Notional</th>
            <th style={thStyle}>Entry Price</th>
            <th style={thStyle}>Stop</th>
            <th style={thStyle}>Target</th>
            <th style={thStyle}>Take Profit ($)</th>
            <th style={thStyle}>Entry Time</th>
          </tr>
        </thead>
        <tbody>
          {trades.map((trade, index) => (
            <tr
              key={`${trade.trade_key || trade.symbol || "trade"}-${index}`}
              style={{ transition: "background 0.2s" }}
              onMouseEnter={(e) => (e.currentTarget.style.background = "#f9fafb")}
              onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
            >
              <td style={tdStyle}>{formatValue(trade.symbol)}</td>
              <td style={tdStyle}>{formatValue(trade.mode)}</td>
              <td style={tdStyle}>
                <span
                  style={{
                    padding: "4px 8px",
                    borderRadius: 6,
                    fontSize: 12,
                    fontWeight: 600,
                    color: "#fff",
                    background:
                      trade.status === "OPEN"
                        ? "#2ecc71"
                        : trade.status === "PENDING"
                        ? "#f39c12"
                        : "#95a5a6",
                  }}
                >
                  {formatValue(trade.status)}
                </span>
              </td>
              <td style={tdStyle}>{formatValue(trade.shares)}</td>
              <td style={tdStyle}>{formatValue(trade.position_cost)}</td>
              <td style={tdStyle}>{formatValue(trade.per_trade_notional)}</td>
              <td style={tdStyle}>{formatValue(trade.entry_price)}</td>
              <td style={tdStyle}>{formatValue(trade.stop_price)}</td>
              <td style={tdStyle}>{formatValue(trade.target_price)}</td>
              <td style={tdStyle}>{formatValue(trade.take_profit_dollars)}</td>
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
  borderBottom: "1px solid #e5e7eb",
  background: "#f3f4f6",
  fontSize: 13,
  fontWeight: 600,
  color: "#374151",
};

const tdStyle = {
  padding: "10px",
  borderBottom: "1px solid #f1f5f9",
  fontSize: 13,
  color: "#111827",
};