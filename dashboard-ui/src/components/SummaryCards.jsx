function formatValue(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  if (typeof value === "number") {
    return value.toFixed(2);
  }
  return String(value);
}

function Card({ title, value }) {
  return (
    <div
      style={{
        border: "1px solid #ddd",
        borderRadius: 8,
        padding: 16,
        minWidth: 180,
        background: "#fff",
      }}
    >
      <div style={{ fontSize: 14, color: "#666", marginBottom: 8 }}>{title}</div>
      <div style={{ fontSize: 24, fontWeight: 600 }}>{formatValue(value)}</div>
    </div>
  );
}

export default function SummaryCards({ data }) {
  const summary = data?.summary || {};

  return (
    <div
      style={{
        display: "flex",
        gap: 16,
        flexWrap: "wrap",
        marginTop: 20,
      }}
    >
      <Card title="Trades" value={summary.trade_count} />
      <Card title="Open Trades" value={summary.open_trade_count} />
      <Card title="Closed Trades" value={summary.closed_trade_count} />
      <Card title="Realized P&L" value={summary.realized_pnl_total} />
      <Card title="Winning Trades" value={summary.winning_trade_count} />
      <Card title="Losing Trades" value={summary.losing_trade_count} />
      <Card title="Win Rate %" value={summary.win_rate_percent} />
      <Card title="Average P&L" value={summary.average_realized_pnl} />
      <Card title="Best Trade" value={summary.best_trade_pnl} />
      <Card title="Worst Trade" value={summary.worst_trade_pnl} />
      <Card title="Avg Duration (min)" value={summary.average_duration_minutes} />
    </div>
  );
}
