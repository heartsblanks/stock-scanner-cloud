function formatValue(value, { isPercent = false, isCurrency = false } = {}) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  if (typeof value === "number") {
    const v = Number.isFinite(value) ? value : 0;
    if (isPercent) {
      return `${v.toFixed(2)}%`;
    }
    if (isCurrency) {
      return `$${v.toFixed(2)}`;
    }
    return v.toFixed(2);
  }
  return String(value);
}

function Card({ title, value, valueColor, isPercent = false, isCurrency = false }) {
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
      <div
        style={{
          fontSize: 24,
          fontWeight: 600,
          color: valueColor || "#111",
        }}
      >
        {formatValue(value, { isPercent, isCurrency })}
      </div>
    </div>
  );
}

function pnlColor(v) {
  if (typeof v !== "number") return undefined;
  if (v > 0) return "#16a34a"; // green
  if (v < 0) return "#dc2626"; // red
  return undefined;
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
      <Card
        title="Realized P&L"
        value={summary.realized_pnl_total}
        isCurrency
        valueColor={pnlColor(summary.realized_pnl_total)}
      />
      <Card title="Winning Trades" value={summary.winning_trade_count} />
      <Card title="Losing Trades" value={summary.losing_trade_count} />
      <Card
        title="Win Rate %"
        value={summary.win_rate_percent}
        isPercent
      />
      <Card
        title="Average P&L"
        value={summary.average_realized_pnl}
        isCurrency
        valueColor={pnlColor(summary.average_realized_pnl)}
      />
      <Card
        title="Best Trade"
        value={summary.best_trade_pnl}
        isCurrency
        valueColor={pnlColor(summary.best_trade_pnl)}
      />
      <Card
        title="Worst Trade"
        value={summary.worst_trade_pnl}
        isCurrency
        valueColor={pnlColor(summary.worst_trade_pnl)}
      />
      <Card
        title="Avg Duration (min)"
        value={summary.average_duration_minutes}
      />
    </div>
  );
}
