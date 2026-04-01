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

function Card({ title, value, valueColor, accent, isPercent = false, isCurrency = false }) {
  return (
    <div
      className={`metric-card${title === "Realized P&L" ? " metric-card-featured" : ""}`}
      style={accent ? { "--metric-accent": accent } : undefined}
    >
      <div className="metric-card-label">{title}</div>
      <div className="metric-card-value" style={{ color: valueColor || undefined }}>
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
    <div className="dashboard-section">
      <div className="dashboard-panel dashboard-panel-strong">
        <div className="dashboard-panel-body">
          <div className="dashboard-panel-heading">
            <div>
              <h2 className="dashboard-panel-title">Performance Snapshot</h2>
              <p className="dashboard-panel-subtitle">
                The fastest read on realized P&amp;L, win rate, trade flow, and how efficiently the book is closing.
              </p>
            </div>
          </div>
          <div className="dashboard-metrics-grid">
      <Card title="Trades" value={summary.trade_count} accent="#8a3a17" />
      <Card title="Open Trades" value={summary.open_trade_count} accent="#0f766e" />
      <Card title="Closed Trades" value={summary.closed_trade_count} accent="#315bb6" />
      <Card
        title="Realized P&L"
        value={summary.realized_pnl_total}
        accent="#bb5a2a"
        isCurrency
        valueColor={pnlColor(summary.realized_pnl_total)}
      />
      <Card title="Winning Trades" value={summary.winning_trade_count} accent="#167a4b" />
      <Card title="Losing Trades" value={summary.losing_trade_count} accent="#b4412f" />
      <Card
        title="Win Rate %"
        value={summary.win_rate_percent}
        accent="#1d6f67"
        isPercent
      />
      <Card
        title="Average P&L"
        value={summary.average_realized_pnl}
        accent="#906245"
        isCurrency
        valueColor={pnlColor(summary.average_realized_pnl)}
      />
      <Card
        title="Best Trade"
        value={summary.best_trade_pnl}
        accent="#167a4b"
        isCurrency
        valueColor={pnlColor(summary.best_trade_pnl)}
      />
      <Card
        title="Worst Trade"
        value={summary.worst_trade_pnl}
        accent="#b4412f"
        isCurrency
        valueColor={pnlColor(summary.worst_trade_pnl)}
      />
      <Card
        title="Avg Duration (min)"
        value={summary.average_duration_minutes}
        accent="#6b7280"
      />
          </div>
        </div>
      </div>
    </div>
  );
}
