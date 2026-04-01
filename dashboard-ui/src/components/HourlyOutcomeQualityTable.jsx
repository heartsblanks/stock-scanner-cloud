function formatCurrency(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }

  const numeric = Number(value);
  return `${numeric < 0 ? "-" : ""}$${Math.abs(numeric).toFixed(2)}`;
}

function formatPercent(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  return `${Number(value).toFixed(1)}%`;
}

function formatHourLabel(hour) {
  if (hour === null || hour === undefined || hour === "") {
    return "-";
  }

  const numericHour = Number(hour);
  if (Number.isNaN(numericHour)) {
    return "-";
  }

  const suffix = numericHour >= 12 ? "PM" : "AM";
  const normalizedHour = numericHour % 12 || 12;
  return `${normalizedHour}:00 ${suffix} ET`;
}

export default function HourlyOutcomeQualityTable({ rows }) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return <div className="dashboard-empty">No hourly outcome quality data.</div>;
  }

  return (
    <div className="dashboard-panel dashboard-panel-strong">
      <div className="dashboard-panel-body dashboard-panel-body-tight">
        <div className="dashboard-panel-heading">
          <div>
            <h3>Outcome Quality By Entry Hour</h3>
            <p className="dashboard-panel-subtitle">
              Realized quality by ET hour so execution pressure can be compared with actual trade outcomes.
            </p>
          </div>
        </div>

        <div className="dashboard-table-wrap">
          <table className="dashboard-table">
            <thead>
              <tr>
                <th>Hour</th>
                <th>Trades</th>
                <th>Wins / Losses</th>
                <th>Win Rate</th>
                <th>Realized P&amp;L</th>
                <th>Avg Trade P&amp;L</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.entry_hour_ny}>
                  <td data-label="Hour">{formatHourLabel(row.entry_hour_ny)}</td>
                  <td data-label="Trades">{row.trade_count ?? 0}</td>
                  <td data-label="Wins / Losses">
                    {row.winning_trade_count ?? 0} / {row.losing_trade_count ?? 0}
                  </td>
                  <td data-label="Win Rate">{formatPercent(row.win_rate)}</td>
                  <td data-label="Realized P&L">{formatCurrency(row.realized_pnl_total)}</td>
                  <td data-label="Avg Trade P&L">{formatCurrency(row.average_realized_pnl)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
