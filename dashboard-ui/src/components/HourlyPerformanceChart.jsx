import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

function normalizeChartData(rows) {
  if (!Array.isArray(rows)) {
    return [];
  }

  return rows.map((row) => ({
    entryHourUtc: row?.entry_hour_utc ?? "-",
    tradeCount: Number(row?.trade_count ?? 0),
    closedTradeCount: Number(row?.closed_trade_count ?? 0),
    realizedPnlTotal: Number(row?.realized_pnl_total ?? 0),
    averageRealizedPnl: Number(row?.average_realized_pnl ?? 0),
    averageDurationMinutes: Number(row?.average_duration_minutes ?? 0),
  }));
}

function formatCurrency(value) {
  const numeric = Number(value ?? 0);
  return `${numeric < 0 ? "-" : ""}$${Math.abs(numeric).toFixed(2)}`;
}

function HourlyTooltip({ active, payload, label }) {
  if (!active || !payload?.length) {
    return null;
  }

  const row = payload[0]?.payload || {};

  return (
    <div className="dashboard-chart-tooltip">
      <div className="dashboard-chart-tooltip-label">Hour {label ?? "-"}</div>
      <div className="dashboard-chart-tooltip-row">
        <span className="dashboard-chart-tooltip-name">Realized P&amp;L</span>
        <span className="dashboard-chart-tooltip-value">{formatCurrency(row.realizedPnlTotal)}</span>
      </div>
      <div className="dashboard-chart-tooltip-row">
        <span className="dashboard-chart-tooltip-name">Trades</span>
        <span className="dashboard-chart-tooltip-value">{row.tradeCount ?? 0}</span>
      </div>
      <div className="dashboard-chart-tooltip-row">
        <span className="dashboard-chart-tooltip-name">Avg Duration</span>
        <span className="dashboard-chart-tooltip-value">{Number(row.averageDurationMinutes ?? 0).toFixed(1)}m</span>
      </div>
    </div>
  );
}

export default function HourlyPerformanceChart({ rows, onHourSelect }) {
  const data = normalizeChartData(rows);

  if (!data.length) {
    return <div className="dashboard-chart-empty">No hourly performance data.</div>;
  }

  return (
    <div className="dashboard-chart-card">
      <div className="dashboard-chart-header">
        <div>
          <div className="dashboard-chart-title">Hourly Performance (UTC)</div>
          <div className="dashboard-chart-subtitle">
            Reveals where the edge improves or fades across the intraday schedule.
          </div>
        </div>
        <div className="dashboard-chart-meta">Intraday</div>
      </div>
      <div style={{ width: "100%", height: 300 }}>
        <ResponsiveContainer>
          <BarChart data={data} margin={{ top: 10, right: 10, left: -16, bottom: 0 }}>
            <CartesianGrid stroke="rgba(80, 62, 37, 0.08)" strokeDasharray="4 8" vertical={false} />
            <XAxis
              dataKey="entryHourUtc"
              stroke="#8c7f69"
              tick={{ fill: "#7b705f", fontSize: 12 }}
              tickLine={false}
              axisLine={false}
            />
            <YAxis
              stroke="#8c7f69"
              tick={{ fill: "#7b705f", fontSize: 12 }}
              tickLine={false}
              axisLine={false}
              tickFormatter={formatCurrency}
            />
            <Tooltip content={<HourlyTooltip />} cursor={{ fill: "rgba(49, 91, 182, 0.08)" }} />
            <Bar
              dataKey="realizedPnlTotal"
              name="Realized P&L"
              fill="#315bb6"
              radius={[8, 8, 2, 2]}
              onClick={(payload) => onHourSelect?.(payload?.entryHourUtc)}
              style={{ cursor: onHourSelect ? "pointer" : "default" }}
            />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
