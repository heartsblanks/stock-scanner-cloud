import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

function formatCurrency(value) {
  const numeric = Number(value ?? 0);
  return `${numeric < 0 ? "-" : ""}$${Math.abs(numeric).toFixed(2)}`;
}

function PerformanceTooltip({ active, payload, label }) {
  if (!active || !payload?.length) {
    return null;
  }

  return (
    <div className="dashboard-chart-tooltip">
      <div className="dashboard-chart-tooltip-label">{label || "Symbol"}</div>
      <div className="dashboard-chart-tooltip-row">
        <span className="dashboard-chart-tooltip-name">Realized P&amp;L</span>
        <span className="dashboard-chart-tooltip-value">{formatCurrency(payload[0]?.value)}</span>
      </div>
    </div>
  );
}

export default function SymbolPerformanceChart({ rows }) {
  const data = Array.isArray(rows) ? rows : [];

  if (!data.length) {
    return <div className="dashboard-chart-empty">No symbol performance data.</div>;
  }

  return (
    <div className="dashboard-chart-card">
      <div className="dashboard-chart-header">
        <div>
          <div className="dashboard-chart-title">Symbol Performance</div>
          <div className="dashboard-chart-subtitle">Which names are carrying the book and which are dragging.</div>
        </div>
        <div className="dashboard-chart-meta">Symbols</div>
      </div>
      <div style={{ width: "100%", height: 280 }}>
        <ResponsiveContainer>
          <BarChart data={data} margin={{ top: 10, right: 10, left: -16, bottom: 0 }}>
            <CartesianGrid stroke="rgba(80, 62, 37, 0.08)" strokeDasharray="4 8" vertical={false} />
            <XAxis
              dataKey="symbol"
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
            <Tooltip content={<PerformanceTooltip />} cursor={{ fill: "rgba(187, 90, 42, 0.08)" }} />
            <Bar dataKey="realized_pnl_total" name="P&L" fill="#bb5a2a" radius={[8, 8, 2, 2]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
