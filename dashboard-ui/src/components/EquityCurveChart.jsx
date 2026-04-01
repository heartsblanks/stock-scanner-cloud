import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

function formatCurrency(value) {
  const numeric = Number(value ?? 0);
  return `${numeric < 0 ? "-" : ""}$${Math.abs(numeric).toFixed(2)}`;
}

function EquityTooltip({ active, payload, label }) {
  if (!active || !payload?.length) {
    return null;
  }

  return (
    <div className="dashboard-chart-tooltip">
      <div className="dashboard-chart-tooltip-label">{label || "Timestamp"}</div>
      <div className="dashboard-chart-tooltip-row">
        <span className="dashboard-chart-tooltip-name">Cumulative P&amp;L</span>
        <span className="dashboard-chart-tooltip-value">{formatCurrency(payload[0]?.value)}</span>
      </div>
    </div>
  );
}

export default function EquityCurveChart({ rows }) {
  const data = Array.isArray(rows) ? rows : [];

  if (!data.length) {
    return <div className="dashboard-chart-empty">No equity curve data.</div>;
  }

  return (
    <div className="dashboard-chart-card">
      <div className="dashboard-chart-header">
        <div>
          <div className="dashboard-chart-title">Equity Curve</div>
          <div className="dashboard-chart-subtitle">
            Tracks cumulative realized performance through the trading sequence.
          </div>
        </div>
        <div className="dashboard-chart-meta">Trend</div>
      </div>

      <div style={{ width: "100%", height: 300 }}>
        <ResponsiveContainer>
          <LineChart data={data} margin={{ top: 10, right: 10, left: -16, bottom: 0 }}>
            <CartesianGrid stroke="rgba(80, 62, 37, 0.08)" strokeDasharray="4 8" vertical={false} />
            <XAxis
              dataKey="timestamp"
              stroke="#8c7f69"
              tick={{ fill: "#7b705f", fontSize: 12 }}
              tickLine={false}
              axisLine={false}
              minTickGap={32}
            />
            <YAxis
              stroke="#8c7f69"
              tick={{ fill: "#7b705f", fontSize: 12 }}
              tickLine={false}
              axisLine={false}
              tickFormatter={formatCurrency}
            />
            <Tooltip content={<EquityTooltip />} cursor={{ stroke: "rgba(187, 90, 42, 0.24)", strokeWidth: 1 }} />
            <Line
              type="monotone"
              dataKey="cumulative_pnl"
              stroke="#bb5a2a"
              strokeWidth={3}
              dot={false}
              activeDot={{ r: 5, fill: "#17312c", stroke: "#fff5ea", strokeWidth: 2 }}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
