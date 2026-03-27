import { ResponsiveContainer, LineChart, Line, XAxis, YAxis, Tooltip, CartesianGrid } from "recharts";

export default function EquityCurveChart({ rows }) {
  const data = Array.isArray(rows) ? rows : [];

  if (!data.length) {
    return <div>No equity curve data.</div>;
  }

  return (
    <div
      style={{
        background: "#fff",
        padding: 16,
        borderRadius: 8,
        boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
      }}
    >
      <h3 style={{ marginTop: 0 }}>Equity Curve</h3>
      <div style={{ width: "100%", height: 280 }}>
        <ResponsiveContainer>
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="timestamp" />
            <YAxis />
            <Tooltip />
            <Line type="monotone" dataKey="cumulative_pnl" />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
