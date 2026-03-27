import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, Tooltip, CartesianGrid } from "recharts";

export default function ModePerformanceChart({ rows }) {
  const data = Array.isArray(rows) ? rows : [];

  if (!data.length) {
    return <div>No mode performance data.</div>;
  }

  return (
    <div style={{ background: "#fff", padding: 16, borderRadius: 8, boxShadow: "0 2px 8px rgba(0,0,0,0.05)" }}>
      <h3 style={{ marginTop: 0 }}>Mode Performance</h3>
      <div style={{ width: "100%", height: 260 }}>
        <ResponsiveContainer>
          <BarChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="mode" />
            <YAxis />
            <Tooltip />
            <Bar dataKey="realized_pnl_total" name="P&L" />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
