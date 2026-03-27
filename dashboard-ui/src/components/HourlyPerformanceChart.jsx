

import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, Tooltip, CartesianGrid } from "recharts";

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

function formatTooltipValue(value, name) {
  if (typeof value === "number") {
    return [value.toFixed(2), name];
  }
  return [value, name];
}

export default function HourlyPerformanceChart({ rows }) {
  const data = normalizeChartData(rows);

  if (!data.length) {
    return <div style={{ marginTop: 12 }}>No hourly performance data.</div>;
  }

  return (
    <div
      style={{
        background: "#fff",
        borderRadius: 8,
        padding: 16,
        boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
        marginTop: 12,
      }}
    >
      <h3 style={{ marginTop: 0, marginBottom: 12 }}>Hourly Performance (UTC)</h3>
      <div style={{ width: "100%", height: 280 }}>
        <ResponsiveContainer>
          <BarChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="entryHourUtc" />
            <YAxis />
            <Tooltip formatter={formatTooltipValue} />
            <Bar dataKey="realizedPnlTotal" name="Realized P&amp;L" />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}