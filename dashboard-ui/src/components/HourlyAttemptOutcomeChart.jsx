import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

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
  return `${normalizedHour}${suffix}`;
}

function normalizeChartData(rows) {
  if (!Array.isArray(rows)) {
    return [];
  }

  return rows.map((row) => {
    const nonPlacedCount =
      Number(row?.scan_rejected_count || 0) +
      Number(row?.refresh_rejected_count || 0) +
      Number(row?.placement_skipped_count || 0) +
      Number(row?.placement_rejected_count || 0);

    return {
      hourNy: Number(row?.hour_ny ?? -1),
      label: formatHourLabel(row?.hour_ny),
      candidateCount: Number(row?.candidate_count ?? 0),
      placedCount: Number(row?.placed_count ?? 0),
      nonPlacedCount,
      placementRate: Number(row?.placement_rate ?? 0),
      topReason: row?.top_non_placement_reason || "",
    };
  });
}

function HourlyAttemptTooltip({ active, payload, label }) {
  if (!active || !payload?.length) {
    return null;
  }

  const row = payload[0]?.payload || {};

  return (
    <div className="dashboard-chart-tooltip">
      <div className="dashboard-chart-tooltip-label">{label || "-"}</div>
      <div className="dashboard-chart-tooltip-row">
        <span className="dashboard-chart-tooltip-name">Candidates</span>
        <span className="dashboard-chart-tooltip-value">{row.candidateCount ?? 0}</span>
      </div>
      <div className="dashboard-chart-tooltip-row">
        <span className="dashboard-chart-tooltip-name">Placed</span>
        <span className="dashboard-chart-tooltip-value">{row.placedCount ?? 0}</span>
      </div>
      <div className="dashboard-chart-tooltip-row">
        <span className="dashboard-chart-tooltip-name">Non-Placed</span>
        <span className="dashboard-chart-tooltip-value">{row.nonPlacedCount ?? 0}</span>
      </div>
      <div className="dashboard-chart-tooltip-row">
        <span className="dashboard-chart-tooltip-name">Placement Rate</span>
        <span className="dashboard-chart-tooltip-value">{formatPercent(row.placementRate)}</span>
      </div>
      <div className="dashboard-chart-tooltip-row">
        <span className="dashboard-chart-tooltip-name">Top Reason</span>
        <span className="dashboard-chart-tooltip-value">{row.topReason || "-"}</span>
      </div>
    </div>
  );
}

export default function HourlyAttemptOutcomeChart({ rows }) {
  const data = normalizeChartData(rows);

  if (!data.length) {
    return <div className="dashboard-chart-empty">No hourly execution attempt data.</div>;
  }

  return (
    <div className="dashboard-chart-card">
      <div className="dashboard-chart-header">
        <div>
          <div className="dashboard-chart-title">Execution Attempt Outcomes (ET)</div>
          <div className="dashboard-chart-subtitle">
            Compare candidate volume, actual placements, and non-placement pressure by session hour.
          </div>
        </div>
        <div className="dashboard-chart-meta">Attempts</div>
      </div>
      <div style={{ width: "100%", height: 320 }}>
        <ResponsiveContainer>
          <BarChart data={data} margin={{ top: 10, right: 10, left: -16, bottom: 0 }}>
            <CartesianGrid stroke="rgba(80, 62, 37, 0.08)" strokeDasharray="4 8" vertical={false} />
            <XAxis
              dataKey="label"
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
            />
            <Tooltip content={<HourlyAttemptTooltip />} cursor={{ fill: "rgba(187, 90, 42, 0.08)" }} />
            <Legend />
            <Bar dataKey="candidateCount" name="Candidates" fill="#d39b63" radius={[8, 8, 0, 0]} />
            <Bar dataKey="placedCount" name="Placed" fill="#167a4b" radius={[8, 8, 0, 0]} />
            <Bar dataKey="nonPlacedCount" name="Non-Placed" fill="#b4412f" radius={[8, 8, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
