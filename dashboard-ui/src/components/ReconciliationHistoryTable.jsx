

import React from "react";

function formatValue(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  return value;
}

function getStatusStyle(status) {
  const normalized = String(status || "").trim().toUpperCase();

  if (normalized === "OK") {
    return {
      background: "#dcfce7",
      color: "#166534",
    };
  }

  if (normalized === "WARNING") {
    return {
      background: "#fef3c7",
      color: "#92400e",
    };
  }

  if (normalized === "CRITICAL" || normalized === "FAILED") {
    return {
      background: "#fee2e2",
      color: "#991b1b",
    };
  }

  return {
    background: "#e5e7eb",
    color: "#374151",
  };
}

function StatusBadge({ status }) {
  const style = getStatusStyle(status);

  return (
    <span
      style={{
        display: "inline-block",
        padding: "4px 8px",
        borderRadius: 999,
        fontSize: 12,
        fontWeight: 600,
        background: style.background,
        color: style.color,
      }}
    >
      {formatValue(status)}
    </span>
  );
}

const wrapStyle = {
  background: "#fff",
  borderRadius: 8,
  padding: 16,
  boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
  overflowX: "auto",
};

const tableStyle = {
  width: "100%",
  borderCollapse: "collapse",
  minWidth: 900,
};

const thStyle = {
  textAlign: "left",
  padding: "10px 12px",
  borderBottom: "1px solid #e5e7eb",
  fontSize: 13,
  color: "#374151",
  background: "#f9fafb",
  whiteSpace: "nowrap",
};

const tdStyle = {
  padding: "10px 12px",
  borderBottom: "1px solid #f1f5f9",
  fontSize: 13,
  color: "#111827",
  verticalAlign: "top",
  whiteSpace: "nowrap",
};

export default function ReconciliationHistoryTable({ rows = [] }) {
  return (
    <div style={wrapStyle}>
      {rows.length === 0 ? (
        <div style={{ color: "#6b7280", fontSize: 14 }}>
          No reconciliation history available
        </div>
      ) : (
        <table style={tableStyle}>
          <thead>
            <tr>
              <th style={thStyle}>Run ID</th>
              <th style={thStyle}>Run Time</th>
              <th style={thStyle}>Status</th>
              <th style={thStyle}>Mismatch Count</th>
              <th style={thStyle}>Matched Count</th>
              <th style={thStyle}>Unmatched Count</th>
              <th style={thStyle}>Notes</th>
              <th style={thStyle}>Created At</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, index) => {
              const mismatchCount =
                row.mismatch_count ?? row.unmatched_count ?? row.unmatched_rows ?? 0;
              const matchedCount = row.matched_count ?? row.matched_rows ?? 0;
              const status = row.severity || row.status || (mismatchCount > 0 ? "WARNING" : "OK");

              return (
                <tr key={`${row.id || "run"}-${index}`}>
                  <td style={tdStyle}>{formatValue(row.id)}</td>
                  <td style={tdStyle}>{formatValue(row.run_time || row.run_started_at)}</td>
                  <td style={tdStyle}>
                    <StatusBadge status={status} />
                  </td>
                  <td style={tdStyle}>{formatValue(mismatchCount)}</td>
                  <td style={tdStyle}>{formatValue(matchedCount)}</td>
                  <td style={tdStyle}>{formatValue(row.unmatched_count ?? row.unmatched_rows)}</td>
                  <td style={tdStyle}>{formatValue(row.notes)}</td>
                  <td style={tdStyle}>{formatValue(row.created_at || row.run_completed_at)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </div>
  );
}