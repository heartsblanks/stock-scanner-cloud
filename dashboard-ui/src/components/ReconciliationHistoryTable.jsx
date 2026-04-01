import { formatNumber, formatTimestamp, formatValue } from "./tableFormatters";

function getStatusClass(status) {
  const normalized = String(status || "").trim().toUpperCase();

  if (normalized === "OK") {
    return "dashboard-badge dashboard-badge-ok";
  }

  if (normalized === "WARNING") {
    return "dashboard-badge dashboard-badge-warn";
  }

  if (normalized === "CRITICAL" || normalized === "FAILED") {
    return "dashboard-badge dashboard-badge-danger";
  }

  return "dashboard-badge dashboard-badge-neutral";
}

function rowTone(status) {
  const normalized = String(status || "").trim().toUpperCase();
  if (normalized === "CRITICAL" || normalized === "FAILED") {
    return "dashboard-table-row-negative";
  }
  if (normalized === "OK") {
    return "dashboard-table-row-positive";
  }
  return "";
}

export default function ReconciliationHistoryTable({ rows = [] }) {
  return (
    <div className="dashboard-table-wrap">
      {rows.length === 0 ? (
        <div className="dashboard-empty">No reconciliation history available</div>
      ) : (
        <table className="dashboard-table" style={{ minWidth: 900 }}>
          <thead>
            <tr>
              <th>Run ID</th>
              <th>Run Time</th>
              <th>Status</th>
              <th>Mismatch Count</th>
              <th>Matched Count</th>
              <th>Unmatched Count</th>
              <th>Notes</th>
              <th>Created At</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, index) => {
              const mismatchCount = row.mismatch_count ?? row.unmatched_count ?? row.unmatched_rows ?? 0;
              const matchedCount = row.matched_count ?? row.matched_rows ?? 0;
              const status = row.severity || row.status || (mismatchCount > 0 ? "WARNING" : "OK");

              return (
                <tr key={`${row.id || "run"}-${index}`} className={rowTone(status)}>
                  <td data-label="Run ID" className="dashboard-cell-strong">{formatValue(row.id)}</td>
                  <td data-label="Run Time" className="dashboard-cell-muted">{formatTimestamp(row.run_time || row.run_started_at)}</td>
                  <td data-label="Status">
                    <span className={getStatusClass(status)}>{formatValue(status)}</span>
                  </td>
                  <td data-label="Mismatch Count">{formatNumber(mismatchCount, 0)}</td>
                  <td data-label="Matched Count">{formatNumber(matchedCount, 0)}</td>
                  <td data-label="Unmatched Count">{formatNumber(row.unmatched_count ?? row.unmatched_rows, 0)}</td>
                  <td data-label="Notes">{formatValue(row.notes)}</td>
                  <td data-label="Created At" className="dashboard-cell-muted">
                    {formatTimestamp(row.created_at || row.run_completed_at)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </div>
  );
}
