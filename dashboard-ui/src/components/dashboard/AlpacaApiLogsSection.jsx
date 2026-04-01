import InsightCard from "../InsightCard";
import { sortRowsByLatest } from "../tableFormatters";

export default function AlpacaApiLogsSection({
  sectionLoading,
  sectionErrors,
  alpacaApiLogs,
  alpacaApiErrors,
}) {
  const sortedApiErrors = sortRowsByLatest(alpacaApiErrors, ["logged_at", "created_at"]);
  const sortedApiLogs = sortRowsByLatest(alpacaApiLogs, ["logged_at", "created_at"]);

  return (
    <section className="dashboard-section">
      <div className="dashboard-panel">
        <div className="dashboard-panel-body">
          <div className="dashboard-panel-heading">
            <div>
              <h2 className="dashboard-panel-title">Alpaca API Logs</h2>
              <p className="dashboard-panel-subtitle">
                Broker-call telemetry for success rate, recent failures, and response health during live operations.
              </p>
            </div>
          </div>
      {sectionLoading.alpacaLogs && (
            <div className="dashboard-empty">Loading Alpaca API logs...</div>
      )}
      {sectionErrors.alpacaLogs && (
            <div className="dashboard-error">{sectionErrors.alpacaLogs}</div>
      )}
          <div className="dashboard-metrics-grid">
          <InsightCard title="Recent Calls" value={alpacaApiLogs.length} />
          <InsightCard
            title="Recent Errors"
            value={sortedApiErrors.length}
            valueColor={sortedApiErrors.length > 0 ? "#dc2626" : "#16a34a"}
          />
          <InsightCard
            title="Last Error At"
            value={sortedApiErrors[0]?.logged_at || "-"}
            valueColor={sortedApiErrors.length > 0 ? "#dc2626" : undefined}
          />
          <InsightCard
            title="Success Rate"
            value={
              sortedApiLogs.length > 0
                ? `${(((sortedApiLogs.filter((row) => !!row.success).length / sortedApiLogs.length) * 100).toFixed(1))}%`
                : "-"
            }
            valueColor={
              sortedApiLogs.length > 0 && sortedApiLogs.filter((row) => !!row.success).length === sortedApiLogs.length
                ? "#16a34a"
                : sortedApiLogs.length > 0
                  ? "#f59e0b"
                  : undefined
            }
          />
          </div>

          <div className="dashboard-section">
            <div className="dashboard-panel-heading">
              <div>
                <h3>Recent Errors</h3>
              </div>
            </div>
            {sortedApiErrors.length === 0 ? (
              <div className="dashboard-empty">No recent Alpaca API errors</div>
            ) : (
              <div className="dashboard-table-wrap">
                <table className="dashboard-table">
                <thead>
                  <tr>
                    <th>Logged At</th>
                    <th>Method</th>
                    <th>URL</th>
                    <th>Status</th>
                    <th>Severity</th>
                    <th>Error</th>
                  </tr>
                </thead>
                <tbody>
                  {sortedApiErrors.map((row, index) => (
                    <tr key={`${row.id || "alpaca-error"}-${index}`}>
                      <td>{row.logged_at || "-"}</td>
                      <td>{row.method || "-"}</td>
                      <td>{row.url || "-"}</td>
                      <td>{row.status_code ?? "-"}</td>
                      <td style={{ color: "#b4412f", fontWeight: 700 }}>ERROR</td>
                      <td style={{ color: "#b4412f" }}>{row.error_message || "-"}</td>
                    </tr>
                  ))}
                </tbody>
                </table>
              </div>
            )}
          </div>

          <div className="dashboard-section">
            <div className="dashboard-panel-heading">
              <div>
                <h3>Recent Calls</h3>
              </div>
            </div>
            {sortedApiLogs.length === 0 ? (
              <div className="dashboard-empty">No recent Alpaca API logs available</div>
            ) : (
              <div className="dashboard-table-wrap">
                <table className="dashboard-table">
                <thead>
                  <tr>
                    <th>Logged At</th>
                    <th>Method</th>
                    <th>URL</th>
                    <th>Status</th>
                    <th>Success</th>
                    <th>Duration (ms)</th>
                  </tr>
                </thead>
                <tbody>
                  {sortedApiLogs.map((row, index) => (
                    <tr key={`${row.id || "alpaca-log"}-${index}`}>
                      <td>{row.logged_at || "-"}</td>
                      <td>{row.method || "-"}</td>
                      <td>{row.url || "-"}</td>
                      <td>{row.status_code ?? "-"}</td>
                      <td style={{ color: row.success ? "#167a4b" : "#b4412f", fontWeight: 700 }}>
                        {row.success ? "Yes" : "No"}
                      </td>
                      <td>{row.duration_ms ?? "-"}</td>
                    </tr>
                  ))}
                </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      </div>
    </section>
  );
}
