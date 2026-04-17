import InsightCard from "../InsightCard";

export default function HealthOverviewSection({
  lastUpdated,
  sectionLoading,
  sectionErrors,
  ibkrOpenCount,
  mismatch,
  mismatchLabel,
  backendHealthStatus,
  syncHealthStatus,
  reconciliationHealthStatus,
  lastReconciliationAt,
  isRunningSync,
  ibkrStatus,
  panelFreshnessLabel,
  panelFreshnessTone,
  onRetry,
  isRetrying = false,
}) {
  const ibkrState = String(ibkrStatus?.state || "UNKNOWN").toUpperCase();
  const ibkrStateColor =
    ibkrState === "READY"
      ? "#16a34a"
      : ibkrState === "LOGIN_REQUIRED" || ibkrState === "MARKET_DATA_UNAVAILABLE"
        ? "#f59e0b"
        : ibkrState === "DISABLED"
          ? "#64748b"
          : "#dc2626";

  return (
    <section className="dashboard-section">
      <div className="dashboard-panel">
        <div className="dashboard-panel-body">
          <div className="dashboard-panel-heading">
            <div>
              <h2 className="dashboard-panel-title">System Health</h2>
            </div>
            <div className="dashboard-panel-tools">
              <span className={`dashboard-badge ${panelFreshnessTone || "dashboard-badge-neutral"}`}>
                {panelFreshnessLabel || "No freshness data"}
              </span>
              <button
                type="button"
                className="dashboard-icon-button"
                onClick={onRetry}
                disabled={isRetrying}
                title="Retry panel"
                aria-label="Retry panel"
              >
                {isRetrying ? "…" : "↻"}
              </button>
            </div>
          </div>

          {lastUpdated && (
            <div className="dashboard-inline-meta" style={{ marginTop: 0, marginBottom: 8 }}>
              <span className="dashboard-pill">Last updated: {new Date(lastUpdated).toLocaleString()}</span>
            </div>
          )}
          {sectionLoading.reconciliation && <div className="dashboard-empty">Loading system health...</div>}
          {sectionErrors.reconciliation && <div className="dashboard-error">{sectionErrors.reconciliation}</div>}

          <div className="dashboard-metrics-grid">
            <InsightCard title="Open Positions (IBKR)" value={ibkrOpenCount ?? "-"} />
            <InsightCard
              title="Mismatch"
              value={mismatch !== null ? `${mismatch} (${mismatchLabel})` : "-"}
              valueColor={
                mismatchLabel === "OK"
                  ? "#16a34a"
                  : mismatchLabel === "WARNING"
                    ? "#f59e0b"
                    : mismatchLabel === "CRITICAL"
                      ? "#dc2626"
                      : undefined
              }
            />
            <InsightCard
              title="Backend Health"
              value={backendHealthStatus}
              valueColor={
                backendHealthStatus === "OK"
                  ? "#16a34a"
                  : backendHealthStatus === "WARNING"
                    ? "#f59e0b"
                    : "#dc2626"
              }
            />
            <InsightCard
              title="Sync Health"
              value={syncHealthStatus}
              valueColor={
                syncHealthStatus === "HEALTHY"
                  ? "#16a34a"
                  : syncHealthStatus === "RUNNING"
                    ? "#2563eb"
                    : "#dc2626"
              }
            />
            <InsightCard
              title="Reconciliation Health"
              value={reconciliationHealthStatus}
              valueColor={
                reconciliationHealthStatus === "OK"
                  ? "#16a34a"
                  : reconciliationHealthStatus === "WARNING"
                    ? "#f59e0b"
                    : reconciliationHealthStatus === "CRITICAL" || reconciliationHealthStatus === "FAILED"
                      ? "#dc2626"
                      : undefined
              }
            />
            <InsightCard title="IBKR Status" value={ibkrState} valueColor={ibkrStateColor} />
          </div>

          <div className="dashboard-inline-meta">
            <span className="dashboard-pill">
              Last Reconciliation: {lastReconciliationAt ? new Date(lastReconciliationAt).toLocaleString() : "-"}
            </span>
            <span className="dashboard-pill">Sync Action State: {isRunningSync ? "In progress" : "Idle"}</span>
            {ibkrStatus?.enabled && (
              <span className="dashboard-pill">
                IBKR Login: {ibkrStatus?.login_required ? "Needed" : ibkrState === "READY" ? "Ready" : ibkrState}
              </span>
            )}
          </div>
        </div>
      </div>
    </section>
  );
}
