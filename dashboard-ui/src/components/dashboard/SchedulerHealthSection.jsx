import InsightCard from "../InsightCard";

function parseTimestamp(value) {
  if (!value) {
    return null;
  }

  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function formatRelativeAge(value) {
  const parsed = parseTimestamp(value);
  if (!parsed) {
    return "-";
  }

  const ageMs = Date.now() - parsed.getTime();
  const ageMinutes = Math.max(0, Math.round(ageMs / 60000));
  if (ageMinutes < 1) {
    return "just now";
  }
  if (ageMinutes < 60) {
    return `${ageMinutes}m ago`;
  }

  const ageHours = Math.round((ageMinutes / 60) * 10) / 10;
  if (ageHours < 24) {
    return `${ageHours}h ago`;
  }

  const ageDays = Math.round((ageHours / 24) * 10) / 10;
  return `${ageDays}d ago`;
}

function getFreshnessTone(value, { okMinutes, warnMinutes }) {
  const parsed = parseTimestamp(value);
  if (!parsed) {
    return { label: "No Data", color: "#dc2626" };
  }

  const ageMinutes = Math.max(0, Math.round((Date.now() - parsed.getTime()) / 60000));
  if (ageMinutes <= okMinutes) {
    return { label: "Fresh", color: "#16a34a" };
  }
  if (ageMinutes <= warnMinutes) {
    return { label: "Aging", color: "#f59e0b" };
  }
  return { label: "Stale", color: "#dc2626" };
}

export default function SchedulerHealthSection({
  opsSummary,
  ibkrRecentAttempts,
  ibkrStatus,
}) {
  const latestScanRun = opsSummary?.latest_scan_run || null;
  const latestReconciliationAt = opsSummary?.latest_reconciliation_run_time || null;
  const latestIbkrAttemptAt = ibkrRecentAttempts?.[0]?.timestamp_utc || null;

  const scanTone = getFreshnessTone(latestScanRun?.scan_time, { okMinutes: 45, warnMinutes: 120 });
  const reconciliationTone = getFreshnessTone(latestReconciliationAt, { okMinutes: 18 * 60, warnMinutes: 36 * 60 });
  const ibkrAttemptTone = getFreshnessTone(latestIbkrAttemptAt, { okMinutes: 45, warnMinutes: 120 });

  const ibkrState = String(ibkrStatus?.state || "UNKNOWN").toUpperCase();
  const ibkrReadyTone =
    ibkrState === "READY"
      ? { label: "Ready", color: "#16a34a" }
      : ibkrState === "LOGIN_REQUIRED" || ibkrState === "MARKET_DATA_UNAVAILABLE"
        ? { label: ibkrState, color: "#f59e0b" }
        : ibkrState === "DISABLED"
          ? { label: "Disabled", color: "#64748b" }
          : { label: ibkrState, color: "#dc2626" };

  return (
    <section className="dashboard-section">
      <div className="dashboard-panel">
        <div className="dashboard-panel-body dashboard-panel-body-tight">
          <div className="dashboard-panel-heading">
            <div>
              <h2 className="dashboard-panel-title">Scheduler Watch</h2>
              <p className="dashboard-panel-subtitle">
                Freshness view for intraday scans, post-close reconciliation, broker activity, and IBKR readiness.
              </p>
            </div>
          </div>

          <div className="dashboard-metrics-grid">
            <InsightCard
              title="Market Ops"
              value={scanTone.label}
              valueColor={scanTone.color}
            />
            <InsightCard
              title="Post-Close"
              value={reconciliationTone.label}
              valueColor={reconciliationTone.color}
            />
            <InsightCard
              title="IBKR Activity"
              value={ibkrAttemptTone.label}
              valueColor={ibkrAttemptTone.color}
            />
            <InsightCard
              title="IBKR Ready"
              value={ibkrReadyTone.label}
              valueColor={ibkrReadyTone.color}
            />
          </div>

          <div className="dashboard-inline-meta">
            <span className="dashboard-pill">
              Last Scan: {latestScanRun?.scan_time ? `${new Date(latestScanRun.scan_time).toLocaleString()} (${formatRelativeAge(latestScanRun.scan_time)})` : "-"}
            </span>
            <span className="dashboard-pill">
              Last Reconcile: {latestReconciliationAt ? `${new Date(latestReconciliationAt).toLocaleString()} (${formatRelativeAge(latestReconciliationAt)})` : "-"}
            </span>
            <span className="dashboard-pill">
              Latest IBKR Attempt: {latestIbkrAttemptAt ? formatRelativeAge(latestIbkrAttemptAt) : "-"}
            </span>
            {latestScanRun?.mode && (
              <span className="dashboard-pill">
                Latest Scan Mode: {latestScanRun.mode}
              </span>
            )}
            {latestScanRun?.scan_source && (
              <span className="dashboard-pill">
                Source: {latestScanRun.scan_source}
              </span>
            )}
          </div>
        </div>
      </div>
    </section>
  );
}
