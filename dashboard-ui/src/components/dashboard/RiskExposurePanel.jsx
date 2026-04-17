import InsightCard from "../InsightCard";

export default function RiskExposurePanel({
  sectionLoading,
  sectionErrors,
  riskExposureSummary,
  panelFreshnessLabel,
  panelFreshnessTone,
  onRetry,
  isRetrying = false,
}) {
  const dailyPnlPercent =
    riskExposureSummary && (riskExposureSummary.account_size ?? 0) > 0
      ? (((riskExposureSummary.daily_realized_pnl ?? 0) + (riskExposureSummary.daily_unrealized_pnl ?? 0)) /
          riskExposureSummary.account_size) *
        100
      : null;

  const tradingBlocked =
    riskExposureSummary && (riskExposureSummary.account_size ?? 0) > 0
      ? (((riskExposureSummary.daily_realized_pnl ?? 0) + (riskExposureSummary.daily_unrealized_pnl ?? 0)) /
          riskExposureSummary.account_size) <= -0.02
      : null;

  return (
    <section className="dashboard-section">
      <div className="dashboard-panel">
        <div className="dashboard-panel-body">
          <div className="dashboard-panel-heading">
            <div>
              <h2 className="dashboard-panel-title">Risk Exposure</h2>
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

          {sectionLoading.risk && <div className="dashboard-empty">Loading risk exposure...</div>}
          {sectionErrors.risk && <div className="dashboard-error">{sectionErrors.risk}</div>}

          <div className="dashboard-metrics-grid">
            <InsightCard title="Open Exposure ($)" value={riskExposureSummary?.total_open_exposure ?? "-"} />
            <InsightCard title="Allocation Used (%)" value={riskExposureSummary?.allocation_used_pct ?? "-"} />
            <InsightCard
              title="Open Positions"
              value={
                riskExposureSummary
                  ? `${riskExposureSummary.open_position_count ?? 0} / ${
                      riskExposureSummary.position_limit_enforced
                        ? riskExposureSummary.max_positions ?? 0
                        : "Unlimited"
                    }`
                  : "-"
              }
            />
            <InsightCard
              title="Unrealized PnL"
              value={riskExposureSummary?.daily_unrealized_pnl ?? "-"}
              valueColor={
                (riskExposureSummary?.daily_unrealized_pnl ?? 0) > 0
                  ? "#16a34a"
                  : (riskExposureSummary?.daily_unrealized_pnl ?? 0) < 0
                    ? "#dc2626"
                    : undefined
              }
            />
            <InsightCard
              title="Daily PnL %"
              value={dailyPnlPercent !== null ? dailyPnlPercent.toFixed(2) : "-"}
              valueColor={
                dailyPnlPercent !== null ? (dailyPnlPercent < 0 ? "#dc2626" : "#16a34a") : undefined
              }
            />
            <InsightCard
              title="Trading Status"
              value={tradingBlocked === null ? "-" : tradingBlocked ? "BLOCKED" : "ALLOWED"}
              valueColor={tradingBlocked === null ? undefined : tradingBlocked ? "#dc2626" : "#16a34a"}
            />
          </div>

          {riskExposureSummary && (
            <div className="dashboard-inline-meta">
              <span className="dashboard-pill">
                Max Allocated Capital: {riskExposureSummary.max_total_allocated_capital ?? "-"}
              </span>
              <span className="dashboard-pill">Account Size: {riskExposureSummary.account_size ?? "-"}</span>
              <span className="dashboard-pill">
                Max Allocation Pct: {riskExposureSummary.max_capital_allocation_pct ?? "-"}
              </span>
            </div>
          )}
        </div>
      </div>
    </section>
  );
}
