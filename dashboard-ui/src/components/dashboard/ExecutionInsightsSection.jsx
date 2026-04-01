import InsightCard from "../InsightCard";

function formatPercent(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  return `${Number(value).toFixed(1)}%`;
}

export default function ExecutionInsightsSection({
  sectionLoading,
  sectionErrors,
  paperTradePlacementRate,
  stageCounts,
  topAttemptReasons,
  paperTradeAttemptRejections,
  paperTradeAttemptDailySummary,
}) {
  const stageCountMap = Object.fromEntries((stageCounts || []).map((row) => [row.decision_stage, Number(row.count || 0)]));
  const placedCount = stageCountMap.PLACED ?? 0;
  const skippedCount =
    (stageCountMap.PLACEMENT_SKIPPED ?? 0) +
    (stageCountMap.PLACEMENT_REJECTED ?? 0) +
    (stageCountMap.REFRESH_REJECTED ?? 0) +
    (stageCountMap.SCAN_REJECTED ?? 0);
  const latestRejection = paperTradeAttemptRejections?.[0] || null;
  const mostRecentDay = paperTradeAttemptDailySummary?.[0]?.trade_date || null;
  const mostCommonReason = topAttemptReasons?.[0] || null;

  return (
    <section className="dashboard-section">
      <div className="dashboard-panel">
        <div className="dashboard-panel-body">
          <div className="dashboard-panel-heading">
            <div>
              <h2 className="dashboard-panel-title">Execution Insights</h2>
              <p className="dashboard-panel-subtitle">
                Why trades are being placed, skipped, or rejected, using the new per-attempt data instead of summary text.
              </p>
            </div>
          </div>

          {sectionLoading.attempts && <div className="dashboard-empty">Loading execution attempt analytics...</div>}
          {sectionErrors.attempts && <div className="dashboard-error">{sectionErrors.attempts}</div>}

          <div className="dashboard-metrics-grid">
            <InsightCard title="Placement Rate" value={formatPercent(paperTradePlacementRate)} />
            <InsightCard title="Placed Attempts" value={placedCount} valueColor={placedCount > 0 ? "#167a4b" : undefined} />
            <InsightCard title="Skipped / Rejected" value={skippedCount} valueColor={skippedCount > 0 ? "#b4412f" : undefined} />
            <InsightCard
              title="Top Reason"
              value={mostCommonReason ? `${mostCommonReason.final_reason} (${mostCommonReason.count})` : "-"}
            />
          </div>

          <div className="dashboard-split">
            <div className="dashboard-stack">
              <div className="dashboard-panel dashboard-panel-strong">
                <div className="dashboard-panel-body dashboard-panel-body-tight">
                  <div className="dashboard-panel-heading">
                    <div>
                      <h3>Recent Rejection</h3>
                    </div>
                  </div>
                  {latestRejection ? (
                    <div className="dashboard-inline-kv">
                      <div><strong>Symbol:</strong> {latestRejection.symbol || "-"}</div>
                      <div><strong>Stage:</strong> {latestRejection.decision_stage || "-"}</div>
                      <div><strong>Reason:</strong> {latestRejection.final_reason || "-"}</div>
                      <div><strong>Time:</strong> {latestRejection.timestamp_utc ? new Date(latestRejection.timestamp_utc).toLocaleString() : "-"}</div>
                    </div>
                  ) : (
                    <div className="dashboard-empty">No recent non-placement attempts recorded.</div>
                  )}
                </div>
              </div>
            </div>

            <div className="dashboard-stack">
              <div className="dashboard-panel dashboard-panel-strong">
                <div className="dashboard-panel-body dashboard-panel-body-tight">
                  <div className="dashboard-panel-heading">
                    <div>
                      <h3>Recent Activity Window</h3>
                    </div>
                  </div>
                  <div className="dashboard-inline-kv">
                    <div><strong>Most Recent Attempt Day:</strong> {mostRecentDay || "-"}</div>
                    <div><strong>Stage Categories Seen:</strong> {(stageCounts || []).length}</div>
                    <div><strong>Recent Rejections Loaded:</strong> {(paperTradeAttemptRejections || []).length}</div>
                    <div><strong>Daily Summary Rows:</strong> {(paperTradeAttemptDailySummary || []).length}</div>
                  </div>
                </div>
              </div>
            </div>
          </div>

          {!!topAttemptReasons?.length && (
            <div className="dashboard-inline-meta">
              {topAttemptReasons.slice(0, 4).map((reason) => (
                <span key={reason.final_reason} className="dashboard-pill">
                  {reason.final_reason}: {reason.count}
                </span>
              ))}
            </div>
          )}
        </div>
      </div>
    </section>
  );
}
