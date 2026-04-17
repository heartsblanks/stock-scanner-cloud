import InsightCard from "../InsightCard";

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
  return `${normalizedHour}:00 ${suffix} ET`;
}

export default function ExecutionInsightsSection({
  sectionLoading,
  sectionErrors,
  paperTradePlacementRate,
  stageCounts,
  topAttemptReasons,
  paperTradeAttemptRejections,
  paperTradeAttemptDailySummary,
  paperTradeAttemptHourlySummary,
  ibkrRecentAttempts,
  ibkrStatus,
  hourlyOutcomeQuality,
  externalExitSummary,
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
  const busiestHour = [...(paperTradeAttemptHourlySummary || [])].sort(
    (left, right) => Number(right.total_attempts || 0) - Number(left.total_attempts || 0)
  )[0] || null;
  const strongestConversionHour = [...(paperTradeAttemptHourlySummary || [])]
    .filter((row) => Number(row.resolved_attempts || 0) > 0)
    .sort((left, right) => Number(right.placement_rate || 0) - Number(left.placement_rate || 0))[0] || null;
  const strongestOutcomeHour = [...(hourlyOutcomeQuality || [])]
    .filter((row) => Number(row.trade_count || 0) > 0)
    .sort((left, right) => Number(right.realized_pnl_total || 0) - Number(left.realized_pnl_total || 0))[0] || null;
  const mergedHourSignals = (paperTradeAttemptHourlySummary || [])
    .map((attemptRow) => {
      const matchingOutcome = (hourlyOutcomeQuality || []).find(
        (outcomeRow) => Number(outcomeRow.entry_hour_ny) === Number(attemptRow.hour_ny)
      );
      return {
        hour_ny: attemptRow.hour_ny,
        placement_rate: Number(attemptRow.placement_rate || 0),
        realized_pnl_total: Number(matchingOutcome?.realized_pnl_total || 0),
        top_non_placement_reason: attemptRow.top_non_placement_reason || "",
      };
    })
    .filter((row) => row.hour_ny !== null && row.hour_ny !== undefined);
  const widestMismatchHour = [...mergedHourSignals].sort((left, right) => {
    const leftMismatch = Math.abs(left.placement_rate) + (left.realized_pnl_total < 0 ? 100 : 0);
    const rightMismatch = Math.abs(right.placement_rate) + (right.realized_pnl_total < 0 ? 100 : 0);
    return rightMismatch - leftMismatch;
  })[0] || null;
  const latestIbkrAttempt = ibkrRecentAttempts?.[0] || null;
  const ibkrState = String(ibkrStatus?.state || "UNKNOWN").toUpperCase();

  function renderAttemptLine(label, attempt) {
    return (
      <div className="dashboard-inline-kv">
        <div><strong>{label} Symbol:</strong> {attempt?.symbol || "-"}</div>
        <div><strong>Stage:</strong> {attempt?.decision_stage || "-"}</div>
        <div><strong>Reason:</strong> {attempt?.final_reason || "-"}</div>
        <div><strong>Time:</strong> {attempt?.timestamp_utc ? new Date(attempt.timestamp_utc).toLocaleString() : "-"}</div>
      </div>
    );
  }

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
            <InsightCard
              title="External Exits"
              value={
                externalExitSummary
                  ? `${externalExitSummary.trade_count || 0} (${Number(externalExitSummary.realized_pnl_total || 0) < 0 ? "-" : ""}$${Math.abs(
                      Number(externalExitSummary.realized_pnl_total || 0)
                    ).toFixed(2)})`
                  : "0"
              }
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

          <div className="dashboard-panel dashboard-panel-strong">
            <div className="dashboard-panel-body dashboard-panel-body-tight">
              <div className="dashboard-panel-heading">
                <div>
                  <h3>Broker Status</h3>
                  <p className="dashboard-panel-subtitle">
                    Quick check that IBKR is active and whether it needs a manual login.
                  </p>
                </div>
              </div>

              {sectionErrors.ibkr && <div className="dashboard-error">{sectionErrors.ibkr}</div>}

              <div className="dashboard-metrics-grid">
                <InsightCard title="IBKR Readiness" value={ibkrState} valueColor={ibkrState === "READY" ? "#16a34a" : ibkrState === "LOGIN_REQUIRED" ? "#f59e0b" : "#dc2626"} />
                <InsightCard title="Latest IBKR Attempt" value={latestIbkrAttempt?.symbol || "-"} />
                <InsightCard title="IBKR Market Bars" value={ibkrStatus?.market_data_count ?? "-"} />
                <InsightCard title="Live Positions" value={ibkrStatus?.position_count ?? "-"} />
              </div>

              <div className="dashboard-panel dashboard-panel-strong">
                <div className="dashboard-panel-body dashboard-panel-body-tight">
                  <div className="dashboard-panel-heading">
                    <div><h3>IBKR Track</h3></div>
                  </div>
                  {latestIbkrAttempt ? renderAttemptLine("Latest", latestIbkrAttempt) : <div className="dashboard-empty">No recent IBKR attempts loaded.</div>}
                  <div className="dashboard-inline-meta">
                    <span className="dashboard-pill">Login Required: {ibkrStatus?.login_required ? "Yes" : "No"}</span>
                    <span className="dashboard-pill">Account: {ibkrStatus?.account_id || "-"}</span>
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

          <div className="dashboard-panel dashboard-panel-strong">
            <div className="dashboard-panel-body dashboard-panel-body-tight">
              <div className="dashboard-panel-heading">
                <div>
                  <h3>Hourly Outcome Pattern</h3>
                  <p className="dashboard-panel-subtitle">
                    Placement rate and dominant non-placement reason by New York session hour.
                  </p>
                </div>
              </div>

              {busiestHour && (
                <div className="dashboard-inline-kv">
                  <div><strong>Busiest Hour:</strong> {formatHourLabel(busiestHour.hour_ny)}</div>
                  <div><strong>Total Attempts:</strong> {busiestHour.total_attempts ?? 0}</div>
                  <div><strong>Placement Rate:</strong> {formatPercent(busiestHour.placement_rate)}</div>
                  <div><strong>Top Reason:</strong> {busiestHour.top_non_placement_reason || "-"}</div>
                </div>
              )}

              {paperTradeAttemptHourlySummary?.length ? (
                <div className="dashboard-table-wrap">
                  <table className="dashboard-table">
                    <thead>
                      <tr>
                        <th>Hour</th>
                        <th>Placed</th>
                        <th>Non-Placed</th>
                        <th>Placement Rate</th>
                        <th>Top Reason</th>
                      </tr>
                    </thead>
                    <tbody>
                      {paperTradeAttemptHourlySummary.map((row) => {
                        const nonPlacedCount =
                          Number(row.scan_rejected_count || 0) +
                          Number(row.refresh_rejected_count || 0) +
                          Number(row.placement_skipped_count || 0) +
                          Number(row.placement_rejected_count || 0);
                        return (
                          <tr key={row.hour_ny}>
                            <td data-label="Hour">{formatHourLabel(row.hour_ny)}</td>
                            <td data-label="Placed">{row.placed_count ?? 0}</td>
                            <td data-label="Non-Placed">{nonPlacedCount}</td>
                            <td data-label="Placement Rate">{formatPercent(row.placement_rate)}</td>
                            <td data-label="Top Reason">
                              {row.top_non_placement_reason
                                ? `${row.top_non_placement_reason} (${row.top_non_placement_reason_count || 0})`
                                : "-"}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="dashboard-empty">No hourly attempt rows are available yet.</div>
              )}
            </div>
          </div>

          <div className="dashboard-panel dashboard-panel-strong">
            <div className="dashboard-panel-body dashboard-panel-body-tight">
              <div className="dashboard-panel-heading">
                <div>
                  <h3>Conversion vs Quality</h3>
                  <p className="dashboard-panel-subtitle">
                    Compare where the system converts candidates well against where strategy-managed realized trade outcomes are actually strongest.
                  </p>
                </div>
              </div>

              <div className="dashboard-inline-kv">
                <div>
                  <strong>Best Conversion Hour:</strong>{" "}
                  {strongestConversionHour ? formatHourLabel(strongestConversionHour.hour_ny) : "-"}
                </div>
                <div>
                  <strong>Placement Rate:</strong>{" "}
                  {strongestConversionHour ? formatPercent(strongestConversionHour.placement_rate) : "-"}
                </div>
                <div>
                  <strong>Best Outcome Hour:</strong>{" "}
                  {strongestOutcomeHour ? formatHourLabel(strongestOutcomeHour.entry_hour_ny) : "-"}
                </div>
                <div>
                  <strong>Realized P&amp;L:</strong>{" "}
                  {strongestOutcomeHour ? `$${Number(strongestOutcomeHour.realized_pnl_total || 0).toFixed(2)}` : "-"}
                </div>
              </div>

              {widestMismatchHour ? (
                <div className="dashboard-inline-meta">
                  <span className="dashboard-pill">
                    Watch Hour: {formatHourLabel(widestMismatchHour.hour_ny)}
                  </span>
                  <span className="dashboard-pill">
                    Conversion: {formatPercent(widestMismatchHour.placement_rate)}
                  </span>
                  <span className="dashboard-pill">
                    Realized P&amp;L: ${Number(widestMismatchHour.realized_pnl_total || 0).toFixed(2)}
                  </span>
                  <span className="dashboard-pill">
                    Top Friction: {widestMismatchHour.top_non_placement_reason || "-"}
                  </span>
                </div>
              ) : (
                <div className="dashboard-empty">Combined conversion and quality comparison will appear once both datasets have rows.</div>
              )}
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
