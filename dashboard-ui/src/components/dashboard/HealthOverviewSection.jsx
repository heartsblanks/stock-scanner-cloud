import InsightCard from "../InsightCard";

export default function HealthOverviewSection({
  lastUpdated,
  sectionLoading,
  sectionErrors,
  openTrades,
  alpacaOpenCount,
  mismatch,
  mismatchLabel,
  backendHealthStatus,
  syncHealthStatus,
  reconciliationHealthStatus,
  lastReconciliationAt,
  alpacaApiErrors,
  isRunningSync,
  riskExposureSummary,
  confidenceMultiplier,
  lossMultiplier,
  finalSizingMultiplier,
  multiplierStatus,
  compact = false,
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
    <div className="dashboard-section dashboard-stack">
      <div className="dashboard-panel">
        <div className="dashboard-panel-body">
          <div className="dashboard-panel-heading">
            <div>
              <h2 className="dashboard-panel-title">System Health</h2>
              <p className="dashboard-panel-subtitle">
                Broker sync, reconciliation drift, and stack health signals presented together so issues surface fast.
              </p>
            </div>
          </div>
        {lastUpdated && (
            <div className="dashboard-inline-meta" style={{ marginTop: 0, marginBottom: 8 }}>
              <span className="dashboard-pill">
            Last updated: {new Date(lastUpdated).toLocaleString()}
              </span>
            </div>
        )}
        {sectionLoading.reconciliation && (
            <div className="dashboard-empty">Loading system health...</div>
        )}
        {sectionErrors.reconciliation && (
            <div className="dashboard-error">{sectionErrors.reconciliation}</div>
        )}
          <div className="dashboard-metrics-grid">
          <InsightCard title="Open Trades (DB)" value={openTrades.length} />
          <InsightCard title="Open Positions (Alpaca)" value={alpacaOpenCount ?? "-"} />
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
          </div>
        {mismatch !== null && (
            <div className="dashboard-inline-meta">
              <span className="dashboard-pill">
              DB Open Trades: {openTrades.length} | Alpaca Open Positions: {alpacaOpenCount ?? "-"}
              </span>
            </div>
        )}
          <div className="dashboard-inline-meta">
            <span className="dashboard-pill">
              Last Reconciliation: {lastReconciliationAt ? new Date(lastReconciliationAt).toLocaleString() : "-"}
            </span>
            <span className="dashboard-pill">Recent Alpaca Errors: {alpacaApiErrors.length}</span>
            <span className="dashboard-pill">Sync Action State: {isRunningSync ? "In progress" : "Idle"}</span>
          </div>
        </div>
      </div>

      <div className="dashboard-split">
        <div className="dashboard-panel">
          <div className="dashboard-panel-body">
            <div className="dashboard-panel-heading">
              <div>
                <h2 className="dashboard-panel-title">Risk Exposure</h2>
                <p className="dashboard-panel-subtitle">
                  Capital usage, open exposure, and account sizing posture for the active book.
                </p>
              </div>
            </div>
        {sectionLoading.risk && (
              <div className="dashboard-empty">Loading risk exposure...</div>
        )}
        {sectionErrors.risk && (
              <div className="dashboard-error">{sectionErrors.risk}</div>
        )}
            <div className="dashboard-metrics-grid">
          <InsightCard title="Open Exposure ($)" value={riskExposureSummary?.total_open_exposure ?? "-"} />
          <InsightCard title="Allocation Used (%)" value={riskExposureSummary?.allocation_used_pct ?? "-"} />
          <InsightCard
            title="Open Positions"
            value={
              riskExposureSummary
                ? `${riskExposureSummary.open_position_count ?? 0} / ${
                    riskExposureSummary.position_limit_enforced
                      ? (riskExposureSummary.max_positions ?? 0)
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

        {!compact && (
          <div className="dashboard-panel">
            <div className="dashboard-panel-body">
              <div className="dashboard-panel-heading">
                <div>
                  <h2 className="dashboard-panel-title">Daily Risk Guardrail</h2>
                  <p className="dashboard-panel-subtitle">
                    Daily stop logic using realized plus unrealized P&amp;L against live account size.
                  </p>
                </div>
              </div>
          {sectionLoading.risk && (
                <div className="dashboard-empty">Loading guardrail data...</div>
          )}
          {sectionErrors.risk && (
                <div className="dashboard-error">{sectionErrors.risk}</div>
          )}
              <div className="dashboard-metrics-grid">
            <InsightCard
              title="Daily Realized PnL"
              value={riskExposureSummary?.daily_realized_pnl ?? "-"}
              valueColor={
                (riskExposureSummary?.daily_realized_pnl ?? 0) > 0
                  ? "#16a34a"
                  : (riskExposureSummary?.daily_realized_pnl ?? 0) < 0
                    ? "#dc2626"
                    : undefined
              }
            />
            <InsightCard
              title="Daily Unrealized PnL"
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
                    Daily loss cutoff: -2.00%
                  </span>
                  <span className="dashboard-pill">
                    Guardrail evaluates realized + unrealized PnL against account size.
                  </span>
                </div>
          )}
            </div>
          </div>
        )}
      </div>

      {!compact && (
        <div className="dashboard-panel">
          <div className="dashboard-panel-body">
            <div className="dashboard-panel-heading">
              <div>
                <h2 className="dashboard-panel-title">Sizing Multipliers</h2>
                <p className="dashboard-panel-subtitle">
                  The latest confidence and loss controls that modulate per-trade notional before placement.
                </p>
              </div>
            </div>
          {sectionLoading.sizing && (
              <div className="dashboard-empty">Loading sizing multipliers...</div>
          )}
          {sectionErrors.sizing && (
              <div className="dashboard-error">{sectionErrors.sizing}</div>
          )}
            <div className="dashboard-metrics-grid">
            <InsightCard
              title="Confidence Multiplier"
              value={confidenceMultiplier ?? "-"}
              valueColor={confidenceMultiplier !== null ? "#2563eb" : undefined}
            />
            <InsightCard
              title="Loss Multiplier"
              value={lossMultiplier ?? "-"}
              valueColor={lossMultiplier !== null ? "#7c3aed" : undefined}
            />
            <InsightCard
              title="Final Sizing Multiplier"
              value={finalSizingMultiplier ?? "-"}
              valueColor={finalSizingMultiplier !== null ? "#16a34a" : undefined}
            />
            <InsightCard
              title="Exposure Status"
              value={multiplierStatus}
              valueColor={multiplierStatus === "EXPOSED" ? "#16a34a" : "#f59e0b"}
            />
            </div>
            <div className="dashboard-inline-meta">
              <span className="dashboard-pill">
            Values are read from the latest scan summary when exposed by the backend. If shown as `-`, backend payload support is still incomplete.
              </span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
