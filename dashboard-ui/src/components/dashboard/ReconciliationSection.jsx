import ReconciliationDetailsTable from "../ReconciliationDetailsTable";
import ReconciliationHistoryTable from "../ReconciliationHistoryTable";

export default function ReconciliationSection({
  sectionLoading,
  sectionErrors,
  lastUpdated,
  lastReconciliationStatus,
  lastReconciliationAt,
  reconciliationSummary,
  reconciliationSymbolFilter,
  setReconciliationSymbolFilter,
  reconciliationSymbols,
  filteredReconciliationDetails,
  reconciliationHistory,
}) {
  const statusTone =
    lastReconciliationStatus === "OK"
      ? "dashboard-pill-ok"
      : lastReconciliationStatus === "WARNING"
        ? "dashboard-pill-warn"
        : lastReconciliationStatus === "CRITICAL" || lastReconciliationStatus === "FAILED"
          ? "dashboard-pill-danger"
          : "dashboard-pill-info";

  return (
    <section className="dashboard-section">
      <div className="dashboard-panel">
        <div className="dashboard-panel-body">
          <div className="dashboard-panel-heading">
            <div>
              <h2 className="dashboard-panel-title">Reconciliation</h2>
              <p className="dashboard-panel-subtitle">
                Where local trade state and Alpaca reality diverge, and how those mismatches are evolving over time.
              </p>
            </div>
          </div>

          {sectionLoading.reconciliation && <div className="dashboard-empty">Loading reconciliation...</div>}
          {sectionErrors.reconciliation && <div className="dashboard-error">{sectionErrors.reconciliation}</div>}
          {lastUpdated && (
            <div className="dashboard-inline-meta" style={{ marginTop: 0 }}>
              <span className="dashboard-pill">Data refreshed at: {new Date(lastUpdated).toLocaleString()}</span>
            </div>
          )}

          <div className="dashboard-inline-meta">
            <div className={`dashboard-pill dashboard-pill-status ${statusTone}`}>
              Last Reconciliation: {lastReconciliationStatus || "-"}
            </div>
            <div className="dashboard-pill">
              {lastReconciliationAt
                ? `Last Reconciliation Time: ${new Date(lastReconciliationAt).toLocaleString()}`
                : "Last Reconciliation Time: -"}
            </div>
          </div>

          {reconciliationSummary ? (
            <div className="dashboard-stack">
              <div className="dashboard-metrics-grid">
                <div className="metric-card">
                  <div className="metric-card-label">Status</div>
                  <div className="metric-card-value">{reconciliationSummary.severity || "-"}</div>
                </div>
                <div className="metric-card">
                  <div className="metric-card-label">Total Mismatch</div>
                  <div className="metric-card-value">{reconciliationSummary.mismatch_count ?? "-"}</div>
                </div>
                <div className="metric-card">
                  <div className="metric-card-label">Missing In Alpaca</div>
                  <div className="metric-card-value">{reconciliationSummary.missing_in_alpaca ?? 0}</div>
                </div>
                <div className="metric-card">
                  <div className="metric-card-label">Missing In DB</div>
                  <div className="metric-card-value">{reconciliationSummary.missing_in_db ?? 0}</div>
                </div>
              </div>

              <div className="dashboard-inline-meta">
                <span className="dashboard-pill">
                  Exit Reason Mismatch: {reconciliationSummary.exit_reason_mismatch ?? 0}
                </span>
                <span className="dashboard-pill">
                  Entry Qty Mismatch: {reconciliationSummary.entry_qty_mismatch ?? 0}
                </span>
                <span className="dashboard-pill">
                  Exit Qty Mismatch: {reconciliationSummary.exit_qty_mismatch ?? 0}
                </span>
                <span className="dashboard-pill">
                  Unresolved Exit: {reconciliationSummary.exit_not_resolved ?? 0}
                </span>
              </div>

              <div className="dashboard-section">
                <div className="dashboard-panel-heading">
                  <div>
                    <h3>Reconciliation Details</h3>
                  </div>
                  <div className="dashboard-field" style={{ minWidth: 180 }}>
                    <label htmlFor="reconciliation-symbol-filter">Symbol</label>
                    <select
                      id="reconciliation-symbol-filter"
                      value={reconciliationSymbolFilter}
                      onChange={(e) => setReconciliationSymbolFilter(e.target.value)}
                      className="dashboard-select"
                    >
                      <option value="">All symbols</option>
                      {reconciliationSymbols.map((symbol) => (
                        <option key={symbol} value={symbol}>
                          {symbol}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>

                <div className="dashboard-inline-meta">
                  <span className="dashboard-pill">
                    Showing {filteredReconciliationDetails.length} detail row(s)
                    {reconciliationSymbolFilter ? ` for ${reconciliationSymbolFilter}` : " across all symbols"}
                  </span>
                </div>

                <ReconciliationDetailsTable rows={filteredReconciliationDetails} />
              </div>

              <div className="dashboard-section">
                <div className="dashboard-panel-heading">
                  <div>
                    <h3>Reconciliation History</h3>
                  </div>
                </div>
                <ReconciliationHistoryTable rows={reconciliationHistory || []} />
              </div>
            </div>
          ) : (
            <div className="dashboard-empty">Reconciliation data not yet loaded</div>
          )}
        </div>
      </div>
    </section>
  );
}
