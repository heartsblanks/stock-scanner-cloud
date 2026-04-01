import SummaryCards from "../components/SummaryCards";
import OpenTradesTable from "../components/OpenTradesTable";
import TradeLifecycleTable from "../components/TradeLifecycleTable";
import HourlyPerformanceChart from "../components/HourlyPerformanceChart";
import SymbolPerformanceChart from "../components/SymbolPerformanceChart";
import ModePerformanceChart from "../components/ModePerformanceChart";
import DashboardFilters from "../components/DashboardFilters";
import EquityCurveChart from "../components/EquityCurveChart";
import InsightCard from "../components/InsightCard";
import AlpacaApiLogsSection from "../components/dashboard/AlpacaApiLogsSection";
import HealthOverviewSection from "../components/dashboard/HealthOverviewSection";
import ReconciliationSection from "../components/dashboard/ReconciliationSection";
import { useDashboardData } from "../hooks/useDashboardData";
import { useEffect, useState } from "react";

const DASHBOARD_VIEWS = [
  { id: "overview", label: "Overview", description: "Best for the first look each session." },
  { id: "trades", label: "Trades", description: "Open positions and lifecycle details." },
  { id: "reconciliation", label: "Reconciliation", description: "Mismatch and repair workflow." },
  { id: "broker", label: "Broker Logs", description: "Alpaca call health and failures." },
  { id: "analytics", label: "Analytics", description: "Charts and performance patterns." },
];

function formatCurrency(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }

  return `$${Number(value).toFixed(2)}`;
}

export default function DashboardPage() {
  const [activeView, setActiveView] = useState("overview");
  const [theme, setTheme] = useState(() => {
    if (typeof window === "undefined") {
      return "light";
    }

    const savedTheme = window.localStorage.getItem("dashboard-theme");
    if (savedTheme === "light" || savedTheme === "dark") {
      return savedTheme;
    }

    return window.matchMedia?.("(prefers-color-scheme: dark)")?.matches ? "dark" : "light";
  });

  const {
    summary,
    openTrades,
    lifecycle,
    loading,
    error,
    sectionLoading,
    sectionErrors,
    reconciliationSummary,
    reconciliationSymbolFilter,
    setReconciliationSymbolFilter,
    reconciliationHistory,
    riskExposureSummary,
    alpacaApiLogs,
    alpacaApiErrors,
    lastUpdated,
    isRefreshing,
    isRunningSync,
    toast,
    lastReconciliationStatus,
    lastReconciliationAt,
    symbolPerformance,
    modePerformance,
    hourlyPerformance,
    equityCurve,
    insights,
    reconciliationSymbols,
    filteredReconciliationDetails,
    mismatch,
    mismatchLabel,
    backendHealthStatus,
    syncHealthStatus,
    reconciliationHealthStatus,
    confidenceMultiplier,
    lossMultiplier,
    finalSizingMultiplier,
    multiplierStatus,
    handleApplyFilters,
    refreshData,
    rerunReconciliation,
    syncPaperTrades,
    alpacaOpenCount,
  } = useDashboardData();

  const mismatchTone =
    mismatchLabel === "OK"
      ? "dashboard-pill-ok"
      : mismatchLabel === "WARNING"
        ? "dashboard-pill-warn"
        : mismatchLabel === "CRITICAL"
          ? "dashboard-pill-danger"
          : "dashboard-pill-info";

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    window.localStorage.setItem("dashboard-theme", theme);
  }, [theme]);

  if (loading) {
    return <div className="dashboard-loading">Loading dashboard...</div>;
  }

  if (error) {
    return <div className="dashboard-error">Error: {error}</div>;
  }

  return (
    <div className="dashboard-shell">
      <div className="dashboard-frame">
        <section className="dashboard-hero">
          <div className="dashboard-hero-grid">
            <div>
              <div className="dashboard-kicker">
                <span className="dashboard-kicker-dot" />
                Trading Operations Console
              </div>
              <h1 className="dashboard-title">See the trading day as one connected system.</h1>
              <p className="dashboard-subtitle">
                A sharper command view for scan quality, risk posture, reconciliation, broker health, and the state of
                every paper trade moving through the stack.
              </p>
              <div className="dashboard-hero-meta">
                <div className="dashboard-hero-stat">
                  <div className="dashboard-hero-stat-label">Realized P&amp;L</div>
                  <div className="dashboard-hero-stat-value">
                    {formatCurrency(summary?.summary?.realized_pnl_total)}
                  </div>
                </div>
                <div className="dashboard-hero-stat">
                  <div className="dashboard-hero-stat-label">Open vs Alpaca</div>
                  <div className="dashboard-hero-stat-value">
                    {openTrades.length} / {alpacaOpenCount ?? "-"}
                  </div>
                </div>
                <div className="dashboard-hero-stat">
                  <div className="dashboard-hero-stat-label">Reconciliation Drift</div>
                  <div className="dashboard-hero-stat-value">
                    {mismatch !== null ? `${mismatch} ${mismatchLabel ? `(${mismatchLabel})` : ""}` : "-"}
                  </div>
                </div>
              </div>
            </div>

            <div className="dashboard-hero-side">
              <div className="dashboard-callout">
                <div className="dashboard-callout-label">Operational Pulse</div>
                <div className="dashboard-callout-value">{backendHealthStatus || "UNKNOWN"}</div>
                <div className="dashboard-callout-note">
                  Last refresh: {lastUpdated ? new Date(lastUpdated).toLocaleString() : "Not available yet"}
                </div>
              </div>

              <div className="dashboard-toolbar">
                <button
                  onClick={refreshData}
                  disabled={isRefreshing}
                  className="dashboard-button dashboard-button-primary"
                >
                  {isRefreshing ? "Refreshing..." : "Refresh Data"}
                </button>
                <button
                  onClick={rerunReconciliation}
                  disabled={isRefreshing}
                  className="dashboard-button dashboard-button-secondary"
                >
                  Re-run Reconciliation
                </button>
                <button
                  onClick={syncPaperTrades}
                  disabled={isRefreshing || isRunningSync}
                  className="dashboard-button dashboard-button-neutral"
                >
                  {isRunningSync ? "Syncing Trades..." : "Sync Paper Trades"}
                </button>
                <button
                  onClick={() => setTheme((currentTheme) => (currentTheme === "dark" ? "light" : "dark"))}
                  className="dashboard-button dashboard-button-theme"
                >
                  {theme === "dark" ? "Switch to Light" : "Switch to Dark"}
                </button>
              </div>

              <div className="dashboard-inline-meta">
                <span className={`dashboard-pill dashboard-pill-status ${mismatchTone}`}>
                  Mismatch {mismatch !== null ? mismatch : "-"}
                </span>
                <span className="dashboard-pill">Sync {syncHealthStatus || "UNKNOWN"}</span>
                <span className="dashboard-pill">Recon {reconciliationHealthStatus || "UNKNOWN"}</span>
              </div>
            </div>
          </div>
        </section>

        {toast && (
          <div
            className={`dashboard-toast ${toast.type === "success" ? "dashboard-toast-success" : "dashboard-toast-error"}`}
          >
            {toast.message}
          </div>
        )}

        <div className="dashboard-grid">
          <DashboardFilters onApply={handleApplyFilters} />

          <div className="dashboard-banner">
            <div className="dashboard-banner-title">Operational Fix Actions</div>
            <div className="dashboard-banner-copy">
              Use these actions when dashboard state looks stale or inconsistent. Refresh reloads dashboard data,
              re-run reconciliation rebuilds mismatch visibility, and sync paper trades refreshes trade state from
              Alpaca.
            </div>
          </div>

          <section className="dashboard-view-nav-panel">
            <div className="dashboard-view-nav-copy">
              <div className="dashboard-banner-title">Focused Views</div>
              <div className="dashboard-banner-copy">
                Keep the landing screen light, then jump into the exact slice you need instead of scrolling through the
                whole operating system at once.
              </div>
            </div>
            <div className="dashboard-view-nav">
              {DASHBOARD_VIEWS.map((view) => (
                <button
                  key={view.id}
                  type="button"
                  onClick={() => setActiveView(view.id)}
                  className={`dashboard-view-tab ${activeView === view.id ? "dashboard-view-tab-active" : ""}`}
                >
                  <span className="dashboard-view-tab-label">{view.label}</span>
                  <span className="dashboard-view-tab-copy">{view.description}</span>
                </button>
              ))}
            </div>
          </section>

          {activeView === "overview" && (
            <>
              <SummaryCards data={summary} />
              {sectionLoading.overview && <div className="dashboard-empty">Loading overview...</div>}
              {sectionErrors.overview && <div className="dashboard-error">{sectionErrors.overview}</div>}

              <HealthOverviewSection
                lastUpdated={lastUpdated}
                sectionLoading={sectionLoading}
                sectionErrors={sectionErrors}
                openTrades={openTrades}
                alpacaOpenCount={alpacaOpenCount}
                mismatch={mismatch}
                mismatchLabel={mismatchLabel}
                backendHealthStatus={backendHealthStatus}
                syncHealthStatus={syncHealthStatus}
                reconciliationHealthStatus={reconciliationHealthStatus}
                lastReconciliationAt={lastReconciliationAt}
                alpacaApiErrors={alpacaApiErrors}
                isRunningSync={isRunningSync}
                riskExposureSummary={riskExposureSummary}
                confidenceMultiplier={confidenceMultiplier}
                lossMultiplier={lossMultiplier}
                finalSizingMultiplier={finalSizingMultiplier}
                multiplierStatus={multiplierStatus}
              />

              <section className="dashboard-section">
                <div className="dashboard-panel dashboard-panel-strong">
                  <div className="dashboard-panel-body">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">Performance Readouts</h2>
                        <p className="dashboard-panel-subtitle">
                          A quick read on where the system has been strongest by symbol, mode, exit pattern, and time of
                          day.
                        </p>
                      </div>
                    </div>
                    <div className="dashboard-metrics-grid">
                      <InsightCard title="Best Symbol" value={insights?.best_symbol?.symbol || "-"} />
                      <InsightCard title="Best Mode" value={insights?.best_mode?.mode || "-"} />
                      <InsightCard title="Most Common Exit" value={insights?.most_common_exit?.exit_reason || "-"} />
                      <InsightCard title="Best Hour (UTC)" value={insights?.best_hour?.entry_hour_utc ?? "-"} />
                    </div>
                  </div>
                </div>
              </section>

              <section className="dashboard-section">
                <div className="dashboard-panel">
                  <div className="dashboard-panel-body">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">Equity Curve</h2>
                        <p className="dashboard-panel-subtitle">
                          See the account arc over time instead of reading isolated trade outcomes.
                        </p>
                      </div>
                    </div>
                    <EquityCurveChart rows={equityCurve} />
                  </div>
                </div>
              </section>

              <section className="dashboard-section">
                <div className="dashboard-panel">
                  <div className="dashboard-panel-body">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">Overview Trends</h2>
                        <p className="dashboard-panel-subtitle">
                          A compact view of mode performance and the hours that are paying or fading.
                        </p>
                      </div>
                    </div>
                    <div className="dashboard-chart-grid">
                      <div>
                        <ModePerformanceChart rows={modePerformance} />
                      </div>
                      <div>
                        <HourlyPerformanceChart rows={hourlyPerformance} />
                      </div>
                    </div>
                  </div>
                </div>
              </section>
            </>
          )}

          {activeView === "trades" && (
            <>
              <section className="dashboard-section">
                <div className="dashboard-panel">
                  <div className="dashboard-panel-body">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">Open Trades</h2>
                        <p className="dashboard-panel-subtitle">
                          Live database positions with sizing and protective levels in one place.
                        </p>
                      </div>
                    </div>
                    <OpenTradesTable trades={openTrades} />
                  </div>
                </div>
              </section>

              <section className="dashboard-section">
                <div className="dashboard-panel">
                  <div className="dashboard-panel-body">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">Trade Lifecycle</h2>
                        <p className="dashboard-panel-subtitle">
                          The canonical trade record for entries, exits, duration, and realized outcome.
                        </p>
                      </div>
                    </div>
                    <TradeLifecycleTable rows={lifecycle} />
                  </div>
                </div>
              </section>
            </>
          )}

          {activeView === "reconciliation" && (
            <ReconciliationSection
              sectionLoading={sectionLoading}
              sectionErrors={sectionErrors}
              lastUpdated={lastUpdated}
              lastReconciliationStatus={lastReconciliationStatus}
              lastReconciliationAt={lastReconciliationAt}
              reconciliationSummary={reconciliationSummary}
              reconciliationSymbolFilter={reconciliationSymbolFilter}
              setReconciliationSymbolFilter={setReconciliationSymbolFilter}
              reconciliationSymbols={reconciliationSymbols}
              filteredReconciliationDetails={filteredReconciliationDetails}
              reconciliationHistory={reconciliationHistory}
            />
          )}

          {activeView === "broker" && (
            <AlpacaApiLogsSection
              sectionLoading={sectionLoading}
              sectionErrors={sectionErrors}
              alpacaApiLogs={alpacaApiLogs}
              alpacaApiErrors={alpacaApiErrors}
            />
          )}

          {activeView === "analytics" && (
            <>
              <section className="dashboard-section">
                <div className="dashboard-panel dashboard-panel-strong">
                  <div className="dashboard-panel-body">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">Performance Readouts</h2>
                        <p className="dashboard-panel-subtitle">
                          Symbol, mode, exit pattern, and timing edges gathered in one analytical view.
                        </p>
                      </div>
                    </div>
                    <div className="dashboard-metrics-grid">
                      <InsightCard title="Best Symbol" value={insights?.best_symbol?.symbol || "-"} />
                      <InsightCard title="Best Mode" value={insights?.best_mode?.mode || "-"} />
                      <InsightCard title="Most Common Exit" value={insights?.most_common_exit?.exit_reason || "-"} />
                      <InsightCard title="Best Hour (UTC)" value={insights?.best_hour?.entry_hour_utc ?? "-"} />
                    </div>
                  </div>
                </div>
              </section>

              <section className="dashboard-section">
                <div className="dashboard-panel">
                  <div className="dashboard-panel-body">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">Performance Charts</h2>
                        <p className="dashboard-panel-subtitle">
                          Compare symbol mix, mode behavior, and intraday timing without losing the bigger picture.
                        </p>
                      </div>
                    </div>
                    <div className="dashboard-chart-grid">
                      <div>
                        <SymbolPerformanceChart rows={symbolPerformance} />
                      </div>
                      <div>
                        <ModePerformanceChart rows={modePerformance} />
                      </div>
                    </div>
                    <div style={{ marginTop: 20 }}>
                      <HourlyPerformanceChart rows={hourlyPerformance} />
                    </div>
                  </div>
                </div>
              </section>

              <section className="dashboard-section">
                <div className="dashboard-panel">
                  <div className="dashboard-panel-body">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">Equity Curve</h2>
                        <p className="dashboard-panel-subtitle">
                          The bigger account arc, isolated from the heavier operational details.
                        </p>
                      </div>
                    </div>
                    <EquityCurveChart rows={equityCurve} />
                  </div>
                </div>
              </section>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
