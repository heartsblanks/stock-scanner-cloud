import SummaryCards from "../components/SummaryCards";
import DashboardFilters from "../components/DashboardFilters";
import AttentionRequiredPanel from "../components/dashboard/AttentionRequiredPanel";
import ExecutionInsightsSection from "../components/dashboard/ExecutionInsightsSection";
import HealthOverviewSection from "../components/dashboard/HealthOverviewSection";
import SchedulerHealthSection from "../components/dashboard/SchedulerHealthSection";
import HourlyAttemptOutcomeChart from "../components/HourlyAttemptOutcomeChart";
import HourlyOutcomeQualityTable from "../components/HourlyOutcomeQualityTable";
import HourlyPerformanceChart from "../components/HourlyPerformanceChart";
import InsightCard from "../components/InsightCard";
import ModePerformanceChart from "../components/ModePerformanceChart";
import RefreshStatusPanel from "../components/dashboard/RefreshStatusPanel";
import SymbolPerformanceChart from "../components/SymbolPerformanceChart";
import { runIbkrStaleCloseRepair, runIbkrVmJournalRepair, sendAdminTestAlert } from "../api/dashboard";
import { useDashboardData } from "../hooks/useDashboardData";
import { Component, Suspense, lazy, useEffect, useState } from "react";

const OpenTradesTable = lazy(() => import("../components/OpenTradesTable"));
const TradeLifecycleTable = lazy(() => import("../components/TradeLifecycleTable"));
const EquityCurveChart = lazy(() => import("../components/EquityCurveChart"));
const ReconciliationSection = lazy(() => import("../components/dashboard/ReconciliationSection"));

const DASHBOARD_VIEWS = [
  { id: "overview", label: "Overview" },
  { id: "trades", label: "Trades" },
  { id: "reconciliation", label: "Reconciliation" },
  { id: "analytics", label: "Analytics" },
];

function formatCurrency(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }

  return `$${Number(value).toFixed(2)}`;
}

function getEntryHourUtc(row) {
  const timestamp = row?.entry_time || row?.timestamp_utc;
  if (!timestamp) {
    return null;
  }

  const parsed = new Date(timestamp);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }

  return String(parsed.getUTCHours()).padStart(2, "0");
}

function LazySection({ children }) {
  return <Suspense fallback={<div className="dashboard-empty">Loading section...</div>}>{children}</Suspense>;
}

class AnalyticsErrorBoundary extends Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false };
  }

  static getDerivedStateFromError() {
    return { hasError: true };
  }

  componentDidUpdate(prevProps) {
    if (prevProps.resetKey !== this.props.resetKey && this.state.hasError) {
      this.setState({ hasError: false });
    }
  }

  render() {
    if (this.state.hasError) {
      return <div className="dashboard-error">Analytics is temporarily unavailable. Refresh and try again.</div>;
    }

    return this.props.children;
  }
}

function getInitialView() {
  if (typeof window === "undefined") {
    return "overview";
  }

  const params = new URLSearchParams(window.location.search);
  const view = String(params.get("view") || "").trim().toLowerCase();
  return DASHBOARD_VIEWS.some((item) => item.id === view) ? view : "overview";
}

function todayDateInputValue() {
  return new Date().toISOString().slice(0, 10);
}

export default function DashboardPage() {
  const [activeView, setActiveView] = useState(getInitialView);
  const [drilldown, setDrilldown] = useState({ symbol: "", mode: "", hourUtc: "" });
  const [isSendingTestAlert, setIsSendingTestAlert] = useState(false);
  const [isRunningIbkrRepair, setIsRunningIbkrRepair] = useState(false);
  const [isRunningIbkrDeepRepair, setIsRunningIbkrDeepRepair] = useState(false);
  const [ibkrRepairDate, setIbkrRepairDate] = useState(todayDateInputValue);
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
    ibkrOpenTrades,
    ibkrLifecycle,
    loading,
    error,
    sectionLoading,
    sectionErrors,
    reconciliationSummary,
    reconciliationSymbolFilter,
    setReconciliationSymbolFilter,
    reconciliationHistory,
    riskExposureSummary,
    opsSummary,
    lastUpdated,
    isRefreshing,
    isRunningSync,
    isRefreshingIbkrStatus,
    toast,
    pushToast,
    lastReconciliationStatus,
    lastReconciliationAt,
    symbolPerformance,
    modePerformance,
    hourlyPerformance,
    hourlyOutcomeQuality,
    strategyHourlyOutcomeQuality,
    externalExitSummary,
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
    topAttemptReasons,
    stageCounts,
    paperTradePlacementRate,
    attentionItems,
    nextRefreshAt,
    autoRefreshActive,
    refreshWindowLabel,
    autoRefreshMarketTime,
    paperTradeAttemptRejections,
    paperTradeAttemptDailySummary,
    paperTradeAttemptHourlySummary,
    ibkrRecentAttempts,
    ibkrStatus,
    handleApplyFilters,
    refreshData,
    refreshIbkrStatusLive,
    rerunReconciliation,
    syncPaperTrades,
  } = useDashboardData(activeView);

  const mismatchTone =
    mismatchLabel === "OK"
      ? "dashboard-pill-ok"
      : mismatchLabel === "WARNING"
        ? "dashboard-pill-warn"
      : mismatchLabel === "CRITICAL"
          ? "dashboard-pill-danger"
          : "dashboard-pill-info";
  const ibkrState = String(ibkrStatus?.state || "UNKNOWN").toUpperCase();
  const ibkrTone =
    ibkrState === "READY"
      ? "dashboard-pill-ok"
      : ibkrState === "LOGIN_REQUIRED" || ibkrState === "MARKET_DATA_UNAVAILABLE"
        ? "dashboard-pill-warn"
        : ibkrState === "DISABLED"
          ? "dashboard-pill-info"
          : "dashboard-pill-danger";
  const hasDrilldown = Boolean(drilldown.symbol || drilldown.mode || drilldown.hourUtc);
  const recentIbkrPlacedCount = ibkrRecentAttempts.filter(
    (row) => String(row?.decision_stage || "").toUpperCase() === "PLACED"
  ).length;

  const filteredIbkrOpenTrades = ibkrOpenTrades.filter((row) => {
    const symbolMatch = !drilldown.symbol || String(row?.symbol || "").trim().toUpperCase() === drilldown.symbol;
    const modeMatch = !drilldown.mode || String(row?.mode || "").trim() === drilldown.mode;
    const hourMatch = !drilldown.hourUtc || getEntryHourUtc(row) === drilldown.hourUtc;
    return symbolMatch && modeMatch && hourMatch;
  });

  const filteredIbkrLifecycle = ibkrLifecycle.filter((row) => {
    const symbolMatch = !drilldown.symbol || String(row?.symbol || "").trim().toUpperCase() === drilldown.symbol;
    const modeMatch = !drilldown.mode || String(row?.mode || "").trim() === drilldown.mode;
    const hourMatch = !drilldown.hourUtc || getEntryHourUtc(row) === drilldown.hourUtc;
    return symbolMatch && modeMatch && hourMatch;
  });

  function applyDrilldown(nextDrilldown) {
    setDrilldown((current) => ({
      symbol: nextDrilldown.symbol ?? current.symbol,
      mode: nextDrilldown.mode ?? current.mode,
      hourUtc: nextDrilldown.hourUtc ?? current.hourUtc,
    }));
    setActiveView("trades");
  }

  function clearDrilldown() {
    setDrilldown({ symbol: "", mode: "", hourUtc: "" });
  }

  async function handleSendTestAlert() {
    try {
      setIsSendingTestAlert(true);
      const data = await sendAdminTestAlert(`Dashboard test alert at ${new Date().toLocaleString()}`);

      if (data?.ok) {
        pushToast({ type: "success", message: "Telegram test alert sent." });
      } else {
        pushToast({ type: "error", message: `Alert failed: ${data?.error || "Unknown error"}` });
      }
    } catch (err) {
      pushToast({ type: "error", message: err?.message || "Failed to send Telegram test alert" });
    } finally {
      setIsSendingTestAlert(false);
    }
  }

  async function handleRunIbkrRepair() {
    try {
      setIsRunningIbkrRepair(true);
      const data = await runIbkrStaleCloseRepair(ibkrRepairDate);
      const repairedCount = Number(data?.repaired_count || 0);
      const skippedCount = Number(data?.skipped_count || 0);
      const staleRowCount = Number(data?.stale_row_count || 0);

      pushToast({
        type: "success",
        message: `IBKR repair finished for ${ibkrRepairDate}: repaired ${repairedCount}, skipped ${skippedCount}, stale rows ${staleRowCount}.`,
      });
      refreshData();
    } catch (err) {
      pushToast({ type: "error", message: err?.message || "Failed to run IBKR stale close repair" });
    } finally {
      setIsRunningIbkrRepair(false);
    }
  }

  async function handleRunIbkrDeepRepair() {
    try {
      setIsRunningIbkrDeepRepair(true);
      const data = await runIbkrVmJournalRepair(ibkrRepairDate);
      const appliedCount = Number(data?.applied_count || 0);
      const candidateCount = Number(data?.candidate_count || 0);
      pushToast({
        type: "success",
        message: `IBKR deep repair finished for ${ibkrRepairDate}: applied ${appliedCount} of ${candidateCount} journal candidates.`,
      });
      refreshData();
    } catch (err) {
      pushToast({ type: "error", message: err?.message || "Failed to run IBKR deep repair" });
    } finally {
      setIsRunningIbkrDeepRepair(false);
    }
  }

  async function handleRefreshIbkrLiveStatus() {
    try {
      await refreshIbkrStatusLive();
      pushToast({ type: "success", message: "IBKR live status refreshed." });
    } catch (err) {
      pushToast({ type: "error", message: err?.message || "Failed to refresh IBKR live status" });
    }
  }

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    window.localStorage.setItem("dashboard-theme", theme);
  }, [theme]);

  const visibleViews = DASHBOARD_VIEWS;

  useEffect(() => {
    const viewLabel = DASHBOARD_VIEWS.find((view) => view.id === activeView)?.label || "Overview";
    document.title = `${viewLabel} | Stock Scanner Console`;

    if (typeof window !== "undefined") {
      const url = new URL(window.location.href);
      url.searchParams.set("view", activeView);
      window.history.replaceState({}, "", url);
    }
  }, [activeView]);

  if (error) {
    if (!lastUpdated && !summary && !reconciliationSummary) {
      return <div className="dashboard-error">Error: {error}</div>;
    }
  }

  const showInitialLoading = loading && !lastUpdated;

  return (
    <div className="dashboard-shell">
      <div className="dashboard-frame">
        {showInitialLoading && (
          <div className="dashboard-loading">Loading dashboard workspace...</div>
        )}
        {error && !showInitialLoading && (
          <div className="dashboard-error">Error: {error}</div>
        )}
        <section className="dashboard-hero">
          <div className="dashboard-hero-grid">
            <div>
              <div className="dashboard-kicker">
                <span className="dashboard-kicker-dot" />
                Trading Operations Console
              </div>
              <div className="dashboard-hero-status-row">
                <span className="dashboard-hero-status-label">Health Snapshot</span>
                <span className={`dashboard-pill dashboard-pill-status ${backendHealthStatus === "OK" ? "dashboard-pill-ok" : backendHealthStatus === "WARNING" ? "dashboard-pill-warn" : "dashboard-pill-danger"}`}>
                  Backend {backendHealthStatus || "UNKNOWN"}
                </span>
                <span className={`dashboard-pill dashboard-pill-status ${mismatchTone}`}>
                  Recon {reconciliationHealthStatus || "UNKNOWN"}
                </span>
                <span className={`dashboard-pill dashboard-pill-status ${ibkrTone}`}>
                  IBKR {ibkrState}
                </span>
                {ibkrStatus?.enabled && (
                  <span className="dashboard-pill">
                    Login {ibkrStatus?.login_required ? "Needed" : "Not Needed"}
                  </span>
                )}
              </div>
              <div className="dashboard-hero-meta">
                <div className="dashboard-hero-stat">
                  <div className="dashboard-hero-stat-label">Realized P&amp;L</div>
                  <div className="dashboard-hero-stat-value">
                    {formatCurrency(summary?.summary?.realized_pnl_total)}
                  </div>
                </div>
                <div className="dashboard-hero-stat">
                  <div className="dashboard-hero-stat-label">Open IBKR Positions</div>
                  <div className="dashboard-hero-stat-value">
                    {ibkrOpenTrades.length}
                  </div>
                </div>
                <div className="dashboard-hero-stat">
                  <div className="dashboard-hero-stat-label">Reconciliation Drift</div>
                  <div className="dashboard-hero-stat-value">
                    {mismatch !== null ? `${mismatch} ${mismatchLabel ? `(${mismatchLabel})` : ""}` : "-"}
                  </div>
                </div>
                <div className="dashboard-hero-stat">
                  <div className="dashboard-hero-stat-label">Recent IBKR Placements</div>
                  <div className="dashboard-hero-stat-value">
                    {recentIbkrPlacedCount}
                  </div>
                </div>
              </div>
            </div>

            <div className="dashboard-hero-side">
              <div className="dashboard-callout">
                <div className="dashboard-callout-label">Operational Pulse</div>
                <div className="dashboard-callout-value">{backendHealthStatus || "UNKNOWN"}</div>
                <div className="dashboard-callout-note">
                  Last refresh: {lastUpdated ? new Date(lastUpdated).toLocaleString() : "Loading initial view..."}
                </div>
              </div>

              <div className="dashboard-hero-controls">
                <DashboardFilters onApply={handleApplyFilters} />
                <div className="dashboard-date-filter-inline">
                  <label htmlFor="ibkr-repair-date">IBKR Repair</label>
                  <input
                    id="ibkr-repair-date"
                    type="date"
                    value={ibkrRepairDate}
                    onChange={(event) => setIbkrRepairDate(event.target.value)}
                    className="dashboard-input dashboard-input-inline-date"
                  />
                </div>
                <button
                  type="button"
                  onClick={handleRunIbkrRepair}
                  disabled={isRunningIbkrRepair || !ibkrRepairDate}
                  className="dashboard-button dashboard-button-neutral dashboard-button-compact"
                >
                  {isRunningIbkrRepair ? "Repairing..." : "Repair IBKR"}
                </button>
                <button
                  type="button"
                  onClick={handleRunIbkrDeepRepair}
                  disabled={isRunningIbkrDeepRepair || !ibkrRepairDate}
                  className="dashboard-button dashboard-button-secondary dashboard-button-compact"
                >
                  {isRunningIbkrDeepRepair ? "Deep Repair..." : "Deep Repair"}
                </button>
                <button
                  type="button"
                  onClick={handleRefreshIbkrLiveStatus}
                  disabled={isRefreshingIbkrStatus}
                  className="dashboard-button dashboard-button-neutral dashboard-button-compact"
                >
                  {isRefreshingIbkrStatus ? "Checking IBKR..." : "Refresh IBKR Live"}
                </button>
                <button
                  type="button"
                  onClick={handleSendTestAlert}
                  disabled={isSendingTestAlert}
                  className="dashboard-button dashboard-button-secondary dashboard-button-compact"
                >
                  {isSendingTestAlert ? "Sending..." : "Test Alert"}
                </button>
                <button
                  onClick={refreshData}
                  disabled={isRefreshing}
                  className="dashboard-button dashboard-button-primary dashboard-button-compact"
                >
                  {isRefreshing ? "Refreshing..." : "Refresh"}
                </button>
                <button
                  onClick={() => setTheme((currentTheme) => (currentTheme === "dark" ? "light" : "dark"))}
                  className="dashboard-button dashboard-button-theme dashboard-button-compact"
                >
                  {theme === "dark" ? "Switch to Light" : "Switch to Dark"}
                </button>
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
          <section className="dashboard-view-nav-panel">
            <div className="dashboard-view-nav">
              {visibleViews.map((view) => (
                <button
                  key={view.id}
                  type="button"
                  onClick={() => setActiveView(view.id)}
                  className={`dashboard-view-tab ${activeView === view.id ? "dashboard-view-tab-active" : ""}`}
                >
                  <span className="dashboard-view-tab-label">{view.label}</span>
                </button>
              ))}
            </div>
          </section>

          {hasDrilldown && (
            <section className="dashboard-drilldown-panel">
              <div className="dashboard-view-nav-copy">
                <div className="dashboard-banner-title">Active Drilldown</div>
                <div className="dashboard-banner-copy">
                  The trades view is narrowed by the selections you made from the overview cards or charts.
                </div>
              </div>
              <div className="dashboard-inline-meta">
                {drilldown.symbol && <span className="dashboard-pill">Symbol {drilldown.symbol}</span>}
                {drilldown.mode && <span className="dashboard-pill">Mode {drilldown.mode}</span>}
                {drilldown.hourUtc && <span className="dashboard-pill">Hour {drilldown.hourUtc}:00 UTC</span>}
                <button type="button" className="dashboard-button dashboard-button-neutral" onClick={clearDrilldown}>
                  Clear Drilldown
                </button>
              </div>
            </section>
          )}

          {activeView === "overview" && (
            <>
              <AttentionRequiredPanel items={attentionItems} />
              <SummaryCards data={summary} />
              {sectionLoading.overview && <div className="dashboard-empty">Loading overview...</div>}
              {sectionErrors.overview && <div className="dashboard-error">{sectionErrors.overview}</div>}

              <HealthOverviewSection
                lastUpdated={lastUpdated}
                sectionLoading={sectionLoading}
                sectionErrors={sectionErrors}
                ibkrOpenCount={ibkrOpenTrades.length}
                mismatch={mismatch}
                mismatchLabel={mismatchLabel}
                backendHealthStatus={backendHealthStatus}
                syncHealthStatus={syncHealthStatus}
                reconciliationHealthStatus={reconciliationHealthStatus}
                lastReconciliationAt={lastReconciliationAt}
                isRunningSync={isRunningSync}
                ibkrStatus={ibkrStatus}
                riskExposureSummary={riskExposureSummary}
                confidenceMultiplier={confidenceMultiplier}
                lossMultiplier={lossMultiplier}
                finalSizingMultiplier={finalSizingMultiplier}
                multiplierStatus={multiplierStatus}
                compact
              />

              <SchedulerHealthSection
                opsSummary={opsSummary}
                ibkrRecentAttempts={ibkrRecentAttempts}
                ibkrStatus={ibkrStatus}
              />

              <div className="dashboard-split">
                <RefreshStatusPanel
                  lastUpdated={lastUpdated}
                  nextRefreshAt={nextRefreshAt}
                  autoRefreshActive={autoRefreshActive}
                  refreshWindowLabel={refreshWindowLabel}
                  autoRefreshMarketTime={autoRefreshMarketTime}
                />
                <ExecutionInsightsSection
                  sectionLoading={sectionLoading}
                  sectionErrors={sectionErrors}
                  paperTradePlacementRate={paperTradePlacementRate}
                  stageCounts={stageCounts}
                  topAttemptReasons={topAttemptReasons}
                  paperTradeAttemptRejections={paperTradeAttemptRejections}
                  paperTradeAttemptDailySummary={paperTradeAttemptDailySummary}
                  paperTradeAttemptHourlySummary={paperTradeAttemptHourlySummary}
                  ibkrRecentAttempts={ibkrRecentAttempts}
                  ibkrStatus={ibkrStatus}
                  hourlyOutcomeQuality={strategyHourlyOutcomeQuality}
                  externalExitSummary={externalExitSummary}
                />
              </div>

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
                    <LazySection>
                      <EquityCurveChart rows={equityCurve} />
                    </LazySection>
                  </div>
                </div>
              </section>
            </>
          )}

          {activeView === "trades" && (
            <>
              <section className="dashboard-section">
                <div className="dashboard-panel dashboard-panel-strong">
                  <div className="dashboard-panel-body dashboard-panel-body-tight">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">Trade Actions</h2>
                        <p className="dashboard-panel-subtitle">
                          Sync refreshes local IBKR paper trade state when fills or exits look stale.
                        </p>
                      </div>
                      <div className="dashboard-toolbar">
                        <button
                          onClick={syncPaperTrades}
                          disabled={isRefreshing || isRunningSync}
                          className="dashboard-button dashboard-button-secondary"
                        >
                          {isRunningSync ? "Syncing Trades..." : "Sync Paper Trades"}
                        </button>
                      </div>
                    </div>
                  </div>
                </div>
              </section>

              <section className="dashboard-section">
                <div className="dashboard-panel">
                  <div className="dashboard-panel-body">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">IBKR Open Trades</h2>
                        <p className="dashboard-panel-subtitle">
                          Live IBKR paper positions currently tracked by the database and broker sync.
                        </p>
                      </div>
                    </div>
                    <LazySection>
                      <OpenTradesTable trades={filteredIbkrOpenTrades} />
                    </LazySection>
                  </div>
                </div>
              </section>

              <section className="dashboard-section">
                <div className="dashboard-panel">
                  <div className="dashboard-panel-body">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">IBKR Lifecycle</h2>
                        <p className="dashboard-panel-subtitle">
                          Canonical IBKR trade history for entries, exits, and realized outcome.
                        </p>
                      </div>
                    </div>
                    <LazySection>
                      <TradeLifecycleTable rows={filteredIbkrLifecycle} />
                    </LazySection>
                  </div>
                </div>
              </section>
            </>
          )}

          {activeView === "reconciliation" && (
            <>
              <section className="dashboard-section">
                <div className="dashboard-panel dashboard-panel-strong">
                  <div className="dashboard-panel-body dashboard-panel-body-tight">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">Reconciliation Actions</h2>
                        <p className="dashboard-panel-subtitle">
                          Run reconciliation when you want a fresh mismatch audit against the stored trade ledger.
                        </p>
                      </div>
                      <div className="dashboard-toolbar">
                        <button
                          onClick={rerunReconciliation}
                          disabled={isRefreshing}
                          className="dashboard-button dashboard-button-secondary"
                        >
                          Re-run Reconciliation
                        </button>
                      </div>
                    </div>
                  </div>
                </div>
              </section>

              <LazySection>
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
              </LazySection>
            </>
          )}

          {activeView === "analytics" && (
            <AnalyticsErrorBoundary resetKey={`${activeView}-${lastUpdated || "none"}`}>
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
                      <InsightCard
                        title="Best Symbol"
                        value={insights?.best_symbol?.symbol || "-"}
                        interactive={Boolean(insights?.best_symbol?.symbol)}
                        onClick={() =>
                          insights?.best_symbol?.symbol &&
                          applyDrilldown({
                            symbol: String(insights.best_symbol.symbol).trim().toUpperCase(),
                            mode: "",
                            hourUtc: "",
                          })
                        }
                      />
                      <InsightCard
                        title="Best Mode"
                        value={insights?.best_mode?.mode || "-"}
                        interactive={Boolean(insights?.best_mode?.mode)}
                        onClick={() =>
                          insights?.best_mode?.mode &&
                          applyDrilldown({
                            symbol: "",
                            mode: String(insights.best_mode.mode).trim(),
                            hourUtc: "",
                          })
                        }
                      />
                      <InsightCard title="Most Common Exit" value={insights?.most_common_exit?.exit_reason || "-"} />
                      <InsightCard
                        title="Best Hour (UTC)"
                        value={insights?.best_hour?.entry_hour_utc ?? "-"}
                        interactive={insights?.best_hour?.entry_hour_utc !== undefined && insights?.best_hour?.entry_hour_utc !== null}
                        onClick={() =>
                          insights?.best_hour?.entry_hour_utc !== undefined &&
                          insights?.best_hour?.entry_hour_utc !== null &&
                          applyDrilldown({
                            symbol: "",
                            mode: "",
                            hourUtc: String(insights.best_hour.entry_hour_utc).padStart(2, "0"),
                          })
                        }
                      />
                      <InsightCard
                        title="Busiest Attempt Hour (ET)"
                        value={
                          paperTradeAttemptHourlySummary?.length
                            ? (() => {
                                const row = [...paperTradeAttemptHourlySummary].sort(
                                  (left, right) => Number(right.total_attempts || 0) - Number(left.total_attempts || 0)
                                )[0];
                                if (!row) return "-";
                                const hour = Number(row.hour_ny);
                                const suffix = hour >= 12 ? "PM" : "AM";
                                const normalizedHour = hour % 12 || 12;
                                return `${normalizedHour}:00 ${suffix}`;
                              })()
                            : "-"
                        }
                      />
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
                        <LazySection>
                          <SymbolPerformanceChart
                            rows={symbolPerformance}
                            onSymbolSelect={(symbol) =>
                              symbol &&
                              applyDrilldown({
                                symbol: String(symbol).trim().toUpperCase(),
                                mode: "",
                                hourUtc: "",
                              })
                            }
                          />
                        </LazySection>
                      </div>
                      <div>
                        <LazySection>
                          <ModePerformanceChart
                            rows={modePerformance}
                            onModeSelect={(mode) =>
                              mode &&
                              applyDrilldown({
                                symbol: "",
                                mode: String(mode).trim(),
                                hourUtc: "",
                              })
                            }
                          />
                        </LazySection>
                      </div>
                    </div>
                    <div style={{ marginTop: 20 }}>
                      <LazySection>
                        <HourlyPerformanceChart
                          rows={hourlyPerformance}
                          onHourSelect={(hour) =>
                            hour !== undefined &&
                            hour !== null &&
                            applyDrilldown({
                              symbol: "",
                              mode: "",
                              hourUtc: String(hour).padStart(2, "0"),
                            })
                          }
                        />
                      </LazySection>
                    </div>
                    <div style={{ marginTop: 20 }}>
                      <LazySection>
                        <HourlyAttemptOutcomeChart rows={paperTradeAttemptHourlySummary} />
                      </LazySection>
                    </div>
                    <div style={{ marginTop: 20 }}>
                      <LazySection>
                        <HourlyOutcomeQualityTable
                          rows={strategyHourlyOutcomeQuality}
                          strategyOnly
                        />
                      </LazySection>
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
                    <LazySection>
                      <EquityCurveChart rows={equityCurve} />
                    </LazySection>
                  </div>
                </div>
              </section>
            </AnalyticsErrorBoundary>
          )}
        </div>
      </div>
    </div>
  );
}
