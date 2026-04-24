import SummaryCards from "../components/SummaryCards";
import DashboardFilters from "../components/DashboardFilters";
import AttentionRequiredPanel from "../components/dashboard/AttentionRequiredPanel";
import ExecutionInsightsSection from "../components/dashboard/ExecutionInsightsSection";
import HealthOverviewSection from "../components/dashboard/HealthOverviewSection";
import SchedulerHealthSection from "../components/dashboard/SchedulerHealthSection";
import RiskExposurePanel from "../components/dashboard/RiskExposurePanel";
import HourlyAttemptOutcomeChart from "../components/HourlyAttemptOutcomeChart";
import HourlyOutcomeQualityTable from "../components/HourlyOutcomeQualityTable";
import HourlyPerformanceChart from "../components/HourlyPerformanceChart";
import InsightCard from "../components/InsightCard";
import ModePerformanceChart from "../components/ModePerformanceChart";
import RefreshStatusPanel from "../components/dashboard/RefreshStatusPanel";
import SymbolPerformanceChart from "../components/SymbolPerformanceChart";
import {
  runAdminPurgeTestData,
  runIbkrStaleCloseRepair,
  runIbkrVmJournalRepair,
  runSchedulerTestDayCycle,
  sendAdminTestAlert,
} from "../api/dashboard";
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

function buildFreshness(timestamp, freshMinutes = 10, staleMinutes = 45) {
  if (!timestamp) {
    return { label: "No data", tone: "dashboard-badge-neutral" };
  }

  const parsed = new Date(timestamp);
  if (Number.isNaN(parsed.getTime())) {
    return { label: "Unknown age", tone: "dashboard-badge-neutral" };
  }

  const ageMinutes = Math.max(0, Math.round((Date.now() - parsed.getTime()) / 60000));
  if (ageMinutes <= freshMinutes) {
    return { label: `Fresh ${ageMinutes}m`, tone: "dashboard-badge-ok" };
  }
  if (ageMinutes <= staleMinutes) {
    return { label: `Aging ${ageMinutes}m`, tone: "dashboard-badge-warn" };
  }
  return { label: `Stale ${ageMinutes}m`, tone: "dashboard-badge-danger" };
}

function deriveSystemState({ backendHealthStatus, mismatchLabel, ibkrStatus }) {
  const ibkrState = String(ibkrStatus?.state || "UNKNOWN").toUpperCase();

  if (ibkrState === "DB_ONLY") {
    return {
      label: "DB_ONLY",
      tone: "dashboard-badge-info",
      detail: ibkrStatus?.message || "Dashboard is running in database-only mode.",
    };
  }

  if (ibkrStatus?.enabled && ibkrStatus?.login_required) {
    return {
      label: "LOGIN_REQUIRED",
      tone: "dashboard-badge-warn",
      detail: ibkrStatus?.message || "IBKR login is required before orders can run.",
    };
  }

  if (ibkrState === "MARKET_DATA_UNAVAILABLE") {
    return {
      label: "MARKET_DATA_UNAVAILABLE",
      tone: "dashboard-badge-warn",
      detail: ibkrStatus?.message || "IBKR market data is unavailable.",
    };
  }

  if (backendHealthStatus !== "OK" || mismatchLabel === "CRITICAL") {
    return {
      label: "DEGRADED",
      tone: "dashboard-badge-danger",
      detail: "Backend or reconciliation health needs attention.",
    };
  }

  if (mismatchLabel === "WARNING") {
    return {
      label: "DEGRADED",
      tone: "dashboard-badge-warn",
      detail: "Reconciliation drift is elevated and should be checked.",
    };
  }

  if (ibkrState === "DISABLED") {
    return {
      label: "DISABLED",
      tone: "dashboard-badge-info",
      detail: "IBKR bridge is disabled in current environment.",
    };
  }

  if (ibkrState !== "READY" && ibkrState !== "UNKNOWN") {
    return {
      label: "DEGRADED",
      tone: "dashboard-badge-warn",
      detail: `IBKR state is ${ibkrState}.`,
    };
  }

  return {
    label: "READY",
    tone: "dashboard-badge-ok",
    detail: "Core systems are healthy and trading workflows are available.",
  };
}

function Sparkline({ points }) {
  if (!Array.isArray(points) || points.length < 2) {
    return null;
  }

  const numeric = points.map((value) => Number(value)).filter((value) => Number.isFinite(value));
  if (numeric.length < 2) {
    return null;
  }

  const min = Math.min(...numeric);
  const max = Math.max(...numeric);
  const range = max - min || 1;

  const polyline = numeric
    .map((value, index) => {
      const x = (index / (numeric.length - 1)) * 100;
      const y = 100 - ((value - min) / range) * 100;
      return `${x},${y}`;
    })
    .join(" ");

  return (
    <svg className="dashboard-sparkline" viewBox="0 0 100 100" preserveAspectRatio="none" aria-hidden="true">
      <polyline points={polyline} className="dashboard-sparkline-line" />
    </svg>
  );
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
  const [isRunningDayCycleTest, setIsRunningDayCycleTest] = useState(false);
  const [isRunningMorningCheck, setIsRunningMorningCheck] = useState(false);
  const [isPurgingTestData, setIsPurgingTestData] = useState(false);
  const [adminDrawerOpen, setAdminDrawerOpen] = useState(false);
  const [ibkrRepairDate, setIbkrRepairDate] = useState(todayDateInputValue);
  const [dayCycleModes, setDayCycleModes] = useState("us_test");
  const [dayCycleScanRounds, setDayCycleScanRounds] = useState("1");
  const [dayCycleIntervalSeconds, setDayCycleIntervalSeconds] = useState("0");
  const [dayCycleRunInitialSync, setDayCycleRunInitialSync] = useState(true);
  const [dayCycleSyncAfterEachScan, setDayCycleSyncAfterEachScan] = useState(true);
  const [dayCycleRunEodClose, setDayCycleRunEodClose] = useState(false);
  const [dayCycleRunPostClose, setDayCycleRunPostClose] = useState(false);
  const [dayCyclePaperTrade, setDayCyclePaperTrade] = useState(true);
  const [dayCycleIgnoreMarketHours, setDayCycleIgnoreMarketHours] = useState(true);
  const [dayCycleDebug, setDayCycleDebug] = useState(false);
  const [dayCycleDisableStrategyGates, setDayCycleDisableStrategyGates] = useState(true);

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
    ibkrLiveChecksEnabled,
    handleApplyFilters,
    refreshData,
    refreshIbkrStatusLive,
    rerunReconciliation,
    syncPaperTrades,
    ibkrOpenTradesHasMore,
    ibkrLifecycleHasMore,
    isLoadingMoreIbkrOpenTrades,
    isLoadingMoreIbkrLifecycle,
    loadMoreIbkrOpenTrades,
    loadMoreIbkrLifecycle,
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
        : ibkrState === "DISABLED" || ibkrState === "DB_ONLY"
          ? "dashboard-pill-info"
          : "dashboard-pill-danger";

  const hasDrilldown = Boolean(drilldown.symbol || drilldown.mode || drilldown.hourUtc);
  const recentIbkrPlacedCount = ibkrRecentAttempts.filter(
    (row) => String(row?.decision_stage || "").toUpperCase() === "PLACED"
  ).length;
  const totalTrades = Number(summary?.summary?.trade_count || 0);
  const realizedPnl = formatCurrency(summary?.summary?.realized_pnl_total);
  const winRate =
    summary?.summary?.win_rate_percent !== null && summary?.summary?.win_rate_percent !== undefined
      ? `${Number(summary.summary.win_rate_percent).toFixed(1)}%`
      : "-";

  const systemState = deriveSystemState({
    backendHealthStatus,
    mismatchLabel,
    ibkrStatus,
  });
  const panelFreshness = buildFreshness(lastUpdated, 8, 30);
  const scanFreshness = buildFreshness(opsSummary?.latest_scan_run?.scan_time, 15, 60);
  const pollingFreshness = buildFreshness(lastUpdated, 10, 40);

  const pnlSpark = (equityCurve || []).slice(-28).map((row) => Number(row?.cumulative_pnl || 0));
  const tradesSpark = (equityCurve || []).slice(-28).map((_, index) => index + 1);
  const attemptSpark = (paperTradeAttemptHourlySummary || [])
    .slice(-24)
    .map((row) => Number(row?.total_attempts || 0));
  const placementSpark = (paperTradeAttemptHourlySummary || [])
    .slice(-24)
    .map((row) => Number(row?.placement_rate || 0));
  const closedLifecycleRows = [...(ibkrLifecycle || [])]
    .filter((row) => String(row?.status || "").toUpperCase() === "CLOSED")
    .sort((left, right) => new Date(left?.exit_time || 0).getTime() - new Date(right?.exit_time || 0).getTime())
    .slice(-24);
  let runningWins = 0;
  const winRateSpark = closedLifecycleRows.map((row, index) => {
    if (Number(row?.realized_pnl || 0) > 0) {
      runningWins += 1;
    }
    return ((runningWins / (index + 1)) * 100).toFixed(2);
  });

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

  async function handleMorningCheck() {
    try {
      setIsRunningMorningCheck(true);
      if (ibkrLiveChecksEnabled) {
        await refreshIbkrStatusLive();
      }
      await syncPaperTrades();
      await rerunReconciliation();
      pushToast({ type: "success", message: "Morning Check completed." });
    } catch (err) {
      pushToast({ type: "error", message: err?.message || "Morning Check failed" });
    } finally {
      setIsRunningMorningCheck(false);
    }
  }

  async function handleRunTestDayCycle() {
    try {
      setIsRunningDayCycleTest(true);
      const parsedModes = dayCycleModes
        .split(",")
        .map((token) => token.trim().toLowerCase())
        .filter(Boolean);
      const scanRounds = Math.max(1, Number.parseInt(dayCycleScanRounds || "1", 10) || 1);
      const scanIntervalSeconds = Math.max(0, Number.parseFloat(dayCycleIntervalSeconds || "0") || 0);

      const payload = {
        modes: parsedModes,
        scan_rounds: scanRounds,
        scan_interval_seconds: scanIntervalSeconds,
        run_initial_sync: dayCycleRunInitialSync,
        sync_after_each_scan: dayCycleSyncAfterEachScan,
        run_eod_close: dayCycleRunEodClose,
        run_post_close: dayCycleRunPostClose,
        paper_trade: dayCyclePaperTrade,
        ignore_market_hours: dayCycleIgnoreMarketHours,
        debug: dayCycleDebug,
        disable_strategy_gates: dayCycleDisableStrategyGates,
      };

      const data = await runSchedulerTestDayCycle(payload);
      const actionCount = Number(data?.action_count || 0);
      const elapsedSeconds = Number(data?.elapsed_seconds || 0);
      if (data?.ok) {
        pushToast({
          type: "success",
          message: `Test cycle completed: ${actionCount} action(s) in ${elapsedSeconds.toFixed(2)}s.`,
        });
      } else {
        pushToast({
          type: "error",
          message: data?.error || "Test cycle completed with issues.",
        });
      }
      refreshData();
    } catch (err) {
      pushToast({ type: "error", message: err?.message || "Failed to run test day cycle" });
    } finally {
      setIsRunningDayCycleTest(false);
    }
  }

  async function handlePurgeTestData() {
    const confirmed = typeof window !== "undefined"
      ? window.confirm("Delete test data from all operational tables? This cannot be undone.")
      : true;
    if (!confirmed) {
      return;
    }

    try {
      setIsPurgingTestData(true);
      const data = await runAdminPurgeTestData();
      const deletedCount = Number(data?.total_deleted || 0);
      pushToast({
        type: "success",
        message: `Test data purged. Deleted rows: ${deletedCount}.`,
      });
      refreshData();
    } catch (err) {
      pushToast({ type: "error", message: err?.message || "Failed to purge test data" });
    } finally {
      setIsPurgingTestData(false);
    }
  }

  useEffect(() => {
    document.documentElement.dataset.theme = "dark";
  }, []);

  useEffect(() => {
    function handleEscape(event) {
      if (event.key === "Escape") {
        setAdminDrawerOpen(false);
      }
    }

    window.addEventListener("keydown", handleEscape);
    return () => window.removeEventListener("keydown", handleEscape);
  }, []);

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
        {showInitialLoading && <div className="dashboard-loading">Loading dashboard workspace...</div>}
        {error && !showInitialLoading && <div className="dashboard-error">Error: {error}</div>}

        <section className="dashboard-command-header">
          <div className="dashboard-command-main">
            <div className="dashboard-command-headline">
              <h1 className="dashboard-command-title">Stock Scanner</h1>
              <span className={`dashboard-badge ${systemState.tone}`}>System {systemState.label}</span>
            </div>
            <div className="dashboard-system-state-detail">{systemState.detail}</div>
            <div className="dashboard-command-meta">
              <span
                className={`dashboard-pill dashboard-pill-status ${
                  backendHealthStatus === "OK"
                    ? "dashboard-pill-ok"
                    : backendHealthStatus === "WARNING"
                      ? "dashboard-pill-warn"
                      : "dashboard-pill-danger"
                }`}
              >
                Backend {backendHealthStatus || "UNKNOWN"}
              </span>
              <span className={`dashboard-pill dashboard-pill-status ${mismatchTone}`}>
                Recon {reconciliationHealthStatus || "UNKNOWN"}
              </span>
              <span className={`dashboard-pill dashboard-pill-status ${ibkrTone}`}>
                IBKR {ibkrState}
              </span>
              {ibkrStatus?.enabled && (
                <span className="dashboard-pill">Login {ibkrStatus?.login_required ? "Needed" : "Ready"}</span>
              )}
            </div>
          </div>

          <div className="dashboard-command-controls">
            <div className="dashboard-command-row dashboard-command-row-dense">
              <div className="dashboard-view-nav dashboard-view-nav-inline">
                {DASHBOARD_VIEWS.map((view) => (
                  <button
                    key={view.id}
                    type="button"
                    onClick={() => setActiveView(view.id)}
                    className={`dashboard-view-tab dashboard-view-tab-inline ${
                      activeView === view.id ? "dashboard-view-tab-active" : ""
                    }`}
                  >
                    <span className="dashboard-view-tab-label">{view.label}</span>
                  </button>
                ))}
              </div>
              <DashboardFilters onApply={handleApplyFilters} />
              <button
                onClick={refreshData}
                disabled={isRefreshing}
                className="dashboard-button dashboard-button-primary dashboard-button-compact"
              >
                {isRefreshing ? "Refreshing..." : "Refresh"}
              </button>
              {ibkrLiveChecksEnabled && (
                <button
                  type="button"
                  onClick={handleRefreshIbkrLiveStatus}
                  disabled={isRefreshingIbkrStatus}
                  className="dashboard-button dashboard-button-neutral dashboard-button-compact"
                >
                  {isRefreshingIbkrStatus ? "Checking..." : "IBKR Live"}
                </button>
              )}
              <button
                onClick={syncPaperTrades}
                disabled={isRefreshing || isRunningSync}
                className="dashboard-button dashboard-button-secondary dashboard-button-compact"
              >
                {isRunningSync ? "Syncing..." : "Sync Trades"}
              </button>
              <button
                type="button"
                onClick={handleMorningCheck}
                disabled={isRunningMorningCheck || isRefreshing || isRunningSync}
                className="dashboard-button dashboard-button-secondary dashboard-button-compact"
              >
                {isRunningMorningCheck ? "Morning Check..." : "Morning Check"}
              </button>
              <button
                type="button"
                onClick={() => setAdminDrawerOpen(true)}
                className="dashboard-button dashboard-button-neutral dashboard-button-compact"
              >
                Admin
              </button>
            </div>
            <div className="dashboard-command-row dashboard-command-row-meta">
              <span className="dashboard-pill">
                Last: {lastUpdated ? new Date(lastUpdated).toLocaleString() : "Loading..."}
              </span>
              <span className="dashboard-pill">
                Next: {nextRefreshAt ? new Date(nextRefreshAt).toLocaleTimeString() : "Paused"}
              </span>
              <span className="dashboard-pill">
                {autoRefreshActive ? "Auto-refresh Active" : `Paused (${autoRefreshMarketTime})`}
              </span>
            </div>
          </div>
        </section>

        {adminDrawerOpen && (
          <div className="dashboard-admin-overlay" onClick={() => setAdminDrawerOpen(false)}>
            <aside className="dashboard-admin-drawer" onClick={(event) => event.stopPropagation()}>
              <div className="dashboard-admin-header">
                <h3>Admin Actions</h3>
                <button
                  type="button"
                  className="dashboard-icon-button"
                  onClick={() => setAdminDrawerOpen(false)}
                  aria-label="Close admin drawer"
                >
                  ✕
                </button>
              </div>
              <div className="dashboard-admin-body">
                <div className="dashboard-date-filter-inline">
                  <label htmlFor="test-cycle-modes">Test Modes</label>
                  <input
                    id="test-cycle-modes"
                    type="text"
                    value={dayCycleModes}
                    onChange={(event) => setDayCycleModes(event.target.value)}
                    className="dashboard-input"
                    placeholder="us_test"
                  />
                </div>
                <div className="dashboard-date-filter-inline">
                  <label htmlFor="test-cycle-rounds">Scan Rounds</label>
                  <input
                    id="test-cycle-rounds"
                    type="number"
                    min="1"
                    value={dayCycleScanRounds}
                    onChange={(event) => setDayCycleScanRounds(event.target.value)}
                    className="dashboard-input"
                  />
                </div>
                <div className="dashboard-date-filter-inline">
                  <label htmlFor="test-cycle-interval">Interval Seconds</label>
                  <input
                    id="test-cycle-interval"
                    type="number"
                    min="0"
                    step="1"
                    value={dayCycleIntervalSeconds}
                    onChange={(event) => setDayCycleIntervalSeconds(event.target.value)}
                    className="dashboard-input"
                  />
                </div>
                <label className="dashboard-inline-meta">
                  <input
                    type="checkbox"
                    checked={dayCycleRunInitialSync}
                    onChange={(event) => setDayCycleRunInitialSync(event.target.checked)}
                  />
                  Run Initial Sync
                </label>
                <label className="dashboard-inline-meta">
                  <input
                    type="checkbox"
                    checked={dayCycleSyncAfterEachScan}
                    onChange={(event) => setDayCycleSyncAfterEachScan(event.target.checked)}
                  />
                  Sync After Each Scan
                </label>
                <label className="dashboard-inline-meta">
                  <input
                    type="checkbox"
                    checked={dayCycleRunEodClose}
                    onChange={(event) => setDayCycleRunEodClose(event.target.checked)}
                  />
                  Run EOD Close
                </label>
                <label className="dashboard-inline-meta">
                  <input
                    type="checkbox"
                    checked={dayCycleRunPostClose}
                    onChange={(event) => setDayCycleRunPostClose(event.target.checked)}
                  />
                  Run Post Close
                </label>
                <label className="dashboard-inline-meta">
                  <input
                    type="checkbox"
                    checked={dayCyclePaperTrade}
                    onChange={(event) => setDayCyclePaperTrade(event.target.checked)}
                  />
                  Paper Trade
                </label>
                <label className="dashboard-inline-meta">
                  <input
                    type="checkbox"
                    checked={dayCycleIgnoreMarketHours}
                    onChange={(event) => setDayCycleIgnoreMarketHours(event.target.checked)}
                  />
                  Ignore Market Hours
                </label>
                <label className="dashboard-inline-meta">
                  <input
                    type="checkbox"
                    checked={dayCycleDebug}
                    onChange={(event) => setDayCycleDebug(event.target.checked)}
                  />
                  Debug Output
                </label>
                <label className="dashboard-inline-meta">
                  <input
                    type="checkbox"
                    checked={dayCycleDisableStrategyGates}
                    onChange={(event) => setDayCycleDisableStrategyGates(event.target.checked)}
                  />
                  Disable Strategy Gates (Test Only)
                </label>
                <button
                  type="button"
                  onClick={handleRunTestDayCycle}
                  disabled={isRunningDayCycleTest || !dayCycleModes.trim()}
                  className="dashboard-button dashboard-button-primary"
                >
                  {isRunningDayCycleTest ? "Running Test Cycle..." : "Run Test Day Cycle"}
                </button>
                <div className="dashboard-date-filter-inline">
                  <label htmlFor="ibkr-repair-date">Repair Date</label>
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
                  className="dashboard-button dashboard-button-neutral"
                >
                  {isRunningIbkrRepair ? "Repairing..." : "Repair IBKR"}
                </button>
                <button
                  type="button"
                  onClick={handleRunIbkrDeepRepair}
                  disabled={isRunningIbkrDeepRepair || !ibkrRepairDate}
                  className="dashboard-button dashboard-button-secondary"
                >
                  {isRunningIbkrDeepRepair ? "Deep Repair..." : "Deep Repair"}
                </button>
                <button
                  type="button"
                  onClick={handleSendTestAlert}
                  disabled={isSendingTestAlert}
                  className="dashboard-button dashboard-button-neutral"
                >
                  {isSendingTestAlert ? "Sending..." : "Send Test Alert"}
                </button>
                <button
                  type="button"
                  onClick={handlePurgeTestData}
                  disabled={isPurgingTestData}
                  className="dashboard-button dashboard-button-secondary"
                >
                  {isPurgingTestData ? "Purging..." : "Purge Test Data (All Tables)"}
                </button>
              </div>
            </aside>
          </div>
        )}

        {toast && (
          <div
            className={`dashboard-toast ${
              toast.type === "success" ? "dashboard-toast-success" : "dashboard-toast-error"
            }`}
          >
            {toast.message}
          </div>
        )}

        <section className="dashboard-quick-grid">
          <article className="dashboard-quick-card">
            <span className="dashboard-quick-label">Realized P&amp;L</span>
            <strong className="dashboard-quick-value">{realizedPnl}</strong>
            <Sparkline points={pnlSpark} />
          </article>
          <article className="dashboard-quick-card">
            <span className="dashboard-quick-label">Trades</span>
            <strong className="dashboard-quick-value">{totalTrades}</strong>
            <Sparkline points={tradesSpark} />
          </article>
          <article className="dashboard-quick-card">
            <span className="dashboard-quick-label">Win Rate</span>
            <strong className="dashboard-quick-value">{winRate}</strong>
            <Sparkline points={winRateSpark} />
          </article>
          <article className="dashboard-quick-card">
            <span className="dashboard-quick-label">Open IBKR</span>
            <strong className="dashboard-quick-value">{ibkrOpenTrades.length}</strong>
            <Sparkline points={attemptSpark} />
          </article>
          <article className="dashboard-quick-card">
            <span className="dashboard-quick-label">Recent IBKR Placements</span>
            <strong className="dashboard-quick-value">{recentIbkrPlacedCount}</strong>
            <Sparkline points={placementSpark} />
          </article>
        </section>

        <div className="dashboard-grid">
          {hasDrilldown && (
            <section className="dashboard-drilldown-panel">
              <div className="dashboard-banner-title">Active Drilldown</div>
              <div className="dashboard-inline-meta">
                {drilldown.symbol && <span className="dashboard-pill">Symbol {drilldown.symbol}</span>}
                {drilldown.mode && <span className="dashboard-pill">Mode {drilldown.mode}</span>}
                {drilldown.hourUtc && <span className="dashboard-pill">Hour {drilldown.hourUtc}:00 UTC</span>}
                <button type="button" className="dashboard-button dashboard-button-neutral" onClick={clearDrilldown}>
                  Clear
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
                panelFreshnessLabel={panelFreshness.label}
                panelFreshnessTone={panelFreshness.tone}
                onRetry={refreshData}
                isRetrying={isRefreshing}
              />

              <div className="dashboard-split">
                <RiskExposurePanel
                  sectionLoading={sectionLoading}
                  sectionErrors={sectionErrors}
                  riskExposureSummary={riskExposureSummary}
                  panelFreshnessLabel={panelFreshness.label}
                  panelFreshnessTone={panelFreshness.tone}
                  onRetry={refreshData}
                  isRetrying={isRefreshing}
                />
                <SchedulerHealthSection
                  opsSummary={opsSummary}
                  ibkrRecentAttempts={ibkrRecentAttempts}
                  ibkrStatus={ibkrStatus}
                  panelFreshnessLabel={scanFreshness.label}
                  panelFreshnessTone={scanFreshness.tone}
                  onRetry={refreshData}
                  isRetrying={isRefreshing}
                />
              </div>

              <RefreshStatusPanel
                lastUpdated={lastUpdated}
                nextRefreshAt={nextRefreshAt}
                autoRefreshActive={autoRefreshActive}
                refreshWindowLabel={refreshWindowLabel}
                autoRefreshMarketTime={autoRefreshMarketTime}
                panelFreshnessLabel={pollingFreshness.label}
                panelFreshnessTone={pollingFreshness.tone}
                onRetry={refreshData}
                isRetrying={isRefreshing}
              />

              <section className="dashboard-section">
                <div className="dashboard-panel">
                  <div className="dashboard-panel-body">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">Equity Curve</h2>
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
                <div className="dashboard-panel">
                  <div className="dashboard-panel-body">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">IBKR Open Trades</h2>
                      </div>
                      <div className="dashboard-toolbar">
                        <button
                          onClick={syncPaperTrades}
                          disabled={isRefreshing || isRunningSync}
                          className="dashboard-button dashboard-button-secondary dashboard-button-compact"
                        >
                          {isRunningSync ? "Syncing..." : "Sync"}
                        </button>
                        <button
                          type="button"
                          onClick={loadMoreIbkrOpenTrades}
                          disabled={!ibkrOpenTradesHasMore || isLoadingMoreIbkrOpenTrades}
                          className="dashboard-button dashboard-button-neutral dashboard-button-compact"
                        >
                          {isLoadingMoreIbkrOpenTrades ? "Loading..." : ibkrOpenTradesHasMore ? "Load More" : "All Loaded"}
                        </button>
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
                      </div>
                      <div className="dashboard-toolbar">
                        <button
                          type="button"
                          onClick={loadMoreIbkrLifecycle}
                          disabled={!ibkrLifecycleHasMore || isLoadingMoreIbkrLifecycle}
                          className="dashboard-button dashboard-button-neutral dashboard-button-compact"
                        >
                          {isLoadingMoreIbkrLifecycle ? "Loading..." : ibkrLifecycleHasMore ? "Load More" : "All Loaded"}
                        </button>
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
                  <div className="dashboard-panel-body dashboard-panel-body-tight dashboard-inline-actions">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">Reconciliation</h2>
                        <p className="dashboard-panel-subtitle">
                          Read-only mismatch history. Manual reconciliation runs are disabled in IBKR-only mode.
                        </p>
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
                panelFreshnessLabel={panelFreshness.label}
                panelFreshnessTone={panelFreshness.tone}
                onRetry={refreshData}
                isRetrying={isRefreshing}
              />

              <section className="dashboard-section">
                <div className="dashboard-panel dashboard-panel-strong">
                  <div className="dashboard-panel-body">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">Performance Readouts</h2>
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
                        interactive={
                          insights?.best_hour?.entry_hour_utc !== undefined &&
                          insights?.best_hour?.entry_hour_utc !== null
                        }
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
                                if (!row) {
                                  return "-";
                                }
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
                    <div style={{ marginTop: 16 }}>
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
                    <div style={{ marginTop: 16 }}>
                      <LazySection>
                        <HourlyAttemptOutcomeChart rows={paperTradeAttemptHourlySummary} />
                      </LazySection>
                    </div>
                    <div style={{ marginTop: 16 }}>
                      <LazySection>
                        <HourlyOutcomeQualityTable rows={strategyHourlyOutcomeQuality} strategyOnly />
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
