import { useCallback, useEffect, useRef, useState } from "react";
import {
  fetchDashboardSummary,
  fetchOpenTrades,
  fetchTradeLifecycle,
  fetchReconciliationSummary,
  fetchReconciliationDetails,
  fetchReconciliationHistory,
  fetchAlpacaOpenPositions,
  fetchRiskExposureSummary,
  fetchAlpacaApiLogs,
  fetchAlpacaApiErrors,
  fetchLatestScanSummary,
  fetchOpsSummary,
  fetchPaperTradeAttemptDailySummary,
  fetchPaperTradeAttemptHourlySummary,
  fetchPaperTradeAttemptRejections,
  runReconciliationNow,
  runSyncPaperTrades,
} from "../api/dashboard";

const AUTO_REFRESH_INTERVAL_MS = 15 * 60 * 1000;

function getEasternMarketSnapshot(now = new Date()) {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const parts = formatter.formatToParts(now);
  const lookup = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  const weekday = lookup.weekday || "";
  const hour = Number(lookup.hour || 0);
  const minute = Number(lookup.minute || 0);
  const totalMinutes = hour * 60 + minute;
  const isWeekday = !["Sat", "Sun"].includes(weekday);
  const active = isWeekday && totalMinutes >= 570 && totalMinutes <= 1020;

  return {
    active,
    weekday,
    totalMinutes,
    label: `${lookup.hour || "00"}:${lookup.minute || "00"} ET`,
  };
}

const INITIAL_SECTION_LOADING = {
  overview: true,
  reconciliation: true,
  risk: true,
  alpacaLogs: true,
  sizing: true,
  attempts: true,
};

const INITIAL_SECTION_ERRORS = {
  overview: null,
  reconciliation: null,
  risk: null,
  alpacaLogs: null,
  sizing: null,
  attempts: null,
};

export function useDashboardData(activeView = "overview") {
  const [summary, setSummary] = useState(null);
  const [openTrades, setOpenTrades] = useState([]);
  const [lifecycle, setLifecycle] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [sectionLoading, setSectionLoading] = useState(INITIAL_SECTION_LOADING);
  const [sectionErrors, setSectionErrors] = useState(INITIAL_SECTION_ERRORS);
  const [filters, setFilters] = useState({ date: "", symbol: "" });
  const [alpacaOpenCount, setAlpacaOpenCount] = useState(null);
  const [reconciliationSummary, setReconciliationSummary] = useState(null);
  const [reconciliationDetails, setReconciliationDetails] = useState([]);
  const [reconciliationSymbolFilter, setReconciliationSymbolFilter] = useState("");
  const [reconciliationHistory, setReconciliationHistory] = useState([]);
  const [riskExposureSummary, setRiskExposureSummary] = useState(null);
  const [alpacaApiLogs, setAlpacaApiLogs] = useState([]);
  const [alpacaApiErrors, setAlpacaApiErrors] = useState([]);
  const [opsSummary, setOpsSummary] = useState(null);
  const [paperTradeAttemptRejections, setPaperTradeAttemptRejections] = useState([]);
  const [paperTradeAttemptDailySummary, setPaperTradeAttemptDailySummary] = useState([]);
  const [paperTradeAttemptHourlySummary, setPaperTradeAttemptHourlySummary] = useState([]);
  const [latestScanSummary, setLatestScanSummary] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [nextRefreshAt, setNextRefreshAt] = useState(null);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isRunningSync, setIsRunningSync] = useState(false);
  const [toast, setToast] = useState(null);
  const [lastReconciliationStatus, setLastReconciliationStatus] = useState(null);
  const [lastReconciliationAt, setLastReconciliationAt] = useState(null);
  const [currentTime, setCurrentTime] = useState(() => new Date());
  const filtersRef = useRef(filters);

  const loadOverviewSection = useCallback(async (activeFilters = filtersRef.current) => {
    try {
      setSectionLoading((prev) => ({ ...prev, overview: true, sizing: true }));
      const [summaryRes, openRes, lifecycleRes, latestScanRes] = await Promise.all([
        fetchDashboardSummary(activeFilters?.date || undefined),
        fetchOpenTrades(100),
        fetchTradeLifecycle(200),
        fetchLatestScanSummary(),
      ]);

      const openRows = openRes?.rows || [];
      const lifecycleRows = lifecycleRes?.rows || [];

      if (activeFilters?.symbol) {
        const normalized = String(activeFilters.symbol).trim().toUpperCase();
        setOpenTrades(openRows.filter((row) => String(row?.symbol || "").trim().toUpperCase() === normalized));
        setLifecycle(
          lifecycleRows.filter((row) => String(row?.symbol || "").trim().toUpperCase() === normalized)
        );
      } else {
        setOpenTrades(openRows);
        setLifecycle(lifecycleRows);
      }

      setSummary(summaryRes || null);
      setLatestScanSummary(latestScanRes || null);
      setSectionErrors((prev) => ({ ...prev, overview: null, sizing: null }));
    } catch (sectionErr) {
      const message = sectionErr?.message || "Failed to load dashboard overview";
      setSectionErrors((prev) => ({
        ...prev,
        overview: message,
        sizing: sectionErr?.message || "Failed to load sizing summary",
      }));
      throw sectionErr;
    } finally {
      setSectionLoading((prev) => ({ ...prev, overview: false, sizing: false }));
    }
  }, []);

  const loadAttemptsSection = useCallback(async () => {
    try {
      setSectionLoading((prev) => ({ ...prev, attempts: true }));
      const [opsRes, rejectionRes, dailyRes, hourlyRes] = await Promise.all([
        fetchOpsSummary(),
        fetchPaperTradeAttemptRejections(12),
        fetchPaperTradeAttemptDailySummary(7),
        fetchPaperTradeAttemptHourlySummary(7),
      ]);

      setOpsSummary(opsRes || null);
      setPaperTradeAttemptRejections(Array.isArray(rejectionRes?.rows) ? rejectionRes.rows : []);
      setPaperTradeAttemptDailySummary(Array.isArray(dailyRes?.rows) ? dailyRes.rows : []);
      setPaperTradeAttemptHourlySummary(Array.isArray(hourlyRes?.rows) ? hourlyRes.rows : []);
      setSectionErrors((prev) => ({ ...prev, attempts: null }));
    } catch (sectionErr) {
      setSectionErrors((prev) => ({
        ...prev,
        attempts: sectionErr?.message || "Failed to load execution attempt analytics",
      }));
      throw sectionErr;
    } finally {
      setSectionLoading((prev) => ({ ...prev, attempts: false }));
    }
  }, []);

  const loadReconciliationSection = useCallback(async () => {
    try {
      setSectionLoading((prev) => ({ ...prev, reconciliation: true }));
      const [reconRes, reconDetailsRes, reconHistoryRes, alpacaRes] = await Promise.all([
        fetchReconciliationSummary(),
        fetchReconciliationDetails(100),
        fetchReconciliationHistory(20),
        fetchAlpacaOpenPositions(),
      ]);

      setReconciliationSummary(reconRes || null);
      setReconciliationDetails(Array.isArray(reconDetailsRes?.rows) ? reconDetailsRes.rows : []);
      setReconciliationHistory(Array.isArray(reconHistoryRes?.rows) ? reconHistoryRes.rows : []);
      setLastReconciliationStatus(reconRes?.severity || null);
      setLastReconciliationAt(new Date().toISOString());
      setAlpacaOpenCount(alpacaRes?.count ?? null);
      setSectionErrors((prev) => ({ ...prev, reconciliation: null }));
    } catch (sectionErr) {
      setSectionErrors((prev) => ({
        ...prev,
        reconciliation: sectionErr?.message || "Failed to load reconciliation data",
      }));
      throw sectionErr;
    } finally {
      setSectionLoading((prev) => ({ ...prev, reconciliation: false }));
    }
  }, []);

  const loadRiskSection = useCallback(async () => {
    try {
      setSectionLoading((prev) => ({ ...prev, risk: true }));
      const riskRes = await fetchRiskExposureSummary();
      setRiskExposureSummary(riskRes || null);
      setSectionErrors((prev) => ({ ...prev, risk: null }));
    } catch (sectionErr) {
      setSectionErrors((prev) => ({
        ...prev,
        risk: sectionErr?.message || "Failed to load risk exposure data",
      }));
      throw sectionErr;
    } finally {
      setSectionLoading((prev) => ({ ...prev, risk: false }));
    }
  }, []);

  const loadAlpacaLogsSection = useCallback(async () => {
    try {
      setSectionLoading((prev) => ({ ...prev, alpacaLogs: true }));
      const [alpacaLogsRes, alpacaErrorsRes] = await Promise.all([
        fetchAlpacaApiLogs(20),
        fetchAlpacaApiErrors(20),
      ]);
      setAlpacaApiLogs(Array.isArray(alpacaLogsRes?.rows) ? alpacaLogsRes.rows : []);
      setAlpacaApiErrors(Array.isArray(alpacaErrorsRes?.rows) ? alpacaErrorsRes.rows : []);
      setSectionErrors((prev) => ({ ...prev, alpacaLogs: null }));
    } catch (sectionErr) {
      setSectionErrors((prev) => ({
        ...prev,
        alpacaLogs: sectionErr?.message || "Failed to load Alpaca API logs",
      }));
      throw sectionErr;
    } finally {
      setSectionLoading((prev) => ({ ...prev, alpacaLogs: false }));
    }
  }, []);

  const refreshByView = useCallback(async (view = activeView, activeFilters = filtersRef.current) => {
    const tasks = [];

    if (view === "overview") {
      tasks.push(loadOverviewSection(activeFilters), loadAttemptsSection(), loadReconciliationSection(), loadRiskSection());
    } else if (view === "trades") {
      tasks.push(loadOverviewSection(activeFilters), loadRiskSection());
    } else if (view === "reconciliation") {
      tasks.push(loadReconciliationSection());
    } else if (view === "broker") {
      tasks.push(loadAlpacaLogsSection());
    } else if (view === "analytics") {
      tasks.push(loadOverviewSection(activeFilters), loadAttemptsSection());
    } else {
      tasks.push(
        loadOverviewSection(activeFilters),
        loadAttemptsSection(),
        loadReconciliationSection(),
        loadRiskSection(),
        loadAlpacaLogsSection()
      );
    }

    await Promise.all(tasks);
  }, [activeView, loadAlpacaLogsSection, loadAttemptsSection, loadOverviewSection, loadReconciliationSection, loadRiskSection]);

  const loadData = useCallback(async (activeFilters = filtersRef.current) => {
    try {
      setLoading(true);
      setError(null);
      setSectionLoading(INITIAL_SECTION_LOADING);
      setSectionErrors(INITIAL_SECTION_ERRORS);

      await Promise.all([
        loadOverviewSection(activeFilters),
        loadAttemptsSection(),
        loadReconciliationSection(),
        loadRiskSection(),
        loadAlpacaLogsSection(),
      ]);

      setLastUpdated(new Date().toISOString());
      setNextRefreshAt(new Date(Date.now() + AUTO_REFRESH_INTERVAL_MS).toISOString());
    } catch (err) {
      setError(err.message || "Failed to load dashboard");
    } finally {
      setLoading(false);
    }
  }, [loadAlpacaLogsSection, loadAttemptsSection, loadOverviewSection, loadReconciliationSection, loadRiskSection]);

  useEffect(() => {
    filtersRef.current = filters;
  }, [filters]);

  useEffect(() => {
    loadData(filtersRef.current);
  }, [loadData]);

  useEffect(() => {
    const clockId = setInterval(() => {
      setCurrentTime(new Date());
    }, 60000);

    return () => clearInterval(clockId);
  }, []);

  useEffect(() => {
    if (loading) {
      return undefined;
    }

    refreshByView(activeView, filtersRef.current).catch(() => {});
    return undefined;
  }, [activeView, loading, refreshByView]);

  useEffect(() => {
    const marketSnapshot = getEasternMarketSnapshot(currentTime);
    if (!marketSnapshot.active) {
      setNextRefreshAt(null);
      return undefined;
    }

    if (!nextRefreshAt && lastUpdated) {
      setNextRefreshAt(new Date(new Date(lastUpdated).getTime() + AUTO_REFRESH_INTERVAL_MS).toISOString());
    }
    const intervalId = setInterval(() => {
      refreshByView(activeView, filtersRef.current).catch(() => {});
      setNextRefreshAt(new Date(Date.now() + AUTO_REFRESH_INTERVAL_MS).toISOString());
    }, AUTO_REFRESH_INTERVAL_MS);

    return () => clearInterval(intervalId);
  }, [activeView, currentTime, lastUpdated, nextRefreshAt, refreshByView]);

  useEffect(() => {
    if (!toast) return undefined;

    const timeoutId = setTimeout(() => {
      setToast(null);
    }, 3000);

    return () => clearTimeout(timeoutId);
  }, [toast]);

  function handleApplyFilters(nextFilters) {
    const appliedFilters = nextFilters || { date: "", symbol: "" };
    setFilters(appliedFilters);
    loadData(appliedFilters);
    setReconciliationSymbolFilter("");
  }

  async function refreshData() {
    try {
      setIsRefreshing(true);
      await refreshByView(activeView, filtersRef.current);
      setLastUpdated(new Date().toISOString());
      setNextRefreshAt(new Date(Date.now() + AUTO_REFRESH_INTERVAL_MS).toISOString());
    } finally {
      setIsRefreshing(false);
    }
  }

  async function rerunReconciliation() {
    try {
      setIsRefreshing(true);
      const data = await runReconciliationNow();

      if (data?.ok) {
        const nextSeverity = data?.result?.severity || data?.severity || "OK";
        setToast({ type: "success", message: "Reconciliation completed successfully" });
        setLastReconciliationStatus(nextSeverity);
        setLastReconciliationAt(new Date().toISOString());
      } else {
        setToast({
          type: "error",
          message: `Reconciliation failed: ${data?.error || "Unknown error"}`,
        });
        setLastReconciliationStatus("FAILED");
        setLastReconciliationAt(new Date().toISOString());
      }

      await Promise.all([loadReconciliationSection(), loadOverviewSection(filtersRef.current)]);
      setLastUpdated(new Date().toISOString());
    } finally {
      setIsRefreshing(false);
    }
  }

  async function syncPaperTrades() {
    try {
      setIsRunningSync(true);
      const data = await runSyncPaperTrades();

      if (data?.ok) {
        setToast({ type: "success", message: "Paper trade sync completed successfully" });
      } else {
        setToast({
          type: "error",
          message: `Paper trade sync failed: ${data?.error || "Unknown error"}`,
        });
      }

      await Promise.all([
        loadOverviewSection(filtersRef.current),
        loadRiskSection(),
        loadAlpacaLogsSection(),
      ]);
      setLastUpdated(new Date().toISOString());
    } finally {
      setIsRunningSync(false);
    }
  }

  const symbolPerformance = summary?.symbol_performance || [];
  const modePerformance = summary?.mode_performance || [];
  const hourlyPerformance = summary?.hourly_performance || [];
  const equityCurve = summary?.equity_curve || [];
  const insights = summary?.insights || {};

  const reconciliationSymbols = Array.from(
    new Set(
      (reconciliationDetails || [])
        .map((row) => String(row?.symbol || "").trim().toUpperCase())
        .filter(Boolean)
    )
  ).sort();

  const filteredReconciliationDetails = reconciliationSymbolFilter
    ? (reconciliationDetails || []).filter(
        (row) => String(row?.symbol || "").trim().toUpperCase() === reconciliationSymbolFilter
      )
    : reconciliationDetails || [];

  const mismatch = reconciliationSummary?.mismatch_count ?? null;
  const mismatchLabel = reconciliationSummary?.severity ?? "-";

  const backendHealthStatus = sectionErrors.overview
    ? "DEGRADED"
    : sectionErrors.reconciliation || sectionErrors.risk || sectionErrors.alpacaLogs
      ? "WARNING"
      : "OK";

  const syncHealthStatus = isRunningSync
    ? "RUNNING"
    : alpacaApiErrors.length > 0
      ? "ERRORS_PRESENT"
      : "HEALTHY";

  const reconciliationHealthStatus = lastReconciliationStatus || "-";
  const latestScanData = latestScanSummary?.summary || latestScanSummary || {};
  const confidenceMultiplier =
    latestScanData?.confidence_multiplier ??
    latestScanData?.sizing_confidence_multiplier ??
    latestScanData?.scan_confidence_multiplier ??
    null;
  const lossMultiplier =
    latestScanData?.loss_multiplier ??
    latestScanData?.sizing_loss_multiplier ??
    latestScanData?.scan_loss_multiplier ??
    null;
  const finalSizingMultiplier =
    latestScanData?.final_sizing_multiplier ??
    latestScanData?.sizing_multiplier ??
    latestScanData?.final_multiplier ??
    null;
  const multiplierStatus =
    confidenceMultiplier !== null || lossMultiplier !== null || finalSizingMultiplier !== null
      ? "EXPOSED"
      : "NOT_EXPOSED";
  const autoRefreshSnapshot = getEasternMarketSnapshot(currentTime);
  const autoRefreshActive = autoRefreshSnapshot.active;
  const refreshWindowLabel = "Weekdays 9:30 AM to 5:00 PM ET";
  const attentionItems = [];

  if ((reconciliationSummary?.mismatch_count ?? 0) > 0) {
    attentionItems.push({
      id: "reconciliation",
      severity: reconciliationSummary?.severity === "CRITICAL" ? "critical" : "warning",
      title: "Reconciliation mismatch detected",
      detail: `${reconciliationSummary?.mismatch_count ?? 0} mismatch row(s) need review.`,
    });
  }

  if (alpacaApiErrors.length > 0) {
    attentionItems.push({
      id: "alpaca-errors",
      severity: "critical",
      title: "Recent Alpaca API failures",
      detail: `${alpacaApiErrors.length} recent broker error call(s) were recorded.`,
    });
  }

  if (
    riskExposureSummary &&
    (riskExposureSummary.daily_realized_pnl ?? 0) + (riskExposureSummary.daily_unrealized_pnl ?? 0) <=
      -0.02 * (riskExposureSummary.account_size ?? 0)
  ) {
    attentionItems.push({
      id: "risk-guardrail",
      severity: "critical",
      title: "Daily risk guardrail is blocking trading",
      detail: "Realized plus unrealized P&L has crossed the daily cutoff.",
    });
  }

  const topAttemptReasons = Array.isArray(opsSummary?.paper_trade_attempt_top_reasons)
    ? opsSummary.paper_trade_attempt_top_reasons
    : [];
  const stageCounts = Array.isArray(opsSummary?.paper_trade_attempt_stage_counts)
    ? opsSummary.paper_trade_attempt_stage_counts
    : [];
  const attemptHourlySummary = Array.isArray(paperTradeAttemptHourlySummary) && paperTradeAttemptHourlySummary.length > 0
    ? paperTradeAttemptHourlySummary
    : Array.isArray(opsSummary?.paper_trade_attempt_hourly_summary)
      ? opsSummary.paper_trade_attempt_hourly_summary
      : [];
  const placedCount = stageCounts.find((row) => row.decision_stage === "PLACED")?.count ?? 0;
  const rejectedCount = stageCounts
    .filter((row) => row.decision_stage !== "PLACED" && row.decision_stage !== "PAPER_CANDIDATE")
    .reduce((total, row) => total + Number(row.count || 0), 0);
  const totalResolvedAttempts = placedCount + rejectedCount;
  const paperTradePlacementRate = totalResolvedAttempts > 0 ? (placedCount / totalResolvedAttempts) * 100 : null;

  if ((topAttemptReasons[0]?.count ?? 0) > 0) {
    attentionItems.push({
      id: "top-reason",
      severity: "warning",
      title: "Most common non-placement reason",
      detail: `${topAttemptReasons[0].final_reason} (${topAttemptReasons[0].count})`,
    });
  }

  if (!autoRefreshActive) {
    attentionItems.push({
      id: "refresh-window",
      severity: "info",
      title: "Auto-refresh is paused",
      detail: `Polling resumes during ${refreshWindowLabel}. Current market clock: ${autoRefreshSnapshot.label}.`,
    });
  }

  return {
    summary,
    openTrades,
    lifecycle,
    loading,
    error,
    sectionLoading,
    sectionErrors,
    filters,
    alpacaOpenCount,
    reconciliationSummary,
    reconciliationDetails,
    reconciliationSymbolFilter,
    setReconciliationSymbolFilter,
    reconciliationHistory,
    riskExposureSummary,
    alpacaApiLogs,
    alpacaApiErrors,
    opsSummary,
    paperTradeAttemptRejections,
    paperTradeAttemptDailySummary,
    paperTradeAttemptHourlySummary: attemptHourlySummary,
    topAttemptReasons,
    stageCounts,
    paperTradePlacementRate,
    attentionItems,
    latestScanSummary,
    lastUpdated,
    nextRefreshAt,
    autoRefreshActive,
    refreshWindowLabel,
    autoRefreshMarketTime: autoRefreshSnapshot.label,
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
  };
}
