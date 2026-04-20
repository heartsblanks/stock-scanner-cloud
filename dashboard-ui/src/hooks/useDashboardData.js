import { useCallback, useEffect, useRef, useState } from "react";
import {
  fetchDashboardSummary,
  fetchOpenTrades,
  fetchTradeLifecycle,
  fetchReconciliationSummary,
  fetchReconciliationDetails,
  fetchReconciliationHistory,
  fetchRiskExposureSummary,
  fetchLatestScanSummary,
  fetchOpsSummary,
  fetchPaperTradeAttemptDailySummary,
  fetchPaperTradeAttemptHourlySummary,
  fetchPaperTradeAttemptRecent,
  fetchPaperTradeAttemptRejections,
  runReconciliationNow,
  runSyncPaperTrades,
} from "../api/dashboard";

const AUTO_REFRESH_INTERVAL_MS = 30 * 60 * 1000;
const DASHBOARD_POLLING_WINDOW_START_MINUTES = 9 * 60 + 35;
const DASHBOARD_POLLING_WINDOW_END_MINUTES = 16 * 60 + 30;
const IBKR_OPEN_TRADES_PAGE_SIZE = 120;
const IBKR_LIFECYCLE_PAGE_SIZE = 180;
const IBKR_DASHBOARD_LIVE_CALLS_ENABLED = false;
const IBKR_DB_ONLY_STATUS = {
  ok: true,
  enabled: false,
  state: "DB_ONLY",
  login_required: false,
  message: "Dashboard live IBKR checks are disabled. Values shown are database-derived.",
};

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
  const active =
    isWeekday &&
    totalMinutes >= DASHBOARD_POLLING_WINDOW_START_MINUTES &&
    totalMinutes <= DASHBOARD_POLLING_WINDOW_END_MINUTES;

  return {
    active,
    weekday,
    totalMinutes,
    label: `${lookup.hour || "00"}:${lookup.minute || "00"} ET`,
  };
}

const INITIAL_SECTION_ERRORS = {
  overview: null,
  reconciliation: null,
  risk: null,
  sizing: null,
  attempts: null,
  ibkr: null,
};

function getInitialSectionLoading(activeView) {
  return {
    overview: activeView === "overview" || activeView === "trades" || activeView === "analytics",
    reconciliation: activeView === "overview" || activeView === "reconciliation",
    risk: activeView === "overview" || activeView === "trades",
    sizing: activeView === "overview" || activeView === "trades" || activeView === "analytics",
    attempts: activeView === "overview" || activeView === "analytics",
    ibkr: activeView === "overview" || activeView === "analytics",
  };
}

export function useDashboardData(activeView = "overview") {
  const [summary, setSummary] = useState(null);
  const [openTrades, setOpenTrades] = useState([]);
  const [lifecycle, setLifecycle] = useState([]);
  const [ibkrOpenTrades, setIbkrOpenTrades] = useState([]);
  const [ibkrLifecycle, setIbkrLifecycle] = useState([]);
  const [ibkrOpenTradesHasMore, setIbkrOpenTradesHasMore] = useState(false);
  const [ibkrLifecycleHasMore, setIbkrLifecycleHasMore] = useState(false);
  const [ibkrOpenTradesCursorTs, setIbkrOpenTradesCursorTs] = useState(null);
  const [ibkrOpenTradesCursorId, setIbkrOpenTradesCursorId] = useState(null);
  const [ibkrLifecycleCursorTs, setIbkrLifecycleCursorTs] = useState(null);
  const [ibkrLifecycleCursorId, setIbkrLifecycleCursorId] = useState(null);
  const [isLoadingMoreIbkrOpenTrades, setIsLoadingMoreIbkrOpenTrades] = useState(false);
  const [isLoadingMoreIbkrLifecycle, setIsLoadingMoreIbkrLifecycle] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [sectionLoading, setSectionLoading] = useState(() => getInitialSectionLoading(activeView));
  const [sectionErrors, setSectionErrors] = useState(INITIAL_SECTION_ERRORS);
  const [filters, setFilters] = useState({ date: "" });
  const [reconciliationSummary, setReconciliationSummary] = useState(null);
  const [reconciliationDetails, setReconciliationDetails] = useState([]);
  const [reconciliationSymbolFilter, setReconciliationSymbolFilter] = useState("");
  const [reconciliationHistory, setReconciliationHistory] = useState([]);
  const [riskExposureSummary, setRiskExposureSummary] = useState(null);
  const [opsSummary, setOpsSummary] = useState(null);
  const [paperTradeAttemptRejections, setPaperTradeAttemptRejections] = useState([]);
  const [paperTradeAttemptDailySummary, setPaperTradeAttemptDailySummary] = useState([]);
  const [paperTradeAttemptHourlySummary, setPaperTradeAttemptHourlySummary] = useState([]);
  const [ibkrRecentAttempts, setIbkrRecentAttempts] = useState([]);
  const [ibkrStatus, setIbkrStatus] = useState(IBKR_DB_ONLY_STATUS);
  const [latestScanSummary, setLatestScanSummary] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [nextRefreshAt, setNextRefreshAt] = useState(null);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isRunningSync, setIsRunningSync] = useState(false);
  const [isRefreshingIbkrStatus, setIsRefreshingIbkrStatus] = useState(false);
  const [toast, setToast] = useState(null);
  const [lastReconciliationStatus, setLastReconciliationStatus] = useState(null);
  const [lastReconciliationAt, setLastReconciliationAt] = useState(null);
  const [currentTime, setCurrentTime] = useState(() => new Date());
  const filtersRef = useRef(filters);
  const loadDataRef = useRef(null);

  function mergeUniqueRows(existingRows = [], newRows = []) {
    const merged = [];
    const seen = new Set();
    [...existingRows, ...newRows].forEach((row) => {
      const stableKey =
        row?.id ??
        row?.trade_key ??
        `${row?.symbol || ""}:${row?.status || ""}:${row?.entry_time || row?.timestamp_utc || row?.created_at || ""}`;
      if (seen.has(stableKey)) {
        return;
      }
      seen.add(stableKey);
      merged.push(row);
    });
    return merged;
  }

  const loadOverviewSection = useCallback(async (activeFilters = filtersRef.current) => {
    try {
      setSectionLoading((prev) => ({ ...prev, overview: true, sizing: true }));
      const [
        summaryRes,
        ibkrOpenRes,
        ibkrLifecycleRes,
        latestScanRes,
      ] = await Promise.all([
        fetchDashboardSummary(activeFilters?.date || undefined, "IBKR"),
        fetchOpenTrades({ limit: IBKR_OPEN_TRADES_PAGE_SIZE, broker: "IBKR", enrichLive: false }),
        fetchTradeLifecycle({ limit: IBKR_LIFECYCLE_PAGE_SIZE, broker: "IBKR" }),
        fetchLatestScanSummary(),
      ]);

      const ibkrOpenRows = ibkrOpenRes?.rows || [];
      const ibkrLifecycleRows = ibkrLifecycleRes?.rows || [];

      setOpenTrades(ibkrOpenRows);
      setLifecycle(ibkrLifecycleRows);
      setIbkrOpenTrades(ibkrOpenRows);
      setIbkrLifecycle(ibkrLifecycleRows);
      setIbkrOpenTradesHasMore(Boolean(ibkrOpenRes?.has_more));
      setIbkrLifecycleHasMore(Boolean(ibkrLifecycleRes?.has_more));
      setIbkrOpenTradesCursorTs(ibkrOpenRes?.next_cursor_ts || null);
      setIbkrOpenTradesCursorId(ibkrOpenRes?.next_cursor_id ?? null);
      setIbkrLifecycleCursorTs(ibkrLifecycleRes?.next_cursor_ts || null);
      setIbkrLifecycleCursorId(ibkrLifecycleRes?.next_cursor_id ?? null);

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
      setSectionLoading((prev) => ({ ...prev, attempts: true, ibkr: true }));
      const [opsResult, rejectionResult, dailyResult, hourlyResult, ibkrRecentResult] = await Promise.allSettled([
        fetchOpsSummary(),
        fetchPaperTradeAttemptRejections(12),
        fetchPaperTradeAttemptDailySummary(7),
        fetchPaperTradeAttemptHourlySummary(7),
        fetchPaperTradeAttemptRecent(8, "IBKR"),
      ]);

      const criticalFailures = [rejectionResult, dailyResult, hourlyResult, ibkrRecentResult]
        .filter((result) => result.status === "rejected")
        .length;
      const analyticsUnavailable = criticalFailures === 4;

      const opsRes = opsResult.status === "fulfilled" ? opsResult.value : null;
      const rejectionRes = rejectionResult.status === "fulfilled" ? rejectionResult.value : null;
      const dailyRes = dailyResult.status === "fulfilled" ? dailyResult.value : null;
      const hourlyRes = hourlyResult.status === "fulfilled" ? hourlyResult.value : null;

      setOpsSummary(opsRes || null);
      setPaperTradeAttemptRejections(Array.isArray(rejectionRes?.rows) ? rejectionRes.rows : []);
      setPaperTradeAttemptDailySummary(Array.isArray(dailyRes?.rows) ? dailyRes.rows : []);
      setPaperTradeAttemptHourlySummary(Array.isArray(hourlyRes?.rows) ? hourlyRes.rows : []);

      if (ibkrRecentResult.status === "fulfilled") {
        setIbkrRecentAttempts(Array.isArray(ibkrRecentResult.value?.rows) ? ibkrRecentResult.value.rows : []);
      } else {
        setIbkrRecentAttempts([]);
      }

      setIbkrStatus(IBKR_DB_ONLY_STATUS);
      setSectionErrors((prev) => ({
        ...prev,
        attempts:
          analyticsUnavailable
            ? "Execution analytics are temporarily unavailable."
            : criticalFailures > 0
              ? "Some execution analytics are temporarily unavailable."
              : null,
        ibkr: null,
      }));
    } catch (sectionErr) {
      setOpsSummary(null);
      setPaperTradeAttemptRejections([]);
      setPaperTradeAttemptDailySummary([]);
      setPaperTradeAttemptHourlySummary([]);
      setIbkrRecentAttempts([]);
      setIbkrStatus(IBKR_DB_ONLY_STATUS);
      setSectionErrors((prev) => ({
        ...prev,
        attempts: sectionErr?.message || "Failed to load execution attempt analytics",
        ibkr: null,
      }));
    } finally {
      setSectionLoading((prev) => ({ ...prev, attempts: false, ibkr: false }));
    }
  }, []);

  const loadReconciliationSection = useCallback(async () => {
    try {
      setSectionLoading((prev) => ({ ...prev, reconciliation: true }));
      const [reconRes, reconDetailsRes, reconHistoryRes] = await Promise.all([
        fetchReconciliationSummary(),
        fetchReconciliationDetails(100),
        fetchReconciliationHistory(20),
      ]);

      setReconciliationSummary(reconRes || null);
      setReconciliationDetails(Array.isArray(reconDetailsRes?.rows) ? reconDetailsRes.rows : []);
      setReconciliationHistory(Array.isArray(reconHistoryRes?.rows) ? reconHistoryRes.rows : []);
      setLastReconciliationStatus(reconRes?.severity || null);
      setLastReconciliationAt(new Date().toISOString());
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

  const loadReconciliationOverviewSection = useCallback(async () => {
    try {
      setSectionLoading((prev) => ({ ...prev, reconciliation: true }));
      const reconRes = await fetchReconciliationSummary();

      setReconciliationSummary(reconRes || null);
      setLastReconciliationStatus(reconRes?.severity || null);
      setLastReconciliationAt(new Date().toISOString());
      setSectionErrors((prev) => ({ ...prev, reconciliation: null }));
    } catch (sectionErr) {
      setSectionErrors((prev) => ({
        ...prev,
        reconciliation: sectionErr?.message || "Failed to load reconciliation overview",
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

  const refreshByView = useCallback(async (view = activeView, activeFilters = filtersRef.current) => {
    const tasks = [];

    if (view === "overview") {
      tasks.push(
        loadOverviewSection(activeFilters),
        loadAttemptsSection(),
        loadReconciliationOverviewSection(),
        loadRiskSection()
      );
    } else if (view === "trades") {
      tasks.push(loadOverviewSection(activeFilters), loadRiskSection());
    } else if (view === "reconciliation") {
      tasks.push(loadReconciliationSection());
    } else if (view === "analytics") {
      tasks.push(loadOverviewSection(activeFilters), loadAttemptsSection());
    } else {
      tasks.push(
        loadOverviewSection(activeFilters),
        loadAttemptsSection(),
        loadReconciliationOverviewSection(),
        loadRiskSection()
      );
    }

    await Promise.allSettled(tasks);
  }, [
    activeView,
    loadAttemptsSection,
    loadOverviewSection,
    loadReconciliationOverviewSection,
    loadReconciliationSection,
    loadRiskSection,
  ]);

  const loadInitialViewData = useCallback(async (view = activeView, activeFilters = filtersRef.current) => {
    if (view === "overview") {
      await Promise.allSettled([
        loadOverviewSection(activeFilters),
        loadAttemptsSection(),
        loadReconciliationOverviewSection(),
        loadRiskSection(),
      ]);
      return;
    }

    if (view === "trades") {
      await Promise.allSettled([loadOverviewSection(activeFilters), loadRiskSection()]);
      return;
    }

    if (view === "reconciliation") {
      await loadReconciliationSection();
      return;
    }

    if (view === "analytics") {
      await Promise.allSettled([loadOverviewSection(activeFilters), loadAttemptsSection()]);
      return;
    }

    await refreshByView(view, activeFilters);
  }, [
    activeView,
    loadAttemptsSection,
    loadOverviewSection,
    loadReconciliationOverviewSection,
    loadReconciliationSection,
    loadRiskSection,
    refreshByView,
  ]);

  const loadData = useCallback(async (activeFilters = filtersRef.current) => {
    try {
      setLoading(true);
      setError(null);
      setSectionLoading(getInitialSectionLoading(activeView));
      setSectionErrors(INITIAL_SECTION_ERRORS);

      await loadInitialViewData(activeView, activeFilters);

      setLastUpdated(new Date().toISOString());
      setNextRefreshAt(new Date(Date.now() + AUTO_REFRESH_INTERVAL_MS).toISOString());
    } catch (err) {
      setError(err.message || "Failed to load dashboard");
    } finally {
      setLoading(false);
    }
  }, [activeView, loadInitialViewData]);

  useEffect(() => {
    loadDataRef.current = loadData;
  }, [loadData]);

  useEffect(() => {
    filtersRef.current = filters;
  }, [filters]);

  useEffect(() => {
    loadDataRef.current?.(filtersRef.current);
  }, []);

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

  function pushToast(nextToast) {
    setToast(nextToast);
  }

  function handleApplyFilters(nextFilters) {
    const appliedFilters = nextFilters || { date: "" };
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

  async function loadMoreIbkrOpenTrades() {
    if (!ibkrOpenTradesHasMore || !ibkrOpenTradesCursorTs || ibkrOpenTradesCursorId === null || ibkrOpenTradesCursorId === undefined) {
      return;
    }
    try {
      setIsLoadingMoreIbkrOpenTrades(true);
      const nextPage = await fetchOpenTrades({
        limit: IBKR_OPEN_TRADES_PAGE_SIZE,
        broker: "IBKR",
        enrichLive: false,
        cursorTs: ibkrOpenTradesCursorTs,
        cursorId: ibkrOpenTradesCursorId,
      });
      const newRows = Array.isArray(nextPage?.rows) ? nextPage.rows : [];
      setIbkrOpenTrades((current) => mergeUniqueRows(current, newRows));
      setOpenTrades((current) => mergeUniqueRows(current, newRows));
      setIbkrOpenTradesHasMore(Boolean(nextPage?.has_more));
      setIbkrOpenTradesCursorTs(nextPage?.next_cursor_ts || null);
      setIbkrOpenTradesCursorId(nextPage?.next_cursor_id ?? null);
    } catch (err) {
      setToast({ type: "error", message: err?.message || "Failed to load more IBKR open trades" });
    } finally {
      setIsLoadingMoreIbkrOpenTrades(false);
    }
  }

  async function loadMoreIbkrLifecycle() {
    if (!ibkrLifecycleHasMore || !ibkrLifecycleCursorTs || ibkrLifecycleCursorId === null || ibkrLifecycleCursorId === undefined) {
      return;
    }
    try {
      setIsLoadingMoreIbkrLifecycle(true);
      const nextPage = await fetchTradeLifecycle({
        limit: IBKR_LIFECYCLE_PAGE_SIZE,
        broker: "IBKR",
        cursorTs: ibkrLifecycleCursorTs,
        cursorId: ibkrLifecycleCursorId,
      });
      const newRows = Array.isArray(nextPage?.rows) ? nextPage.rows : [];
      setIbkrLifecycle((current) => mergeUniqueRows(current, newRows));
      setLifecycle((current) => mergeUniqueRows(current, newRows));
      setIbkrLifecycleHasMore(Boolean(nextPage?.has_more));
      setIbkrLifecycleCursorTs(nextPage?.next_cursor_ts || null);
      setIbkrLifecycleCursorId(nextPage?.next_cursor_id ?? null);
    } catch (err) {
      setToast({ type: "error", message: err?.message || "Failed to load more IBKR lifecycle rows" });
    } finally {
      setIsLoadingMoreIbkrLifecycle(false);
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

      await Promise.all([loadReconciliationSection(), loadOverviewSection(filtersRef.current), loadReconciliationOverviewSection()]);
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

      await Promise.all([loadOverviewSection(filtersRef.current), loadRiskSection()]);
      setLastUpdated(new Date().toISOString());
    } finally {
      setIsRunningSync(false);
    }
  }

  async function refreshIbkrStatusLive() {
    if (!IBKR_DASHBOARD_LIVE_CALLS_ENABLED) {
      setIbkrStatus(IBKR_DB_ONLY_STATUS);
      setSectionErrors((prev) => ({ ...prev, ibkr: null }));
      return IBKR_DB_ONLY_STATUS;
    }
    try {
      setIsRefreshingIbkrStatus(true);
      const data = await fetchIbkrStatus({ live: true, ttlSeconds: 15 });
      setIbkrStatus(data || null);
      setSectionErrors((prev) => ({ ...prev, ibkr: null }));
      setLastUpdated(new Date().toISOString());
    } catch (err) {
      setSectionErrors((prev) => ({
        ...prev,
        ibkr: err?.message || "Failed to refresh IBKR live status",
      }));
      throw err;
    } finally {
      setIsRefreshingIbkrStatus(false);
    }
  }

  const symbolPerformance = summary?.symbol_performance || [];
  const modePerformance = summary?.mode_performance || [];
  const hourlyPerformance = summary?.hourly_performance || [];
  const hourlyOutcomeQuality = summary?.hourly_outcome_quality || [];
  const equityCurve = summary?.equity_curve || [];
  const insights = summary?.insights || {};
  const strategyHourlyOutcomeQuality = summary?.strategy_hourly_outcome_quality || hourlyOutcomeQuality;
  const externalExitSummary = summary?.external_exit_summary || insights?.external_exit_summary || null;

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
    : sectionErrors.reconciliation || sectionErrors.risk || sectionErrors.ibkr
      ? "WARNING"
      : "OK";

  const syncHealthStatus = isRunningSync
    ? "RUNNING"
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
  const refreshWindowLabel = "Weekdays 9:35 AM to 4:30 PM ET";
  const attentionItems = [];

  if ((reconciliationSummary?.mismatch_count ?? 0) > 0) {
    attentionItems.push({
      id: "reconciliation",
      severity: reconciliationSummary?.severity === "CRITICAL" ? "critical" : "warning",
      title: "Reconciliation mismatch detected",
      detail: `${reconciliationSummary?.mismatch_count ?? 0} mismatch row(s) need review.`,
    });
  }

  if (ibkrStatus?.enabled && ibkrStatus?.login_required) {
    attentionItems.push({
      id: "ibkr-login",
      severity: "warning",
      title: "IBKR login required",
      detail: ibkrStatus?.message || "IBKR bridge is up, but the broker session is not ready.",
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

  return {
    summary,
    openTrades,
    lifecycle,
    ibkrOpenTrades,
    ibkrLifecycle,
    loading,
    error,
    sectionLoading,
    sectionErrors,
    filters,
    reconciliationSummary,
    reconciliationDetails,
    reconciliationSymbolFilter,
    setReconciliationSymbolFilter,
    reconciliationHistory,
    riskExposureSummary,
    opsSummary,
    paperTradeAttemptRejections,
    paperTradeAttemptDailySummary,
    paperTradeAttemptHourlySummary: attemptHourlySummary,
    ibkrRecentAttempts,
    ibkrStatus,
    ibkrLiveChecksEnabled: IBKR_DASHBOARD_LIVE_CALLS_ENABLED,
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
  };
}
