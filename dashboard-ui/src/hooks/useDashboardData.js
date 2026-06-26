import { useCallback, useEffect, useRef, useState } from "react";
import {
  fetchDashboardDaily,
  fetchDashboardSummary,
  fetchIbkrStatus,
  fetchOpenTrades,
  fetchOpsSummary,
  fetchPaperTradeAttemptDailySummary,
  fetchPaperTradeAttemptHourlySummary,
  fetchPaperTradeAttemptRecent,
  fetchPaperTradeAttemptRejections,
  fetchReconciliationSummary,
  fetchRiskExposureSummary,
  runSyncPaperTrades,
} from "../api/dashboard";

const AUTO_REFRESH_INTERVAL_MS = 5 * 60 * 1000;
const OPEN_TRADES_PAGE_SIZE = 80;

const INITIAL_DAILY = {
  realized_pnl: 0,
  unrealized_pnl: null,
  total_day_pnl: 0,
  open_position_count: 0,
  open_exposure: 0,
  placements_today: 0,
  placement_rate: null,
  latest_scan: null,
};

const INITIAL_SECTION_LOADING = {
  risk: false,
  attempts: false,
  scheduler: false,
  ibkr: false,
  reconciliation: false,
};

const INITIAL_SECTION_ERRORS = {
  risk: null,
  attempts: null,
  scheduler: null,
  ibkr: null,
  reconciliation: null,
};

function isCanceled(error) {
  return error?.name === "CanceledError" || error?.message === "canceled";
}

export function useDashboardData() {
  const [daily, setDaily] = useState(INITIAL_DAILY);
  const [openTrades, setOpenTrades] = useState([]);
  const [filters, setFilters] = useState({ date: "" });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [nextRefreshAt, setNextRefreshAt] = useState(null);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isRunningSync, setIsRunningSync] = useState(false);
  const [toast, setToast] = useState(null);

  // Section state
  const [sectionLoading, setSectionLoading] = useState(INITIAL_SECTION_LOADING);
  const [sectionErrors, setSectionErrors] = useState(INITIAL_SECTION_ERRORS);
  const [opsSummary, setOpsSummary] = useState(null);
  const [riskExposureSummary, setRiskExposureSummary] = useState(null);
  const [paperTradeAttempts, setPaperTradeAttempts] = useState(null);
  const [paperTradeAttemptRejections, setPaperTradeAttemptRejections] = useState(null);
  const [paperTradeAttemptDailySummary, setPaperTradeAttemptDailySummary] = useState(null);
  const [paperTradeAttemptHourlySummary, setPaperTradeAttemptHourlySummary] = useState(null);
  const [dashboardSummary, setDashboardSummary] = useState(null);
  const [reconciliationSummary, setReconciliationSummary] = useState(null);
  const [ibkrStatus, setIbkrStatus] = useState(null);

  const abortRef = useRef(null);
  const sectionAbortRef = useRef(null);
  const requestIdRef = useRef(0);
  const filtersRef = useRef(filters);

  const loadDailyDashboard = useCallback(async (activeFilters = filtersRef.current, { quiet = false } = {}) => {
    const requestId = requestIdRef.current + 1;
    requestIdRef.current = requestId;
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    if (!quiet) {
      setLoading(true);
    }
    setError(null);

    try {
      const [dailyRes, openRes] = await Promise.all([
        fetchDashboardDaily(activeFilters?.date || undefined, "IBKR", { signal: controller.signal }),
        fetchOpenTrades({
          limit: OPEN_TRADES_PAGE_SIZE,
          broker: "IBKR",
          enrichLive: false,
          signal: controller.signal,
        }),
      ]);

      if (requestId !== requestIdRef.current) {
        return;
      }

      setDaily(dailyRes || INITIAL_DAILY);
      setOpenTrades(Array.isArray(openRes?.rows) ? openRes.rows : []);
      setLastUpdated(new Date().toISOString());
      setNextRefreshAt(new Date(Date.now() + AUTO_REFRESH_INTERVAL_MS).toISOString());
    } catch (err) {
      if (!isCanceled(err)) {
        setError(err?.message || "Failed to load dashboard");
      }
    } finally {
      if (requestId === requestIdRef.current) {
        setLoading(false);
        setIsRefreshing(false);
      }
    }
  }, []);

  const loadSectionData = useCallback(async () => {
    sectionAbortRef.current?.abort();
    const controller = new AbortController();
    sectionAbortRef.current = controller;

    setSectionLoading({ risk: true, attempts: true, scheduler: true, ibkr: true, reconciliation: true });
    setSectionErrors(INITIAL_SECTION_ERRORS);

    const settled = await Promise.allSettled([
      fetchRiskExposureSummary(),
      fetchPaperTradeAttemptRecent(25, "IBKR"),
      fetchPaperTradeAttemptRejections(25),
      fetchPaperTradeAttemptDailySummary(7),
      fetchPaperTradeAttemptHourlySummary(7),
      fetchDashboardSummary(null, "IBKR"),
      fetchOpsSummary(),
      fetchReconciliationSummary(),
      fetchIbkrStatus(),
    ]);

    if (controller.signal.aborted) {
      return;
    }

    const [
      riskRes,
      attemptsRes,
      rejectionsRes,
      dailySummaryRes,
      hourlySummaryRes,
      dashboardSummaryRes,
      opsSummaryRes,
      reconcileRes,
      ibkrStatusRes,
    ] = settled;

    if (riskRes.status === "fulfilled") setRiskExposureSummary(riskRes.value);
    if (attemptsRes.status === "fulfilled") setPaperTradeAttempts(attemptsRes.value);
    if (rejectionsRes.status === "fulfilled") setPaperTradeAttemptRejections(rejectionsRes.value);
    if (dailySummaryRes.status === "fulfilled") setPaperTradeAttemptDailySummary(dailySummaryRes.value);
    if (hourlySummaryRes.status === "fulfilled") setPaperTradeAttemptHourlySummary(hourlySummaryRes.value);
    if (dashboardSummaryRes.status === "fulfilled") setDashboardSummary(dashboardSummaryRes.value);
    if (opsSummaryRes.status === "fulfilled") setOpsSummary(opsSummaryRes.value);
    if (reconcileRes.status === "fulfilled") setReconciliationSummary(reconcileRes.value);
    if (ibkrStatusRes.status === "fulfilled") setIbkrStatus(ibkrStatusRes.value);

    const attemptsGroupFailed =
      attemptsRes.status === "rejected" ||
      rejectionsRes.status === "rejected" ||
      dailySummaryRes.status === "rejected" ||
      hourlySummaryRes.status === "rejected" ||
      dashboardSummaryRes.status === "rejected";

    setSectionErrors({
      risk: riskRes.status === "rejected" ? (riskRes.reason?.message || "Failed to load risk exposure") : null,
      attempts: attemptsGroupFailed ? "Failed to load execution attempt data" : null,
      scheduler: opsSummaryRes.status === "rejected" ? (opsSummaryRes.reason?.message || "Failed to load ops summary") : null,
      ibkr: ibkrStatusRes.status === "rejected" ? (ibkrStatusRes.reason?.message || "Failed to load IBKR status") : null,
      reconciliation: reconcileRes.status === "rejected" ? (reconcileRes.reason?.message || "Failed to load reconciliation data") : null,
    });

    setSectionLoading({ risk: false, attempts: false, scheduler: false, ibkr: false, reconciliation: false });
  }, []);

  const retryRisk = useCallback(async () => {
    setSectionLoading((prev) => ({ ...prev, risk: true }));
    setSectionErrors((prev) => ({ ...prev, risk: null }));
    try {
      const result = await fetchRiskExposureSummary();
      setRiskExposureSummary(result);
    } catch (err) {
      setSectionErrors((prev) => ({ ...prev, risk: err?.message || "Failed to load risk exposure" }));
    } finally {
      setSectionLoading((prev) => ({ ...prev, risk: false }));
    }
  }, []);

  const retryScheduler = useCallback(async () => {
    setSectionLoading((prev) => ({ ...prev, scheduler: true, ibkr: true }));
    setSectionErrors((prev) => ({ ...prev, scheduler: null, ibkr: null }));
    try {
      const [opsResult, ibkrResult] = await Promise.allSettled([fetchOpsSummary(), fetchIbkrStatus()]);
      if (opsResult.status === "fulfilled") {
        setOpsSummary(opsResult.value);
      } else {
        setSectionErrors((prev) => ({ ...prev, scheduler: opsResult.reason?.message || "Failed to load ops summary" }));
      }
      if (ibkrResult.status === "fulfilled") {
        setIbkrStatus(ibkrResult.value);
      } else {
        setSectionErrors((prev) => ({ ...prev, ibkr: ibkrResult.reason?.message || "Failed to load IBKR status" }));
      }
    } finally {
      setSectionLoading((prev) => ({ ...prev, scheduler: false, ibkr: false }));
    }
  }, []);

  useEffect(() => {
    filtersRef.current = filters;
  }, [filters]);

  useEffect(() => {
    loadDailyDashboard(filtersRef.current);
    loadSectionData();
    return () => {
      abortRef.current?.abort();
      sectionAbortRef.current?.abort();
    };
  }, [loadDailyDashboard, loadSectionData]);

  useEffect(() => {
    const intervalId = setInterval(() => {
      loadDailyDashboard(filtersRef.current, { quiet: true });
    }, AUTO_REFRESH_INTERVAL_MS);
    return () => clearInterval(intervalId);
  }, [loadDailyDashboard]);

  useEffect(() => {
    if (!toast) {
      return undefined;
    }
    const timeoutId = setTimeout(() => setToast(null), 3000);
    return () => clearTimeout(timeoutId);
  }, [toast]);

  function handleApplyFilters(nextFilters) {
    const appliedFilters = nextFilters || { date: "" };
    setFilters(appliedFilters);
    loadDailyDashboard(appliedFilters);
  }

  async function refreshData() {
    setIsRefreshing(true);
    await loadDailyDashboard(filtersRef.current, { quiet: true });
  }

  async function syncPaperTrades() {
    try {
      setIsRunningSync(true);
      const data = await runSyncPaperTrades();
      setToast(
        data?.ok
          ? { type: "success", message: "Trades synced." }
          : { type: "error", message: data?.error || "Trade sync failed." }
      );
      await loadDailyDashboard(filtersRef.current, { quiet: true });
    } finally {
      setIsRunningSync(false);
    }
  }

  return {
    daily,
    openTrades,
    filters,
    loading,
    error,
    lastUpdated,
    nextRefreshAt,
    isRefreshing,
    isRunningSync,
    toast,
    handleApplyFilters,
    refreshData,
    syncPaperTrades,
    // Section state
    sectionLoading,
    sectionErrors,
    opsSummary,
    riskExposureSummary,
    paperTradeAttempts,
    paperTradeAttemptRejections,
    paperTradeAttemptDailySummary,
    paperTradeAttemptHourlySummary,
    dashboardSummary,
    reconciliationSummary,
    ibkrStatus,
    retryRisk,
    retryScheduler,
  };
}
