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
} from "../api/dashboard";

const INITIAL_SECTION_LOADING = {
  overview: true,
  reconciliation: true,
  risk: true,
  alpacaLogs: true,
  sizing: true,
};

const INITIAL_SECTION_ERRORS = {
  overview: null,
  reconciliation: null,
  risk: null,
  alpacaLogs: null,
  sizing: null,
};

export function useDashboardData() {
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
  const [latestScanSummary, setLatestScanSummary] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isRunningSync, setIsRunningSync] = useState(false);
  const [toast, setToast] = useState(null);
  const [lastReconciliationStatus, setLastReconciliationStatus] = useState(null);
  const [lastReconciliationAt, setLastReconciliationAt] = useState(null);
  const filtersRef = useRef(filters);

  const loadData = useCallback(async (activeFilters = filtersRef.current) => {
    try {
      setLoading(true);
      setError(null);
      setSectionLoading(INITIAL_SECTION_LOADING);
      setSectionErrors(INITIAL_SECTION_ERRORS);

      try {
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
          setOpenTrades(
            openRows.filter((row) => String(row?.symbol || "").trim().toUpperCase() === normalized)
          );
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
      } finally {
        setSectionLoading((prev) => ({ ...prev, overview: false, sizing: false }));
      }

      try {
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
      } finally {
        setSectionLoading((prev) => ({ ...prev, reconciliation: false }));
      }

      try {
        const riskRes = await fetchRiskExposureSummary();
        setRiskExposureSummary(riskRes || null);
        setSectionErrors((prev) => ({ ...prev, risk: null }));
      } catch (sectionErr) {
        setSectionErrors((prev) => ({
          ...prev,
          risk: sectionErr?.message || "Failed to load risk exposure data",
        }));
      } finally {
        setSectionLoading((prev) => ({ ...prev, risk: false }));
      }

      try {
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
      } finally {
        setSectionLoading((prev) => ({ ...prev, alpacaLogs: false }));
      }

      setLastUpdated(new Date().toISOString());
    } catch (err) {
      setError(err.message || "Failed to load dashboard");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    filtersRef.current = filters;
  }, [filters]);

  useEffect(() => {
    loadData(filtersRef.current);

    const intervalId = setInterval(() => {
      loadData(filtersRef.current);
    }, 900000);

    return () => clearInterval(intervalId);
  }, [loadData]);

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
      await loadData(filters);
    } finally {
      setIsRefreshing(false);
    }
  }

  async function rerunReconciliation() {
    try {
      setIsRefreshing(true);
      const res = await fetch("/reconcile-now", { method: "POST" });
      const data = await res.json();

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

      await loadData(filters);
    } finally {
      setIsRefreshing(false);
    }
  }

  async function syncPaperTrades() {
    try {
      setIsRunningSync(true);
      const res = await fetch("/sync-paper-trades", { method: "POST" });
      const data = await res.json();

      if (data?.ok) {
        setToast({ type: "success", message: "Paper trade sync completed successfully" });
      } else {
        setToast({
          type: "error",
          message: `Paper trade sync failed: ${data?.error || "Unknown error"}`,
        });
      }

      await loadData(filters);
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
    latestScanSummary,
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
  };
}
