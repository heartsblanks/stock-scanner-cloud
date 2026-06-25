import { useCallback, useEffect, useRef, useState } from "react";
import {
  fetchDashboardDaily,
  fetchOpenTrades,
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
  const abortRef = useRef(null);
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

  useEffect(() => {
    filtersRef.current = filters;
  }, [filters]);

  useEffect(() => {
    loadDailyDashboard(filtersRef.current);
    return () => abortRef.current?.abort();
  }, [loadDailyDashboard]);

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
  };
}
