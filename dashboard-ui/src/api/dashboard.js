import apiClient from "./client";

export async function fetchDashboardSummary(date) {
  const response = await apiClient.get("/dashboard-summary", {
    params: date ? { date } : {},
  });

  const data = response.data || {};

  return {
    ...data,
    symbol_performance: data.symbol_performance || data.top_symbols || [],
    mode_performance: data.mode_performance || [],
    hourly_performance: data.hourly_performance || [],
    exit_reason_breakdown: data.exit_reason_breakdown || [],
    equity_curve: data.equity_curve || [],
  };
}

export async function fetchOpenTrades(limit = 100) {
  const response = await apiClient.get("/open-trades", {
    params: { limit },
  });
  return response.data;
}

export async function fetchClosedTrades(limit = 100) {
  const response = await apiClient.get("/closed-trades", {
    params: { limit },
  });
  return response.data;
}

export async function fetchTradeLifecycle(limit = 100, status) {
  const params = { limit };
  if (status) {
    params.status = status;
  }

  const response = await apiClient.get("/trade-lifecycle", { params });
  return response.data;
}

export async function fetchTradeLifecycleSummary(limit = 1000) {
  const response = await apiClient.get("/trade-lifecycle-summary", {
    params: { limit },
  });
  return response.data;
}

export async function fetchOpsSummary() {
  const response = await apiClient.get("/ops-summary");
  return response.data;
}

export async function fetchReconcileSummary() {
  const response = await apiClient.get("/reconcile-summary");
  return response.data;
}

export async function fetchReconciliationSummary() {
  return fetchReconcileSummary();
}

export async function fetchReconciliationMismatches(limit = 100, runId) {
  const params = { limit };
  if (runId !== undefined && runId !== null && runId !== "") {
    params.run_id = runId;
  }

  const response = await apiClient.get("/reconciliation-mismatches", { params });
  return response.data;
}

export async function fetchReconciliationDetails(limit = 100, runId) {
  const data = await fetchReconciliationMismatches(limit, runId);
  return {
    ...data,
    rows: data?.rows || [],
    count: data?.count || 0,
    limit: data?.limit || limit,
  };
}

export async function fetchReconciliationHistory(limit = 20) {
  const response = await apiClient.get("/reconciliation-runs", {
    params: { limit },
  });

  const data = response.data || {};
  return {
    ...data,
    rows: data?.rows || [],
    count: data?.count || 0,
    limit: data?.limit || limit,
  };
}

export async function fetchRiskExposureSummary() {
  const response = await apiClient.get("/risk-exposure-summary");
  const data = response.data || {};

  return {
    ...data,
    total_open_exposure: data?.total_open_exposure || 0,
    open_position_count: data?.open_position_count || 0,
    daily_realized_pnl: data?.daily_realized_pnl || 0,
    daily_unrealized_pnl: data?.daily_unrealized_pnl || 0,
    allocation_used_pct: data?.allocation_used_pct || 0,
    max_positions: data?.max_positions || 0,
  };
}

export async function fetchAlpacaApiLogs(limit = 100) {
  const response = await apiClient.get("/alpaca-api-logs/recent", {
    params: { limit },
  });
  return response.data;
}

export async function fetchAlpacaApiErrors(limit = 100) {
  const response = await apiClient.get("/alpaca-api-logs/errors", {
    params: { limit },
  });
  return response.data;
}

export async function fetchLatestScanSummary() {
  const response = await apiClient.get("/latest-scan-summary");
  return response.data;
}

export async function fetchAlpacaOpenPositions() {
  const response = await apiClient.get("/alpaca-open-positions");
  const data = response.data || {};

  return {
    count: data.count || 0,
    positions: data.positions || [],
  };
}
