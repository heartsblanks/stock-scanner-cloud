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

export async function fetchReconciliationMismatches(limit = 100, runId) {
  const params = { limit };
  if (runId !== undefined && runId !== null && runId !== "") {
    params.run_id = runId;
  }

  const response = await apiClient.get("/reconciliation-mismatches", { params });
  return response.data;
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
