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
    hourly_outcome_quality: data.hourly_outcome_quality || [],
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

export async function fetchPaperTradeAttemptRejections(limit = 25) {
  const response = await apiClient.get("/paper-trade-attempts/rejections", {
    params: { limit },
  });
  const data = response.data || {};
  return {
    ...data,
    rows: Array.isArray(data?.rows) ? data.rows : [],
    count: data?.count ?? (Array.isArray(data?.rows) ? data.rows.length : 0),
    limit: data?.limit || limit,
  };
}

export async function fetchPaperTradeAttemptDailySummary(limitDays = 7) {
  const response = await apiClient.get("/paper-trade-attempts/daily-summary", {
    params: { limit_days: limitDays },
  });
  const data = response.data || {};
  return {
    ...data,
    rows: Array.isArray(data?.rows) ? data.rows : [],
    count: data?.count ?? (Array.isArray(data?.rows) ? data.rows.length : 0),
    limit_days: data?.limit_days || limitDays,
  };
}

export async function fetchPaperTradeAttemptHourlySummary(limitDays = 7) {
  const response = await apiClient.get("/paper-trade-attempts/hourly-summary", {
    params: { limit_days: limitDays },
  });
  const data = response.data || {};
  return {
    ...data,
    rows: Array.isArray(data?.rows) ? data.rows : [],
    count: data?.count ?? (Array.isArray(data?.rows) ? data.rows.length : 0),
    limit_days: data?.limit_days || limitDays,
  };
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
  const summary = data.summary || {};

  return {
    ...summary,
    total_open_exposure: summary?.total_open_exposure || 0,
    open_position_count: summary?.open_position_count || 0,
    daily_realized_pnl: summary?.daily_realized_pnl || 0,
    daily_unrealized_pnl: summary?.daily_unrealized_pnl || 0,
    allocation_used_pct: summary?.allocation_used_pct || 0,
    max_positions: summary?.max_positions || 0,
    max_total_allocated_capital: summary?.max_total_allocated_capital || 0,
    max_capital_allocation_pct: summary?.max_capital_allocation_pct || 0,
    account_size: summary?.account_size || 0,
  };
}

export async function fetchAlpacaApiLogs(limit = 100) {
  const response = await apiClient.get("/alpaca-api-logs/recent", {
    params: { limit },
  });
  const data = response.data || {};

  return {
    ...data,
    rows: Array.isArray(data?.rows)
      ? data.rows
      : Array.isArray(data?.logs)
      ? data.logs
      : Array.isArray(data)
      ? data
      : [],
    count:
      data?.count ??
      (Array.isArray(data?.rows)
        ? data.rows.length
        : Array.isArray(data?.logs)
        ? data.logs.length
        : Array.isArray(data)
        ? data.length
        : 0),
    limit: data?.limit || limit,
  };
}

export async function fetchAlpacaApiErrors(limit = 100) {
  const response = await apiClient.get("/alpaca-api-logs/errors", {
    params: { limit },
  });
  const data = response.data || {};

  return {
    ...data,
    rows: Array.isArray(data?.rows)
      ? data.rows
      : Array.isArray(data?.errors)
      ? data.errors
      : Array.isArray(data)
      ? data
      : [],
    count:
      data?.count ??
      (Array.isArray(data?.rows)
        ? data.rows.length
        : Array.isArray(data?.errors)
        ? data.errors.length
        : Array.isArray(data)
        ? data.length
        : 0),
    limit: data?.limit || limit,
  };
}

export async function fetchLatestScanSummary() {
  const response = await apiClient.get("/latest-scan-summary");
  const data = response.data || {};
  const summary = data?.summary || data;

  return {
    ...data,
    summary,
    confidence_multiplier:
      summary?.confidence_multiplier ??
      summary?.sizing_confidence_multiplier ??
      summary?.scan_confidence_multiplier ??
      null,
    loss_multiplier:
      summary?.loss_multiplier ??
      summary?.sizing_loss_multiplier ??
      summary?.scan_loss_multiplier ??
      null,
    final_sizing_multiplier:
      summary?.final_sizing_multiplier ??
      summary?.sizing_multiplier ??
      summary?.final_multiplier ??
      null,
  };
}

export async function fetchAlpacaOpenPositions() {
  const response = await apiClient.get("/alpaca-open-positions");
  const data = response.data || {};

  return {
    count: data.count || 0,
    positions: data.positions || [],
  };
}

export async function runReconciliationNow() {
  const response = await apiClient.post("/reconcile-now");
  return response.data || {};
}

export async function runSyncPaperTrades() {
  const response = await apiClient.post("/sync-paper-trades");
  return response.data || {};
}
