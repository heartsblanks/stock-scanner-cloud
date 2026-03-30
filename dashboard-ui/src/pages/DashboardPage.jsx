import { useEffect, useRef, useState } from "react";
import {
  fetchDashboardSummary,
  fetchOpenTrades,
  fetchTradeLifecycle,
  fetchReconciliationSummary,
  fetchReconciliationDetails,
  fetchReconciliationHistory,
  fetchAlpacaOpenPositions,
} from "../api/dashboard";

import SummaryCards from "../components/SummaryCards";
import OpenTradesTable from "../components/OpenTradesTable";
import TradeLifecycleTable from "../components/TradeLifecycleTable";
import HourlyPerformanceChart from "../components/HourlyPerformanceChart";
import SymbolPerformanceChart from "../components/SymbolPerformanceChart";
import ModePerformanceChart from "../components/ModePerformanceChart";
import DashboardFilters from "../components/DashboardFilters";
import EquityCurveChart from "../components/EquityCurveChart";
import ReconciliationDetailsTable from "../components/ReconciliationDetailsTable";
import ReconciliationHistoryTable from "../components/ReconciliationHistoryTable";

export default function DashboardPage() {
  const [summary, setSummary] = useState(null);
  const [openTrades, setOpenTrades] = useState([]);
  const [lifecycle, setLifecycle] = useState([]);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [filters, setFilters] = useState({ date: "", symbol: "" });

  const [alpacaOpenCount, setAlpacaOpenCount] = useState(null);
  const [reconciliationSummary, setReconciliationSummary] = useState(null);
  const [reconciliationDetails, setReconciliationDetails] = useState([]);
  const [reconciliationHistory, setReconciliationHistory] = useState([]);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [toast, setToast] = useState(null);
  const [lastReconciliationStatus, setLastReconciliationStatus] = useState(null);
  const [lastReconciliationAt, setLastReconciliationAt] = useState(null);
  const filtersRef = useRef(filters);

  async function loadData(activeFilters = filters) {
    try {
      setLoading(true);
      setError(null);

      const [summaryRes, openRes, lifecycleRes] = await Promise.all([
        fetchDashboardSummary(activeFilters?.date || undefined),
        fetchOpenTrades(100),
        fetchTradeLifecycle(200),
      ]);

      const openRows = openRes?.rows || [];
      const lifecycleRows = lifecycleRes?.rows || [];

      if (activeFilters?.symbol) {
        const normalized = String(activeFilters.symbol).trim().toUpperCase();
        setOpenTrades(
          openRows.filter(
            (row) => String(row?.symbol || "").trim().toUpperCase() === normalized
          )
        );
        setLifecycle(
          lifecycleRows.filter(
            (row) => String(row?.symbol || "").trim().toUpperCase() === normalized
          )
        );
      } else {
        setOpenTrades(openRows);
        setLifecycle(lifecycleRows);
      }

      setSummary(summaryRes || null);

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
      setLastUpdated(new Date().toISOString());
    } catch (err) {
      setError(err.message || "Failed to load dashboard");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    filtersRef.current = filters;
  }, [filters]);

  useEffect(() => {
    loadData(filters);

    const intervalId = setInterval(() => {
      loadData(filtersRef.current);
    }, 900000);

    return () => clearInterval(intervalId);
  }, []);

  useEffect(() => {
    if (!toast) return;

    const timeoutId = setTimeout(() => {
      setToast(null);
    }, 3000);

    return () => clearTimeout(timeoutId);
  }, [toast]);

  const symbolPerformance = summary?.symbol_performance || [];
  const modePerformance = summary?.mode_performance || [];
  const hourlyPerformance = summary?.hourly_performance || [];
  const equityCurve = summary?.equity_curve || [];
  const insights = summary?.insights || {};

  function handleApplyFilters(nextFilters) {
    const appliedFilters = nextFilters || { date: "", symbol: "" };
    setFilters(appliedFilters);
    loadData(appliedFilters);
  }

  const mismatch = reconciliationSummary?.mismatch_count ?? null;
  const mismatchLabel = reconciliationSummary?.severity ?? "-";

  if (loading) {
    return <div style={{ padding: 20 }}>Loading dashboard...</div>;
  }

  if (error) {
    return (
      <div style={{ padding: 20, color: "red" }}>
        Error: {error}
      </div>
    );
  }

  return (
    <div style={{ padding: 20, background: "#f5f6fa", minHeight: "100vh" }}>
      <h1 style={{ marginBottom: 10 }}>Trading Dashboard</h1>

      {toast && (
        <div
          style={{
            marginBottom: 12,
            padding: "10px 14px",
            borderRadius: 8,
            background: toast.type === "success" ? "#dcfce7" : "#fee2e2",
            color: toast.type === "success" ? "#166534" : "#991b1b",
            border: `1px solid ${toast.type === "success" ? "#86efac" : "#fca5a5"}`,
          }}
        >
          {toast.message}
        </div>
      )}

      <DashboardFilters onApply={handleApplyFilters} />

      <div style={{ marginTop: 10, marginBottom: 10, display: "flex", gap: 10 }}>
        <button
          onClick={async () => {
            try {
              setIsRefreshing(true);
              await loadData(filters);
            } finally {
              setIsRefreshing(false);
            }
          }}
          disabled={isRefreshing}
          style={{
            padding: "8px 16px",
            background: isRefreshing ? "#93c5fd" : "#2563eb",
            color: "#fff",
            border: "none",
            borderRadius: 6,
            cursor: isRefreshing ? "not-allowed" : "pointer",
            fontWeight: 500,
            opacity: isRefreshing ? 0.8 : 1,
          }}
        >
          {isRefreshing ? "Refreshing..." : "Refresh Data"}
        </button>

        <button
          onClick={async () => {
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
                  message: "Reconciliation failed: " + (data?.error || "Unknown error"),
                });
                setLastReconciliationStatus("FAILED");
                setLastReconciliationAt(new Date().toISOString());
              }
              await loadData(filters);
            } finally {
              setIsRefreshing(false);
            }
          }}
          disabled={isRefreshing}
          style={{
            padding: "8px 16px",
            background: "#16a34a",
            color: "#fff",
            border: "none",
            borderRadius: 6,
            cursor: isRefreshing ? "not-allowed" : "pointer",
            fontWeight: 500,
            opacity: isRefreshing ? 0.8 : 1,
          }}
        >
          Re-run Reconciliation
        </button>
      </div>

      <SummaryCards data={summary} />

      <div style={{ marginTop: 30 }}>
        <h2>System Health</h2>
        {lastUpdated && (
          <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 8 }}>
            Last updated: {new Date(lastUpdated).toLocaleString()}
          </div>
        )}
        <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
          <InsightCard
            title="Open Trades (DB)"
            value={openTrades.length}
          />
          <InsightCard
            title="Open Positions (Alpaca)"
            value={alpacaOpenCount ?? "-"}
          />
          <InsightCard
            title="Mismatch"
            value={mismatch !== null ? `${mismatch} (${mismatchLabel})` : "-"}
            valueColor={
              mismatchLabel === "OK"
                ? "#16a34a"
                : mismatchLabel === "WARNING"
                ? "#f59e0b"
                : mismatchLabel === "CRITICAL"
                ? "#dc2626"
                : undefined
            }
          />
        </div>
        {mismatch !== null && (
          <div style={{ marginTop: 12, fontSize: 14 }}>
            <span style={{ color: "#6b7280" }}>
              DB Open Trades: {openTrades.length} | Alpaca Open Positions: {alpacaOpenCount ?? "-"}
            </span>
          </div>
        )}
      </div>

      <div style={{ marginTop: 24, display: "flex", gap: 16, flexWrap: "wrap" }}>
        <InsightCard title="Best Symbol" value={insights?.best_symbol?.symbol || "-"} />
        <InsightCard title="Best Mode" value={insights?.best_mode?.mode || "-"} />
        <InsightCard title="Most Common Exit" value={insights?.most_common_exit?.exit_reason || "-"} />
        <InsightCard title="Best Hour (UTC)" value={insights?.best_hour?.entry_hour_utc ?? "-"} />
      </div>

      <div style={{ marginTop: 40 }}>
        <h2>Equity Curve</h2>
        <EquityCurveChart rows={equityCurve} />
      </div>

      <div style={{ marginTop: 40 }}>
        <h2>Performance Charts</h2>

        <div style={{ display: "flex", gap: 20, flexWrap: "wrap" }}>
          <div style={{ flex: 1, minWidth: 300 }}>
            <SymbolPerformanceChart rows={symbolPerformance} />
          </div>

          <div style={{ flex: 1, minWidth: 300 }}>
            <ModePerformanceChart rows={modePerformance} />
          </div>
        </div>

        <div style={{ marginTop: 20 }}>
          <HourlyPerformanceChart rows={hourlyPerformance} />
        </div>
      </div>

      <div style={{ marginTop: 40 }}>
        <h2>Reconciliation</h2>
        {lastUpdated && (
          <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 8 }}>
            Data refreshed at: {new Date(lastUpdated).toLocaleString()}
          </div>
        )}
        <div
          style={{
            background: "#fff",
            borderRadius: 8,
            padding: 16,
            boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
          }}
        >
          <div style={{ marginBottom: 12, display: "flex", gap: 12, flexWrap: "wrap" }}>
            <div
              style={{
                padding: "6px 10px",
                borderRadius: 999,
                fontSize: 12,
                fontWeight: 600,
                background:
                  lastReconciliationStatus === "OK"
                    ? "#dcfce7"
                    : lastReconciliationStatus === "WARNING"
                    ? "#fef3c7"
                    : lastReconciliationStatus === "CRITICAL" || lastReconciliationStatus === "FAILED"
                    ? "#fee2e2"
                    : "#e5e7eb",
                color:
                  lastReconciliationStatus === "OK"
                    ? "#166534"
                    : lastReconciliationStatus === "WARNING"
                    ? "#92400e"
                    : lastReconciliationStatus === "CRITICAL" || lastReconciliationStatus === "FAILED"
                    ? "#991b1b"
                    : "#374151",
              }}
            >
              Last Reconciliation: {lastReconciliationStatus || "-"}
            </div>

            <div style={{ fontSize: 12, color: "#6b7280", alignSelf: "center" }}>
              {lastReconciliationAt
                ? `Last Reconciliation Time: ${new Date(lastReconciliationAt).toLocaleString()}`
                : "Last Reconciliation Time: -"}
            </div>
          </div>
          {reconciliationSummary ? (
            <div>
              <div><strong>Status:</strong> {reconciliationSummary.severity || "-"}</div>
              <div><strong>Total Mismatch:</strong> {reconciliationSummary.mismatch_count ?? "-"}</div>

              <div style={{ marginTop: 10 }}>
                <strong>Breakdown:</strong>
                <div>Missing in Alpaca: {reconciliationSummary.missing_in_alpaca ?? 0}</div>
                <div>Missing in DB: {reconciliationSummary.missing_in_db ?? 0}</div>
                <div>Exit Reason Mismatch: {reconciliationSummary.exit_reason_mismatch ?? 0}</div>
                <div>Entry Qty Mismatch: {reconciliationSummary.entry_qty_mismatch ?? 0}</div>
                <div>Exit Qty Mismatch: {reconciliationSummary.exit_qty_mismatch ?? 0}</div>
                <div>Unresolved Exit: {reconciliationSummary.exit_not_resolved ?? 0}</div>
              </div>

              <div style={{ marginTop: 20 }}>
                <strong>Reconciliation Details</strong>
                <div style={{ marginTop: 10 }}>
                  <ReconciliationDetailsTable rows={reconciliationDetails || []} />
                </div>
              </div>

              <div style={{ marginTop: 20 }}>
                <strong>Reconciliation History</strong>
                <div style={{ marginTop: 10 }}>
                  <ReconciliationHistoryTable rows={reconciliationHistory || []} />
                </div>
              </div>
            </div>
          ) : (
            <div style={{ color: "#6b7280" }}>
              Reconciliation data not yet loaded
            </div>
          )}
        </div>
      </div>

      <h2 style={{ marginTop: 40 }}>Open Trades</h2>
      <OpenTradesTable trades={openTrades} />

      <h2 style={{ marginTop: 40 }}>Trade Lifecycle</h2>
      <TradeLifecycleTable rows={lifecycle} />
    </div>
  );
}

function InsightCard({ title, value, valueColor }) {
  return (
    <div
      style={{
        background: "#fff",
        borderRadius: 8,
        padding: 16,
        minWidth: 180,
        boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
      }}
    >
      <div style={{ fontSize: 13, color: "#6b7280", marginBottom: 6 }}>{title}</div>
      <div
        style={{
          fontSize: 20,
          fontWeight: 600,
          color: valueColor || "#111827",
        }}
      >
        {value || "-"}
      </div>
    </div>
  );
}
