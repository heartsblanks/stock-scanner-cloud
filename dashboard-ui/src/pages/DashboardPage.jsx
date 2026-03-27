import { useEffect, useState } from "react";
import {
  fetchDashboardSummary,
  fetchOpenTrades,
  fetchTradeLifecycle,
} from "../api/dashboard";

import SummaryCards from "../components/SummaryCards";
import OpenTradesTable from "../components/OpenTradesTable";
import TradeLifecycleTable from "../components/TradeLifecycleTable";
import HourlyPerformanceChart from "../components/HourlyPerformanceChart";
import SymbolPerformanceChart from "../components/SymbolPerformanceChart";
import ModePerformanceChart from "../components/ModePerformanceChart";
import DashboardFilters from "../components/DashboardFilters";
import EquityCurveChart from "../components/EquityCurveChart";

export default function DashboardPage() {
  const [summary, setSummary] = useState(null);
  const [openTrades, setOpenTrades] = useState([]);
  const [lifecycle, setLifecycle] = useState([]);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [filters, setFilters] = useState({ date: "", symbol: "" });

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
    } catch (err) {
      setError(err.message || "Failed to load dashboard");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadData(filters);
  }, []);

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

      <DashboardFilters onApply={handleApplyFilters} />

      <SummaryCards data={summary} />

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

      <h2 style={{ marginTop: 40 }}>Open Trades</h2>
      <OpenTradesTable trades={openTrades} />

      <h2 style={{ marginTop: 40 }}>Trade Lifecycle</h2>
      <TradeLifecycleTable rows={lifecycle} />
    </div>
  );
}

function InsightCard({ title, value }) {
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
      <div style={{ fontSize: 20, fontWeight: 600 }}>{value || "-"}</div>
    </div>
  );
}
