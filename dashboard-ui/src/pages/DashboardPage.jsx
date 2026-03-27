import { useEffect, useState } from "react";
import {
  fetchDashboardSummary,
  fetchOpenTrades,
  fetchTradeLifecycle,
} from "../api/dashboard";

import SummaryCards from "../components/SummaryCards";
import OpenTradesTable from "../components/OpenTradesTable";
import TradeLifecycleTable from "../components/TradeLifecycleTable";

export default function DashboardPage() {
  const [summary, setSummary] = useState(null);
  const [openTrades, setOpenTrades] = useState([]);
  const [lifecycle, setLifecycle] = useState([]);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  async function loadData() {
    try {
      setLoading(true);
      setError(null);

      const [summaryRes, openRes, lifecycleRes] = await Promise.all([
        fetchDashboardSummary(),
        fetchOpenTrades(100),
        fetchTradeLifecycle(200),
      ]);

      setSummary(summaryRes);
      setOpenTrades(openRes?.rows || []);
      setLifecycle(lifecycleRes?.rows || []);
    } catch (err) {
      setError(err.message || "Failed to load dashboard");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadData();
  }, []);

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
    <div style={{ padding: 20 }}>
      <h1>Trading Dashboard</h1>

      <SummaryCards data={summary} />

      <h2 style={{ marginTop: 30 }}>Open Trades</h2>
      <OpenTradesTable trades={openTrades} />

      <h2 style={{ marginTop: 30 }}>Trade Lifecycle</h2>
      <TradeLifecycleTable rows={lifecycle} />
    </div>
  );
}
