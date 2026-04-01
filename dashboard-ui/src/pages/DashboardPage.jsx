import SummaryCards from "../components/SummaryCards";
import DashboardFilters from "../components/DashboardFilters";
import InsightCard from "../components/InsightCard";
import AttentionRequiredPanel from "../components/dashboard/AttentionRequiredPanel";
import ExecutionInsightsSection from "../components/dashboard/ExecutionInsightsSection";
import HealthOverviewSection from "../components/dashboard/HealthOverviewSection";
import RefreshStatusPanel from "../components/dashboard/RefreshStatusPanel";
import { useDashboardData } from "../hooks/useDashboardData";
import { Suspense, lazy, useEffect, useState } from "react";

const OpenTradesTable = lazy(() => import("../components/OpenTradesTable"));
const TradeLifecycleTable = lazy(() => import("../components/TradeLifecycleTable"));
const EquityCurveChart = lazy(() => import("../components/EquityCurveChart"));
const HourlyPerformanceChart = lazy(() => import("../components/HourlyPerformanceChart"));
const HourlyAttemptOutcomeChart = lazy(() => import("../components/HourlyAttemptOutcomeChart"));
const HourlyOutcomeQualityTable = lazy(() => import("../components/HourlyOutcomeQualityTable"));
const SymbolPerformanceChart = lazy(() => import("../components/SymbolPerformanceChart"));
const ModePerformanceChart = lazy(() => import("../components/ModePerformanceChart"));
const AlpacaApiLogsSection = lazy(() => import("../components/dashboard/AlpacaApiLogsSection"));
const ReconciliationSection = lazy(() => import("../components/dashboard/ReconciliationSection"));

const DASHBOARD_VIEWS = [
  { id: "overview", label: "Overview", description: "Best for the first look each session." },
  { id: "trades", label: "Trades", description: "Open positions and lifecycle details." },
  { id: "reconciliation", label: "Reconciliation", description: "Mismatch and repair workflow." },
  { id: "broker", label: "Broker Logs", description: "Alpaca call health and failures." },
  { id: "analytics", label: "Analytics", description: "Charts and performance patterns." },
];

function formatCurrency(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }

  return `$${Number(value).toFixed(2)}`;
}

function getEntryHourUtc(row) {
  const timestamp = row?.entry_time || row?.timestamp_utc;
  if (!timestamp) {
    return null;
  }

  const parsed = new Date(timestamp);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }

  return String(parsed.getUTCHours()).padStart(2, "0");
}

function LazySection({ children }) {
  return <Suspense fallback={<div className="dashboard-empty">Loading section...</div>}>{children}</Suspense>;
}

function getInitialView() {
  if (typeof window === "undefined") {
    return "overview";
  }

  const params = new URLSearchParams(window.location.search);
  const view = String(params.get("view") || "").trim().toLowerCase();
  return DASHBOARD_VIEWS.some((item) => item.id === view) ? view : "overview";
}

export default function DashboardPage() {
  const [activeView, setActiveView] = useState(getInitialView);
  const [drilldown, setDrilldown] = useState({ symbol: "", mode: "", hourUtc: "" });
  const [theme, setTheme] = useState(() => {
    if (typeof window === "undefined") {
      return "light";
    }

    const savedTheme = window.localStorage.getItem("dashboard-theme");
    if (savedTheme === "light" || savedTheme === "dark") {
      return savedTheme;
    }

    return window.matchMedia?.("(prefers-color-scheme: dark)")?.matches ? "dark" : "light";
  });

  const {
    summary,
    openTrades,
    lifecycle,
    loading,
    error,
    sectionLoading,
    sectionErrors,
    reconciliationSummary,
    reconciliationSymbolFilter,
    setReconciliationSymbolFilter,
    reconciliationHistory,
    riskExposureSummary,
    alpacaApiLogs,
    alpacaApiErrors,
    lastUpdated,
    isRefreshing,
    isRunningSync,
    toast,
    lastReconciliationStatus,
    lastReconciliationAt,
    symbolPerformance,
    modePerformance,
    hourlyPerformance,
    hourlyOutcomeQuality,
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
    topAttemptReasons,
    stageCounts,
    paperTradePlacementRate,
    attentionItems,
    nextRefreshAt,
    autoRefreshActive,
    refreshWindowLabel,
    autoRefreshMarketTime,
    paperTradeAttemptRejections,
    paperTradeAttemptDailySummary,
    paperTradeAttemptHourlySummary,
    handleApplyFilters,
    refreshData,
    rerunReconciliation,
    syncPaperTrades,
    alpacaOpenCount,
  } = useDashboardData(activeView);

  const mismatchTone =
    mismatchLabel === "OK"
      ? "dashboard-pill-ok"
      : mismatchLabel === "WARNING"
        ? "dashboard-pill-warn"
        : mismatchLabel === "CRITICAL"
          ? "dashboard-pill-danger"
          : "dashboard-pill-info";
  const hasDrilldown = Boolean(drilldown.symbol || drilldown.mode || drilldown.hourUtc);

  const filteredOpenTrades = openTrades.filter((row) => {
    const symbolMatch = !drilldown.symbol || String(row?.symbol || "").trim().toUpperCase() === drilldown.symbol;
    const modeMatch = !drilldown.mode || String(row?.mode || "").trim() === drilldown.mode;
    const hourMatch = !drilldown.hourUtc || getEntryHourUtc(row) === drilldown.hourUtc;
    return symbolMatch && modeMatch && hourMatch;
  });

  const filteredLifecycle = lifecycle.filter((row) => {
    const symbolMatch = !drilldown.symbol || String(row?.symbol || "").trim().toUpperCase() === drilldown.symbol;
    const modeMatch = !drilldown.mode || String(row?.mode || "").trim() === drilldown.mode;
    const hourMatch = !drilldown.hourUtc || getEntryHourUtc(row) === drilldown.hourUtc;
    return symbolMatch && modeMatch && hourMatch;
  });

  function applyDrilldown(nextDrilldown) {
    setDrilldown((current) => ({
      symbol: nextDrilldown.symbol ?? current.symbol,
      mode: nextDrilldown.mode ?? current.mode,
      hourUtc: nextDrilldown.hourUtc ?? current.hourUtc,
    }));
    setActiveView("trades");
  }

  function clearDrilldown() {
    setDrilldown({ symbol: "", mode: "", hourUtc: "" });
  }

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    window.localStorage.setItem("dashboard-theme", theme);
  }, [theme]);

  useEffect(() => {
    const viewLabel = DASHBOARD_VIEWS.find((view) => view.id === activeView)?.label || "Overview";
    document.title = `${viewLabel} | Stock Scanner Console`;

    if (typeof window !== "undefined") {
      const url = new URL(window.location.href);
      url.searchParams.set("view", activeView);
      window.history.replaceState({}, "", url);
    }
  }, [activeView]);

  if (loading) {
    return <div className="dashboard-loading">Loading dashboard...</div>;
  }

  if (error) {
    return <div className="dashboard-error">Error: {error}</div>;
  }

  return (
    <div className="dashboard-shell">
      <div className="dashboard-frame">
        <section className="dashboard-hero">
          <div className="dashboard-hero-grid">
            <div>
              <div className="dashboard-kicker">
                <span className="dashboard-kicker-dot" />
                Trading Operations Console
              </div>
              <h1 className="dashboard-title">See the trading day as one connected system.</h1>
              <p className="dashboard-subtitle">
                A sharper command view for scan quality, risk posture, reconciliation, broker health, and the state of
                every paper trade moving through the stack.
              </p>
              <div className="dashboard-hero-meta">
                <div className="dashboard-hero-stat">
                  <div className="dashboard-hero-stat-label">Realized P&amp;L</div>
                  <div className="dashboard-hero-stat-value">
                    {formatCurrency(summary?.summary?.realized_pnl_total)}
                  </div>
                </div>
                <div className="dashboard-hero-stat">
                  <div className="dashboard-hero-stat-label">Open vs Alpaca</div>
                  <div className="dashboard-hero-stat-value">
                    {openTrades.length} / {alpacaOpenCount ?? "-"}
                  </div>
                </div>
                <div className="dashboard-hero-stat">
                  <div className="dashboard-hero-stat-label">Reconciliation Drift</div>
                  <div className="dashboard-hero-stat-value">
                    {mismatch !== null ? `${mismatch} ${mismatchLabel ? `(${mismatchLabel})` : ""}` : "-"}
                  </div>
                </div>
              </div>
            </div>

            <div className="dashboard-hero-side">
              <div className="dashboard-callout">
                <div className="dashboard-callout-label">Operational Pulse</div>
                <div className="dashboard-callout-value">{backendHealthStatus || "UNKNOWN"}</div>
                <div className="dashboard-callout-note">
                  Last refresh: {lastUpdated ? new Date(lastUpdated).toLocaleString() : "Not available yet"}
                </div>
              </div>

              <div className="dashboard-toolbar">
                <button
                  onClick={refreshData}
                  disabled={isRefreshing}
                  className="dashboard-button dashboard-button-primary"
                >
                  {isRefreshing ? "Refreshing..." : "Refresh View"}
                </button>
                <button
                  onClick={() => setTheme((currentTheme) => (currentTheme === "dark" ? "light" : "dark"))}
                  className="dashboard-button dashboard-button-theme"
                >
                  {theme === "dark" ? "Switch to Light" : "Switch to Dark"}
                </button>
              </div>

              <div className="dashboard-inline-meta">
                <span className={`dashboard-pill dashboard-pill-status ${mismatchTone}`}>
                  Mismatch {mismatch !== null ? mismatch : "-"}
                </span>
                <span className="dashboard-pill">Sync {syncHealthStatus || "UNKNOWN"}</span>
                <span className="dashboard-pill">Recon {reconciliationHealthStatus || "UNKNOWN"}</span>
              </div>
            </div>
          </div>
        </section>

        {toast && (
          <div
            className={`dashboard-toast ${toast.type === "success" ? "dashboard-toast-success" : "dashboard-toast-error"}`}
          >
            {toast.message}
          </div>
        )}

        <div className="dashboard-grid">
          <DashboardFilters onApply={handleApplyFilters} />

          <div className="dashboard-banner">
            <div className="dashboard-banner-title">Focused Operations</div>
            <div className="dashboard-banner-copy">
              Refresh now follows the active view instead of reloading the whole dashboard. Manual sync and
              reconciliation actions live inside their own workspaces so the landing page stays operational, not noisy.
            </div>
          </div>

          <section className="dashboard-view-nav-panel">
            <div className="dashboard-view-nav-copy">
              <div className="dashboard-banner-title">Focused Views</div>
              <div className="dashboard-banner-copy">
                Keep the landing screen light, then jump into the exact slice you need instead of scrolling through the
                whole operating system at once.
              </div>
            </div>
            <div className="dashboard-view-nav">
              {DASHBOARD_VIEWS.map((view) => (
                <button
                  key={view.id}
                  type="button"
                  onClick={() => setActiveView(view.id)}
                  className={`dashboard-view-tab ${activeView === view.id ? "dashboard-view-tab-active" : ""}`}
                >
                  <span className="dashboard-view-tab-label">{view.label}</span>
                  <span className="dashboard-view-tab-copy">{view.description}</span>
                </button>
              ))}
            </div>
          </section>

          {hasDrilldown && (
            <section className="dashboard-drilldown-panel">
              <div className="dashboard-view-nav-copy">
                <div className="dashboard-banner-title">Active Drilldown</div>
                <div className="dashboard-banner-copy">
                  The trades view is narrowed by the selections you made from the overview cards or charts.
                </div>
              </div>
              <div className="dashboard-inline-meta">
                {drilldown.symbol && <span className="dashboard-pill">Symbol {drilldown.symbol}</span>}
                {drilldown.mode && <span className="dashboard-pill">Mode {drilldown.mode}</span>}
                {drilldown.hourUtc && <span className="dashboard-pill">Hour {drilldown.hourUtc}:00 UTC</span>}
                <button type="button" className="dashboard-button dashboard-button-neutral" onClick={clearDrilldown}>
                  Clear Drilldown
                </button>
              </div>
            </section>
          )}

          {activeView === "overview" && (
            <>
              <AttentionRequiredPanel items={attentionItems} />
              <SummaryCards data={summary} />
              {sectionLoading.overview && <div className="dashboard-empty">Loading overview...</div>}
              {sectionErrors.overview && <div className="dashboard-error">{sectionErrors.overview}</div>}

              <HealthOverviewSection
                lastUpdated={lastUpdated}
                sectionLoading={sectionLoading}
                sectionErrors={sectionErrors}
                openTrades={openTrades}
                alpacaOpenCount={alpacaOpenCount}
                mismatch={mismatch}
                mismatchLabel={mismatchLabel}
                backendHealthStatus={backendHealthStatus}
                syncHealthStatus={syncHealthStatus}
                reconciliationHealthStatus={reconciliationHealthStatus}
                lastReconciliationAt={lastReconciliationAt}
                alpacaApiErrors={alpacaApiErrors}
                isRunningSync={isRunningSync}
                riskExposureSummary={riskExposureSummary}
                confidenceMultiplier={confidenceMultiplier}
                lossMultiplier={lossMultiplier}
                finalSizingMultiplier={finalSizingMultiplier}
                multiplierStatus={multiplierStatus}
                compact
              />

              <div className="dashboard-split">
                <RefreshStatusPanel
                  lastUpdated={lastUpdated}
                  nextRefreshAt={nextRefreshAt}
                  autoRefreshActive={autoRefreshActive}
                  refreshWindowLabel={refreshWindowLabel}
                  autoRefreshMarketTime={autoRefreshMarketTime}
                />
                <ExecutionInsightsSection
                  sectionLoading={sectionLoading}
                  sectionErrors={sectionErrors}
                  paperTradePlacementRate={paperTradePlacementRate}
                  stageCounts={stageCounts}
                  topAttemptReasons={topAttemptReasons}
                  paperTradeAttemptRejections={paperTradeAttemptRejections}
                  paperTradeAttemptDailySummary={paperTradeAttemptDailySummary}
                  paperTradeAttemptHourlySummary={paperTradeAttemptHourlySummary}
                />
              </div>

              <section className="dashboard-section">
                <div className="dashboard-panel">
                  <div className="dashboard-panel-body">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">Equity Curve</h2>
                        <p className="dashboard-panel-subtitle">
                          See the account arc over time instead of reading isolated trade outcomes.
                        </p>
                      </div>
                    </div>
                    <LazySection>
                      <EquityCurveChart rows={equityCurve} />
                    </LazySection>
                  </div>
                </div>
              </section>
            </>
          )}

          {activeView === "trades" && (
            <>
              <section className="dashboard-section">
                <div className="dashboard-panel dashboard-panel-strong">
                  <div className="dashboard-panel-body dashboard-panel-body-tight">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">Trade Actions</h2>
                        <p className="dashboard-panel-subtitle">
                          Sync refreshes local paper trade state from Alpaca when fills or exits look stale.
                        </p>
                      </div>
                      <div className="dashboard-toolbar">
                        <button
                          onClick={syncPaperTrades}
                          disabled={isRefreshing || isRunningSync}
                          className="dashboard-button dashboard-button-secondary"
                        >
                          {isRunningSync ? "Syncing Trades..." : "Sync Paper Trades"}
                        </button>
                      </div>
                    </div>
                  </div>
                </div>
              </section>

              <section className="dashboard-section">
                <div className="dashboard-panel">
                  <div className="dashboard-panel-body">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">Open Trades</h2>
                        <p className="dashboard-panel-subtitle">
                          Live database positions with sizing and protective levels in one place.
                        </p>
                      </div>
                    </div>
                    <LazySection>
                      <OpenTradesTable trades={filteredOpenTrades} />
                    </LazySection>
                  </div>
                </div>
              </section>

              <section className="dashboard-section">
                <div className="dashboard-panel">
                  <div className="dashboard-panel-body">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">Trade Lifecycle</h2>
                        <p className="dashboard-panel-subtitle">
                          The canonical trade record for entries, exits, duration, and realized outcome.
                        </p>
                      </div>
                    </div>
                    <LazySection>
                      <TradeLifecycleTable rows={filteredLifecycle} />
                    </LazySection>
                  </div>
                </div>
              </section>
            </>
          )}

          {activeView === "reconciliation" && (
            <>
              <section className="dashboard-section">
                <div className="dashboard-panel dashboard-panel-strong">
                  <div className="dashboard-panel-body dashboard-panel-body-tight">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">Reconciliation Actions</h2>
                        <p className="dashboard-panel-subtitle">
                          Run reconciliation when you want a fresh mismatch audit against Alpaca and a new stored run.
                        </p>
                      </div>
                      <div className="dashboard-toolbar">
                        <button
                          onClick={rerunReconciliation}
                          disabled={isRefreshing}
                          className="dashboard-button dashboard-button-secondary"
                        >
                          Re-run Reconciliation
                        </button>
                      </div>
                    </div>
                  </div>
                </div>
              </section>

              <LazySection>
                <ReconciliationSection
                  sectionLoading={sectionLoading}
                  sectionErrors={sectionErrors}
                  lastUpdated={lastUpdated}
                  lastReconciliationStatus={lastReconciliationStatus}
                  lastReconciliationAt={lastReconciliationAt}
                  reconciliationSummary={reconciliationSummary}
                  reconciliationSymbolFilter={reconciliationSymbolFilter}
                  setReconciliationSymbolFilter={setReconciliationSymbolFilter}
                  reconciliationSymbols={reconciliationSymbols}
                  filteredReconciliationDetails={filteredReconciliationDetails}
                  reconciliationHistory={reconciliationHistory}
                />
              </LazySection>
            </>
          )}

          {activeView === "broker" && (
            <LazySection>
              <AlpacaApiLogsSection
                sectionLoading={sectionLoading}
                sectionErrors={sectionErrors}
                alpacaApiLogs={alpacaApiLogs}
                alpacaApiErrors={alpacaApiErrors}
              />
            </LazySection>
          )}

          {activeView === "analytics" && (
            <>
              <section className="dashboard-section">
                <div className="dashboard-panel dashboard-panel-strong">
                  <div className="dashboard-panel-body">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">Performance Readouts</h2>
                        <p className="dashboard-panel-subtitle">
                          Symbol, mode, exit pattern, and timing edges gathered in one analytical view.
                        </p>
                      </div>
                    </div>
                    <div className="dashboard-metrics-grid">
                      <InsightCard
                        title="Best Symbol"
                        value={insights?.best_symbol?.symbol || "-"}
                        interactive={Boolean(insights?.best_symbol?.symbol)}
                        onClick={() =>
                          insights?.best_symbol?.symbol &&
                          applyDrilldown({
                            symbol: String(insights.best_symbol.symbol).trim().toUpperCase(),
                            mode: "",
                            hourUtc: "",
                          })
                        }
                      />
                      <InsightCard
                        title="Best Mode"
                        value={insights?.best_mode?.mode || "-"}
                        interactive={Boolean(insights?.best_mode?.mode)}
                        onClick={() =>
                          insights?.best_mode?.mode &&
                          applyDrilldown({
                            symbol: "",
                            mode: String(insights.best_mode.mode).trim(),
                            hourUtc: "",
                          })
                        }
                      />
                      <InsightCard title="Most Common Exit" value={insights?.most_common_exit?.exit_reason || "-"} />
                      <InsightCard
                        title="Best Hour (UTC)"
                        value={insights?.best_hour?.entry_hour_utc ?? "-"}
                        interactive={insights?.best_hour?.entry_hour_utc !== undefined && insights?.best_hour?.entry_hour_utc !== null}
                        onClick={() =>
                          insights?.best_hour?.entry_hour_utc !== undefined &&
                          insights?.best_hour?.entry_hour_utc !== null &&
                          applyDrilldown({
                            symbol: "",
                            mode: "",
                            hourUtc: String(insights.best_hour.entry_hour_utc).padStart(2, "0"),
                          })
                        }
                      />
                      <InsightCard
                        title="Busiest Attempt Hour (ET)"
                        value={
                          paperTradeAttemptHourlySummary?.length
                            ? (() => {
                                const row = [...paperTradeAttemptHourlySummary].sort(
                                  (left, right) => Number(right.total_attempts || 0) - Number(left.total_attempts || 0)
                                )[0];
                                if (!row) return "-";
                                const hour = Number(row.hour_ny);
                                const suffix = hour >= 12 ? "PM" : "AM";
                                const normalizedHour = hour % 12 || 12;
                                return `${normalizedHour}:00 ${suffix}`;
                              })()
                            : "-"
                        }
                      />
                    </div>
                  </div>
                </div>
              </section>

              <section className="dashboard-section">
                <div className="dashboard-panel">
                  <div className="dashboard-panel-body">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">Performance Charts</h2>
                        <p className="dashboard-panel-subtitle">
                          Compare symbol mix, mode behavior, and intraday timing without losing the bigger picture.
                        </p>
                      </div>
                    </div>
                    <div className="dashboard-chart-grid">
                      <div>
                        <LazySection>
                          <SymbolPerformanceChart
                            rows={symbolPerformance}
                            onSymbolSelect={(symbol) =>
                              symbol &&
                              applyDrilldown({
                                symbol: String(symbol).trim().toUpperCase(),
                                mode: "",
                                hourUtc: "",
                              })
                            }
                          />
                        </LazySection>
                      </div>
                      <div>
                        <LazySection>
                          <ModePerformanceChart
                            rows={modePerformance}
                            onModeSelect={(mode) =>
                              mode &&
                              applyDrilldown({
                                symbol: "",
                                mode: String(mode).trim(),
                                hourUtc: "",
                              })
                            }
                          />
                        </LazySection>
                      </div>
                    </div>
                    <div style={{ marginTop: 20 }}>
                      <LazySection>
                        <HourlyPerformanceChart
                          rows={hourlyPerformance}
                          onHourSelect={(hour) =>
                            hour !== undefined &&
                            hour !== null &&
                            applyDrilldown({
                              symbol: "",
                              mode: "",
                              hourUtc: String(hour).padStart(2, "0"),
                            })
                          }
                        />
                      </LazySection>
                    </div>
                    <div style={{ marginTop: 20 }}>
                      <LazySection>
                        <HourlyAttemptOutcomeChart rows={paperTradeAttemptHourlySummary} />
                      </LazySection>
                    </div>
                    <div style={{ marginTop: 20 }}>
                      <LazySection>
                        <HourlyOutcomeQualityTable rows={hourlyOutcomeQuality} />
                      </LazySection>
                    </div>
                  </div>
                </div>
              </section>

              <section className="dashboard-section">
                <div className="dashboard-panel">
                  <div className="dashboard-panel-body">
                    <div className="dashboard-panel-heading">
                      <div>
                        <h2 className="dashboard-panel-title">Equity Curve</h2>
                        <p className="dashboard-panel-subtitle">
                          The bigger account arc, isolated from the heavier operational details.
                        </p>
                      </div>
                    </div>
                    <LazySection>
                      <EquityCurveChart rows={equityCurve} />
                    </LazySection>
                  </div>
                </div>
              </section>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
