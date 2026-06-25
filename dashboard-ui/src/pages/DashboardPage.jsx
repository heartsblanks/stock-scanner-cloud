import DashboardFilters from "../components/DashboardFilters";
import OpenTradesTable from "../components/OpenTradesTable";
import { useDashboardData } from "../hooks/useDashboardData";

function formatCurrency(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "-";
  }
  return `${numeric < 0 ? "-" : ""}$${Math.abs(numeric).toFixed(2)}`;
}

function formatPercent(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) ? `${numeric.toFixed(1)}%` : "-";
}

function formatTimestamp(value) {
  if (!value) {
    return "-";
  }
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime())
    ? "-"
    : parsed.toLocaleString([], {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      });
}

function getPnlTone(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric === 0) {
    return "dashboard-daily-neutral";
  }
  return numeric > 0 ? "dashboard-daily-positive" : "dashboard-daily-negative";
}

function DailyMetric({ label, value, tone = "dashboard-daily-neutral", note }) {
  return (
    <article className={`dashboard-daily-card ${tone}`}>
      <span className="dashboard-daily-label">{label}</span>
      <strong className="dashboard-daily-value">{value}</strong>
      {note ? <span className="dashboard-daily-note">{note}</span> : null}
    </article>
  );
}

export default function DashboardPage() {
  const {
    daily,
    openTrades,
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
  } = useDashboardData();

  const realizedPnl = Number(daily?.realized_pnl || 0);
  const unrealizedPnl = daily?.unrealized_pnl;
  const totalDayPnl = daily?.total_day_pnl;
  const latestScan = daily?.latest_scan || {};
  const urgentItems = [];

  if (error) {
    urgentItems.push(error);
  }
  if (!latestScan?.scan_time) {
    urgentItems.push("No scan has been recorded yet.");
  }

  return (
    <div className="dashboard-shell dashboard-shell-daily">
      <div className="dashboard-frame dashboard-frame-daily">
        <header className="dashboard-daily-header">
          <div>
            <h1 className="dashboard-command-title">Daily Trading</h1>
            <p className="dashboard-daily-subtitle">Today&apos;s P/L, exposure, scan status, and open trades.</p>
          </div>
          <div className="dashboard-command-row dashboard-command-row-dense">
            <DashboardFilters onApply={handleApplyFilters} />
            <button
              type="button"
              onClick={refreshData}
              disabled={isRefreshing}
              className="dashboard-button dashboard-button-primary dashboard-button-compact"
            >
              {isRefreshing ? "Refreshing..." : "Refresh"}
            </button>
            <button
              type="button"
              onClick={syncPaperTrades}
              disabled={isRunningSync || isRefreshing}
              className="dashboard-button dashboard-button-secondary dashboard-button-compact"
            >
              {isRunningSync ? "Syncing..." : "Sync Trades"}
            </button>
          </div>
        </header>

        {loading && !lastUpdated ? <div className="dashboard-loading">Loading daily dashboard...</div> : null}

        {toast ? (
          <div className={`dashboard-toast ${toast.type === "success" ? "dashboard-toast-success" : "dashboard-toast-error"}`}>
            {toast.message}
          </div>
        ) : null}

        <section className="dashboard-daily-grid">
          <DailyMetric
            label="Realized P/L"
            value={formatCurrency(realizedPnl)}
            tone={getPnlTone(realizedPnl)}
            note={`${daily?.closed_trade_count || 0} closed`}
          />
          <DailyMetric
            label="Unrealized P/L"
            value={formatCurrency(unrealizedPnl)}
            tone={getPnlTone(unrealizedPnl)}
            note={unrealizedPnl === null || unrealizedPnl === undefined ? "Not live-priced" : "Open positions"}
          />
          <DailyMetric
            label="Total Day P/L"
            value={formatCurrency(totalDayPnl)}
            tone={getPnlTone(totalDayPnl)}
            note={`${daily?.winning_trade_count || 0} wins / ${daily?.losing_trade_count || 0} losses`}
          />
          <DailyMetric
            label="Open Exposure"
            value={formatCurrency(daily?.open_exposure)}
            note={`${daily?.open_position_count || openTrades.length || 0} open`}
          />
          <DailyMetric
            label="Placements"
            value={String(daily?.placements_today || 0)}
            note={`${formatPercent(daily?.placement_rate)} placement rate`}
          />
          <DailyMetric
            label="Latest Scan"
            value={formatTimestamp(latestScan?.scan_time)}
            note={latestScan?.mode ? `${latestScan.mode} ${latestScan.scan_source || ""}` : "No mode"}
          />
        </section>

        {urgentItems.length ? (
          <section className="dashboard-daily-alerts">
            {urgentItems.map((item) => (
              <div key={item} className="dashboard-attention-item dashboard-attention-item-warning">
                <div className="dashboard-attention-title">Attention</div>
                <div className="dashboard-attention-detail">{item}</div>
              </div>
            ))}
          </section>
        ) : null}

        <section className="dashboard-section">
          <div className="dashboard-panel">
            <div className="dashboard-panel-body">
              <div className="dashboard-panel-heading">
                <div>
                  <h2 className="dashboard-panel-title">Open Trades</h2>
                  <p className="dashboard-panel-subtitle">
                    Last updated {lastUpdated ? new Date(lastUpdated).toLocaleTimeString() : "-"}
                    {nextRefreshAt ? ` · next ${new Date(nextRefreshAt).toLocaleTimeString()}` : ""}
                  </p>
                </div>
              </div>
              <OpenTradesTable trades={openTrades} compact />
            </div>
          </div>
        </section>
      </div>
    </div>
  );
}
