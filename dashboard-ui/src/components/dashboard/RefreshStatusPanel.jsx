export default function RefreshStatusPanel({
  lastUpdated,
  nextRefreshAt,
  autoRefreshActive,
  refreshWindowLabel,
  autoRefreshMarketTime,
}) {
  return (
    <section className="dashboard-section">
      <div className="dashboard-panel">
        <div className="dashboard-panel-body dashboard-panel-body-tight">
          <div className="dashboard-panel-heading">
            <div>
              <h3>Dashboard Polling</h3>
            </div>
          </div>

          <div className="dashboard-inline-kv">
            <div><strong>Last Dashboard Refresh:</strong> {lastUpdated ? new Date(lastUpdated).toLocaleString() : "-"}</div>
            <div><strong>Next Dashboard Refresh:</strong> {nextRefreshAt ? new Date(nextRefreshAt).toLocaleString() : "Paused"}</div>
            <div><strong>Auto Refresh:</strong> {autoRefreshActive ? "Active" : "Paused outside polling window"}</div>
            <div><strong>Market Clock:</strong> {autoRefreshMarketTime}</div>
            <div><strong>Polling Window:</strong> {refreshWindowLabel}</div>
          </div>
        </div>
      </div>
    </section>
  );
}
