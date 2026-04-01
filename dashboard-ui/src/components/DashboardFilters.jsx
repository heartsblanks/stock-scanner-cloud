import { useState } from "react";

export default function DashboardFilters({ onApply }) {
  const [date, setDate] = useState("");
  const [symbol, setSymbol] = useState("");

  function handleApply() {
    onApply?.({ date, symbol });
  }

  return (
    <div className="dashboard-panel">
      <div className="dashboard-panel-body dashboard-filter-panel">
        <div>
          <h2 className="dashboard-panel-title">Filters</h2>
          <p className="dashboard-panel-subtitle">
            Narrow the dashboard to a specific date or symbol without leaving the main operating view.
          </p>
        </div>
        <div className="dashboard-filter-row">
          <div className="dashboard-field">
            <label htmlFor="dashboard-filter-date">Date</label>
        <input
          id="dashboard-filter-date"
          className="dashboard-input"
          type="date"
          value={date}
          onChange={(e) => setDate(e.target.value)}
        />
          </div>

          <div className="dashboard-field">
            <label htmlFor="dashboard-filter-symbol">Symbol</label>
        <input
          id="dashboard-filter-symbol"
          className="dashboard-input"
          type="text"
          placeholder="e.g. NVDA"
          value={symbol}
          onChange={(e) => setSymbol(e.target.value)}
        />
          </div>

          <button className="dashboard-button dashboard-button-primary" onClick={handleApply}>
            Apply Filters
          </button>
        </div>
      </div>
    </div>
  );
}
