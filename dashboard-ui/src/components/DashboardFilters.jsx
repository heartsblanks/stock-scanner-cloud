import { useState } from "react";

export default function DashboardFilters({ onApply }) {
  const [date, setDate] = useState("");

  function handleApply() {
    onApply?.({ date });
  }

  function handleClear() {
    setDate("");
    onApply?.({ date: "" });
  }

  return (
    <div className="dashboard-panel">
      <div className="dashboard-panel-body dashboard-filter-panel">
        <div>
          <h2 className="dashboard-panel-title">Session Filter</h2>
          <p className="dashboard-panel-subtitle">
            Keep this compact: date scope only.
          </p>
        </div>
        <div className="dashboard-filter-row">
          <div className="dashboard-field dashboard-field-compact">
            <label htmlFor="dashboard-filter-date">Date</label>
            <input
              id="dashboard-filter-date"
              className="dashboard-input"
              type="date"
              value={date}
              onChange={(e) => setDate(e.target.value)}
            />
          </div>

          <button className="dashboard-button dashboard-button-primary" onClick={handleApply}>
            Apply
          </button>
          <button className="dashboard-button dashboard-button-neutral" onClick={handleClear}>
            Clear
          </button>
        </div>
      </div>
    </div>
  );
}
