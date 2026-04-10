import { useState } from "react";

export default function DashboardFilters({ onApply }) {
  const [date, setDate] = useState("");

  function handleDateChange(nextDate) {
    setDate(nextDate);
    onApply?.({ date: nextDate });
  }

  return (
    <div className="dashboard-date-filter-inline">
      <label htmlFor="dashboard-filter-date">Date</label>
      <input
        id="dashboard-filter-date"
        className="dashboard-input dashboard-input-inline-date"
        type="date"
        value={date}
        onChange={(e) => handleDateChange(e.target.value)}
      />
    </div>
  );
}
