import { useState } from "react";

export default function DashboardFilters({ onApply }) {
  const [date, setDate] = useState("");
  const [symbol, setSymbol] = useState("");

  function handleApply() {
    onApply?.({ date, symbol });
  }

  return (
    <div
      style={{
        background: "#fff",
        padding: 16,
        borderRadius: 8,
        boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
        marginBottom: 20,
        display: "flex",
        gap: 12,
        flexWrap: "wrap",
        alignItems: "center",
      }}
    >
      <div>
        <label style={{ fontSize: 12 }}>Date</label>
        <br />
        <input
          type="date"
          value={date}
          onChange={(e) => setDate(e.target.value)}
        />
      </div>

      <div>
        <label style={{ fontSize: 12 }}>Symbol</label>
        <br />
        <input
          type="text"
          placeholder="e.g. NVDA"
          value={symbol}
          onChange={(e) => setSymbol(e.target.value)}
        />
      </div>

      <button
        onClick={handleApply}
        style={{
          height: 32,
          padding: "0 12px",
          borderRadius: 6,
          border: "none",
          background: "#2563eb",
          color: "#fff",
          cursor: "pointer",
        }}
      >
        Apply
      </button>
    </div>
  );
}
