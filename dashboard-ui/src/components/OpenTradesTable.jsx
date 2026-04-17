import { formatCurrency, formatNumber, formatTimestamp, formatValue, sortRowsByLatest } from "./tableFormatters";
import { useVirtualizedTableRows } from "./useVirtualizedTableRows";

function getStatusBadge(status) {
  if (status === "OPEN") {
    return "dashboard-badge dashboard-badge-ok";
  }
  if (status === "PENDING") {
    return "dashboard-badge dashboard-badge-warn";
  }
  return "dashboard-badge dashboard-badge-neutral";
}

function getBrokerBadge(broker) {
  const normalized = String(broker || "").trim().toUpperCase();
  if (normalized === "IBKR") {
    return "dashboard-badge dashboard-badge-broker-ibkr";
  }
  return "dashboard-badge dashboard-badge-neutral";
}

export default function OpenTradesTable({ trades }) {
  const sortedTrades = sortRowsByLatest(Array.isArray(trades) ? trades : [], ["entry_time", "timestamp_utc", "created_at"]);
  const showLiveComparison = sortedTrades.some((trade) => String(trade.broker || "").trim().toUpperCase() === "IBKR");
  const columnCount = 12 + (showLiveComparison ? 2 : 0);
  const {
    containerRef,
    handleScroll,
    virtualizationEnabled,
    startIndex,
    endIndex,
    topPadding,
    bottomPadding,
  } = useVirtualizedTableRows({
    rowCount: sortedTrades.length,
    rowHeight: showLiveComparison ? 92 : 76,
    overscan: 8,
    minRowsToVirtualize: 40,
  });
  const visibleTrades = sortedTrades.slice(startIndex, endIndex);

  if (!sortedTrades.length) {
    return <div className="dashboard-empty">No open trades.</div>;
  }

  function renderStoredLiveCurrency(storedValue, liveValue, mismatch, liveLabel = "Live", liveFallbackText = "-") {
    const hasStored = storedValue !== null && storedValue !== undefined && storedValue !== "";
    const hasLive = liveValue !== null && liveValue !== undefined && liveValue !== "";
    if (!hasStored && !hasLive) {
      return <span className="dashboard-cell-muted">-</span>;
    }
    return (
      <div className="dashboard-stored-live">
        <div className="dashboard-stored-live-line">
          <span className="dashboard-stored-live-label">Stored</span>
          <span>{hasStored ? formatCurrency(storedValue) : "-"}</span>
        </div>
        <div className={`dashboard-stored-live-line ${mismatch ? "dashboard-stored-live-line-mismatch" : ""}`}>
          <span className="dashboard-stored-live-label">{liveLabel}</span>
          <span>{hasLive ? formatCurrency(liveValue) : liveFallbackText}</span>
        </div>
      </div>
    );
  }

  function getIbkrLiveExitLabel(trade) {
    const hasLivePosition = Boolean(trade.live_position_detected);
    const liveSyncReady = Boolean(trade.live_sync_available);
    const liveOrdersKnown = Boolean(trade.live_orders_available);
    const liveOrdersDetected = Number(trade.live_orders_detected || 0);

    if (hasLivePosition && liveSyncReady && liveOrdersKnown && liveOrdersDetected === 0) {
      return "Strategy on position";
    }
    return "-";
  }

  function renderStoredLiveNumber(storedValue, liveValue, mismatch) {
    const hasStored = storedValue !== null && storedValue !== undefined && storedValue !== "";
    const hasLive = liveValue !== null && liveValue !== undefined && liveValue !== "";
    if (!hasStored && !hasLive) {
      return <span className="dashboard-cell-muted">-</span>;
    }
    return (
      <div className="dashboard-stored-live">
        <div className="dashboard-stored-live-line">
          <span className="dashboard-stored-live-label">Stored</span>
          <span>{hasStored ? formatNumber(storedValue, 0) : "-"}</span>
        </div>
        <div className={`dashboard-stored-live-line ${mismatch ? "dashboard-stored-live-line-mismatch" : ""}`}>
          <span className="dashboard-stored-live-label">Live</span>
          <span>{hasLive ? formatNumber(liveValue, 0) : "-"}</span>
        </div>
      </div>
    );
  }

  return (
    <div
      ref={containerRef}
      onScroll={handleScroll}
      className={`dashboard-table-wrap ${virtualizationEnabled ? "dashboard-table-wrap-virtualized" : ""}`}
    >
      <table className="dashboard-table">
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Broker</th>
            <th>Mode</th>
            <th>Status</th>
            <th className="dashboard-col-number">Shares</th>
            <th className="dashboard-col-number">Position Cost</th>
            <th className="dashboard-col-number">Per Trade Notional</th>
            <th className="dashboard-col-number">Entry Price</th>
            <th className="dashboard-col-number">Stop</th>
            <th className="dashboard-col-number">Target</th>
            {showLiveComparison ? <th className="dashboard-col-number">Current</th> : null}
            {showLiveComparison ? <th>Live Sync</th> : null}
            <th className="dashboard-col-number">Take Profit</th>
            <th>Entry Time</th>
          </tr>
        </thead>
        <tbody>
          {topPadding > 0 ? (
            <tr className="dashboard-virtual-spacer" aria-hidden="true">
              <td colSpan={columnCount} style={{ height: `${topPadding}px` }} />
            </tr>
          ) : null}
          {visibleTrades.map((trade, index) => (
            <tr key={`${trade.trade_key || trade.symbol || "trade"}-${startIndex + index}`}>
              <td data-label="Symbol" className="dashboard-cell-strong">{formatValue(trade.symbol)}</td>
              <td data-label="Broker">
                <span className={getBrokerBadge(trade.broker)}>{formatValue(trade.broker)}</span>
              </td>
              <td data-label="Mode">{formatValue(trade.mode)}</td>
              <td data-label="Status">
                <span className={getStatusBadge(trade.status)}>{formatValue(trade.status)}</span>
              </td>
              <td data-label="Shares" className="dashboard-cell-number">
                {String(trade.broker || "").trim().toUpperCase() === "IBKR"
                  ? renderStoredLiveNumber(trade.stored_shares ?? trade.shares, trade.live_shares, trade.shares_mismatch)
                  : formatNumber(trade.shares, 0)}
              </td>
              <td data-label="Position Cost" className="dashboard-cell-number">{formatCurrency(trade.position_cost)}</td>
              <td data-label="Per Trade Notional" className="dashboard-cell-number">{formatCurrency(trade.per_trade_notional)}</td>
              <td data-label="Entry Price" className="dashboard-cell-number">
                {String(trade.broker || "").trim().toUpperCase() === "IBKR"
                  ? renderStoredLiveCurrency(trade.stored_entry_price ?? trade.entry_price, trade.live_entry_price ?? trade.entry_price, trade.entry_price_mismatch)
                  : formatCurrency(trade.entry_price)}
              </td>
              <td data-label="Stop" className="dashboard-cell-number">
                {String(trade.broker || "").trim().toUpperCase() === "IBKR"
                  ? renderStoredLiveCurrency(
                      trade.stored_stop_price ?? trade.stop_price,
                      trade.live_stop_price,
                      trade.stop_price_mismatch,
                      "Live",
                      getIbkrLiveExitLabel(trade),
                    )
                  : formatCurrency(trade.stop_price)}
              </td>
              <td data-label="Target" className="dashboard-cell-number">
                {String(trade.broker || "").trim().toUpperCase() === "IBKR"
                  ? renderStoredLiveCurrency(
                      trade.stored_target_price ?? trade.target_price,
                      trade.live_target_price,
                      trade.target_price_mismatch,
                      "Live",
                      getIbkrLiveExitLabel(trade),
                    )
                  : formatCurrency(trade.target_price)}
              </td>
              {showLiveComparison ? (
                <td data-label="Current" className="dashboard-cell-number">
                  {String(trade.broker || "").trim().toUpperCase() === "IBKR"
                    ? renderStoredLiveCurrency(trade.live_entry_price, trade.live_current_price, false, "Current")
                    : <span className="dashboard-cell-muted">-</span>}
                </td>
              ) : null}
              {showLiveComparison ? (
                <td data-label="Live Sync">
                  {String(trade.broker || "").trim().toUpperCase() === "IBKR" ? (
                    <div className="dashboard-stored-live">
                      <div className={`dashboard-stored-live-line ${trade.live_sync_available ? "" : "dashboard-stored-live-line-mismatch"}`}>
                        <span className="dashboard-stored-live-label">Status</span>
                        <span>
                          {trade.live_positions_available && trade.live_orders_available
                            ? "Live"
                            : trade.live_positions_available || trade.live_orders_available
                            ? "Partial live"
                            : "Stored only"}
                        </span>
                      </div>
                      <div className="dashboard-stored-live-line">
                        <span className="dashboard-stored-live-label">Orders</span>
                        <span>{trade.live_orders_detected ?? 0}</span>
                      </div>
                    </div>
                  ) : (
                    <span className="dashboard-cell-muted">-</span>
                  )}
                </td>
              ) : null}
              <td data-label="Take Profit" className="dashboard-cell-number">{formatCurrency(trade.take_profit_dollars)}</td>
              <td data-label="Entry Time" className="dashboard-cell-muted">{formatTimestamp(trade.entry_time)}</td>
            </tr>
          ))}
          {bottomPadding > 0 ? (
            <tr className="dashboard-virtual-spacer" aria-hidden="true">
              <td colSpan={columnCount} style={{ height: `${bottomPadding}px` }} />
            </tr>
          ) : null}
        </tbody>
      </table>
    </div>
  );
}
