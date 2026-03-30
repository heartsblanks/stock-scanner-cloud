-- Scan runs: one row per scan execution
CREATE TABLE IF NOT EXISTS scan_runs (
    id SERIAL PRIMARY KEY,
    scan_time TIMESTAMPTZ NOT NULL,
    mode TEXT,
    scan_source TEXT,
    market_phase TEXT,
    candidate_count INT,
    placed_count INT,
    skipped_count INT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Trade events: append-only log of all trade actions
CREATE TABLE IF NOT EXISTS trade_events (
    id SERIAL PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL, -- entry, exit, stop, cancel, etc
    symbol TEXT NOT NULL,
    side TEXT, -- buy/sell
    shares NUMERIC,
    price NUMERIC,
    mode TEXT,
    order_id TEXT,
    parent_order_id TEXT,
    status TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Broker orders snapshot (truth from Alpaca)
CREATE TABLE IF NOT EXISTS broker_orders (
    id SERIAL PRIMARY KEY,
    order_id TEXT NOT NULL,
    symbol TEXT,
    side TEXT,
    order_type TEXT,
    status TEXT,
    qty NUMERIC,
    filled_qty NUMERIC,
    avg_fill_price NUMERIC,
    submitted_at TIMESTAMPTZ,
    filled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Reconciliation runs summary
CREATE TABLE IF NOT EXISTS reconciliation_runs (
    id SERIAL PRIMARY KEY,
    run_time TIMESTAMPTZ NOT NULL,
    matched_count INT,
    unmatched_count INT,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Reconciliation details: one row per compared trade (local vs Alpaca)
CREATE TABLE IF NOT EXISTS reconciliation_details (
    id SERIAL PRIMARY KEY,
    run_id INT,
    broker_parent_order_id TEXT,
    symbol TEXT,
    mode TEXT,
    client_order_id TEXT,
    local_entry_timestamp_utc TIMESTAMPTZ,
    local_exit_timestamp_utc TIMESTAMPTZ,
    local_entry_price NUMERIC,
    alpaca_entry_price NUMERIC,
    local_exit_price NUMERIC,
    alpaca_exit_price NUMERIC,
    local_shares NUMERIC,
    alpaca_entry_qty NUMERIC,
    alpaca_exit_qty NUMERIC,
    local_exit_reason TEXT,
    alpaca_exit_reason TEXT,
    alpaca_exit_order_id TEXT,
    entry_price_diff NUMERIC,
    exit_price_diff NUMERIC,
    match_status TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_trade_events_symbol ON trade_events(symbol);
CREATE INDEX IF NOT EXISTS idx_trade_events_time ON trade_events(event_time);
CREATE INDEX IF NOT EXISTS idx_scan_runs_time ON scan_runs(scan_time);
CREATE INDEX IF NOT EXISTS idx_broker_orders_order_id ON broker_orders(order_id);

CREATE INDEX IF NOT EXISTS idx_reconciliation_details_run_id ON reconciliation_details(run_id);
CREATE INDEX IF NOT EXISTS idx_reconciliation_details_symbol ON reconciliation_details(symbol);
CREATE INDEX IF NOT EXISTS idx_reconciliation_details_parent_order ON reconciliation_details(broker_parent_order_id);
CREATE INDEX IF NOT EXISTS idx_reconciliation_details_status ON reconciliation_details(match_status);

-- Alpaca API audit logs (for debugging & reconciliation verification)
CREATE TABLE IF NOT EXISTS alpaca_api_logs (
    id SERIAL PRIMARY KEY,
    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    method TEXT NOT NULL,
    url TEXT NOT NULL,
    params_json TEXT,
    request_body_json TEXT,
    status_code INT,
    response_body TEXT,
    success BOOLEAN,
    error_message TEXT,
    duration_ms INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alpaca_api_logs_logged_at ON alpaca_api_logs(logged_at);
CREATE INDEX IF NOT EXISTS idx_alpaca_api_logs_method ON alpaca_api_logs(method);

-- Trade lifecycles: one row per trade (OPEN → CLOSED unified)
CREATE TABLE IF NOT EXISTS trade_lifecycles (
    id SERIAL PRIMARY KEY,

    trade_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    mode TEXT,
    side TEXT,
    direction TEXT,

    status TEXT, -- OPEN / CLOSED

    entry_time TIMESTAMPTZ,
    exit_time TIMESTAMPTZ,
    duration_minutes NUMERIC,

    shares NUMERIC,
    entry_price NUMERIC,
    exit_price NUMERIC,
    stop_price NUMERIC,
    target_price NUMERIC,

    exit_reason TEXT,

    -- Signal linkage (optional but important for analytics)
    signal_timestamp TIMESTAMPTZ,
    signal_entry NUMERIC,
    signal_stop NUMERIC,
    signal_target NUMERIC,
    signal_confidence NUMERIC,

    -- Broker linkage
    order_id TEXT,
    parent_order_id TEXT,
    exit_order_id TEXT,

    -- Performance metrics
    realized_pnl NUMERIC,
    realized_pnl_percent NUMERIC,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for trade lifecycles
CREATE INDEX IF NOT EXISTS idx_trade_lifecycles_symbol ON trade_lifecycles(symbol);
CREATE INDEX IF NOT EXISTS idx_trade_lifecycles_status ON trade_lifecycles(status);
CREATE INDEX IF NOT EXISTS idx_trade_lifecycles_entry_time ON trade_lifecycles(entry_time);
CREATE INDEX IF NOT EXISTS idx_trade_lifecycles_trade_key ON trade_lifecycles(trade_key);

ALTER TABLE reconciliation_runs
ADD COLUMN IF NOT EXISTS severity TEXT;
ALTER TABLE reconciliation_runs
ADD COLUMN IF NOT EXISTS mismatch_count INT;
ALTER TABLE reconciliation_runs
ADD COLUMN IF NOT EXISTS run_started_at TIMESTAMPTZ;

ALTER TABLE reconciliation_runs
ADD COLUMN IF NOT EXISTS run_completed_at TIMESTAMPTZ;