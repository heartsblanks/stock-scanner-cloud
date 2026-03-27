

-- Scan runs: one row per scan execution
CREATE TABLE IF NOT EXISTS scan_runs (
    id SERIAL PRIMARY KEY,
    scan_time TIMESTAMP NOT NULL,
    mode TEXT,
    scan_source TEXT,
    market_phase TEXT,
    candidate_count INT,
    placed_count INT,
    skipped_count INT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Trade events: append-only log of all trade actions
CREATE TABLE IF NOT EXISTS trade_events (
    id SERIAL PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    event_type TEXT NOT NULL, -- entry, exit, stop, cancel, etc
    symbol TEXT NOT NULL,
    side TEXT, -- buy/sell
    shares NUMERIC,
    price NUMERIC,
    mode TEXT,
    order_id TEXT,
    parent_order_id TEXT,
    status TEXT,
    created_at TIMESTAMP DEFAULT NOW()
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
    submitted_at TIMESTAMP,
    filled_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Reconciliation runs summary
CREATE TABLE IF NOT EXISTS reconciliation_runs (
    id SERIAL PRIMARY KEY,
    run_time TIMESTAMP NOT NULL,
    matched_count INT,
    unmatched_count INT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_trade_events_symbol ON trade_events(symbol);
CREATE INDEX IF NOT EXISTS idx_trade_events_time ON trade_events(event_time);
CREATE INDEX IF NOT EXISTS idx_scan_runs_time ON scan_runs(scan_time);
CREATE INDEX IF NOT EXISTS idx_broker_orders_order_id ON broker_orders(order_id);


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
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alpaca_api_logs_logged_at ON alpaca_api_logs(logged_at);
CREATE INDEX IF NOT EXISTS idx_alpaca_api_logs_method ON alpaca_api_logs(method);