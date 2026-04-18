CREATE TABLE IF NOT EXISTS market_candles (
    id BIGSERIAL PRIMARY KEY,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    open_time TIMESTAMPTZ NOT NULL,
    close_time TIMESTAMPTZ NOT NULL,
    open NUMERIC(28, 12) NOT NULL,
    high NUMERIC(28, 12) NOT NULL,
    low NUMERIC(28, 12) NOT NULL,
    close NUMERIC(28, 12) NOT NULL,
    volume NUMERIC(28, 12) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (exchange, symbol, timeframe, open_time)
);

CREATE TABLE IF NOT EXISTS market_quotes (
    id BIGSERIAL PRIMARY KEY,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    bid NUMERIC(28, 12) NOT NULL,
    ask NUMERIC(28, 12) NOT NULL,
    spread NUMERIC(28, 12) NOT NULL,
    spread_bps NUMERIC(18, 8) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS market_trades (
    id BIGSERIAL PRIMARY KEY,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    trade_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    price NUMERIC(28, 12) NOT NULL,
    amount NUMERIC(28, 12) NOT NULL,
    side TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (exchange, symbol, trade_id)
);

CREATE TABLE IF NOT EXISTS order_book_snapshots (
    id BIGSERIAL PRIMARY KEY,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    depth INTEGER NOT NULL,
    bids_json JSONB NOT NULL,
    asks_json JSONB NOT NULL,
    best_bid NUMERIC(28, 12) NOT NULL,
    best_ask NUMERIC(28, 12) NOT NULL,
    spread NUMERIC(28, 12) NOT NULL,
    imbalance NUMERIC(18, 8) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS paper_signals (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    side TEXT,
    confidence NUMERIC(8, 4),
    reason TEXT,
    features_json JSONB,
    decision TEXT NOT NULL,
    skip_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS paper_trades (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    status TEXT NOT NULL,
    entry_time TIMESTAMPTZ,
    exit_time TIMESTAMPTZ,
    entry_price NUMERIC(28, 12),
    exit_price NUMERIC(28, 12),
    quantity NUMERIC(28, 12),
    notional_idr NUMERIC(28, 2),
    take_profit_price NUMERIC(28, 12),
    stop_loss_price NUMERIC(28, 12),
    pnl_idr NUMERIC(28, 2),
    pnl_percent NUMERIC(18, 8),
    fee_estimate_idr NUMERIC(28, 2),
    slippage_estimate_idr NUMERIC(28, 2),
    exit_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS daily_performance (
    id BIGSERIAL PRIMARY KEY,
    trading_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    realized_pnl_idr NUMERIC(28, 2) NOT NULL DEFAULT 0,
    unrealized_pnl_idr NUMERIC(28, 2) NOT NULL DEFAULT 0,
    trade_count INTEGER NOT NULL DEFAULT 0,
    win_count INTEGER NOT NULL DEFAULT 0,
    loss_count INTEGER NOT NULL DEFAULT 0,
    win_rate NUMERIC(18, 8),
    profit_factor NUMERIC(18, 8),
    max_drawdown_idr NUMERIC(28, 2) NOT NULL DEFAULT 0,
    daily_stop_triggered BOOLEAN NOT NULL DEFAULT false,
    profit_target_triggered BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (trading_date, symbol)
);

CREATE TABLE IF NOT EXISTS market_events (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    title TEXT NOT NULL,
    summary TEXT,
    impact_level TEXT,
    affected_assets TEXT[],
    raw_url TEXT,
    llm_model TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS service_health (
    id BIGSERIAL PRIMARY KEY,
    service_name TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    last_success_at TIMESTAMPTZ,
    message TEXT,
    metadata_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_market_candles_symbol_time ON market_candles (symbol, open_time DESC);
CREATE INDEX IF NOT EXISTS idx_market_quotes_symbol_time ON market_quotes (symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_market_trades_symbol_time ON market_trades (symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_order_book_symbol_time ON order_book_snapshots (symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_service_health_name_time ON service_health (service_name, timestamp DESC);
