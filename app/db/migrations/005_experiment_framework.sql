CREATE TABLE IF NOT EXISTS experiments (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    strategy_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE paper_signals ADD COLUMN IF NOT EXISTS experiment_id BIGINT;
ALTER TABLE paper_signals ADD COLUMN IF NOT EXISTS experiment_name TEXT;

ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS experiment_id BIGINT;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS experiment_name TEXT;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS strategy_name TEXT;

UPDATE paper_signals
SET experiment_name = coalesce(experiment_name, 'legacy_single_strategy');

UPDATE paper_trades
SET
    experiment_name = coalesce(experiment_name, 'legacy_single_strategy'),
    strategy_name = coalesce(strategy_name, 'micro_momentum_burst_v0');

CREATE INDEX IF NOT EXISTS idx_experiments_name ON experiments (name);
CREATE INDEX IF NOT EXISTS idx_paper_signals_experiment_time ON paper_signals (experiment_name, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_paper_trades_experiment_entry_time ON paper_trades (experiment_name, entry_time DESC);
