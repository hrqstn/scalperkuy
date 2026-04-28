ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS gross_pnl_idr NUMERIC(28, 2);
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS gross_pnl_percent NUMERIC(18, 8);
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS hold_seconds INTEGER;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS max_favorable_excursion_bps NUMERIC(18, 8);
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS max_adverse_excursion_bps NUMERIC(18, 8);
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS horizon_3m_label TEXT;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS horizon_5m_label TEXT;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS horizon_10m_label TEXT;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS label_source TEXT;

CREATE INDEX IF NOT EXISTS idx_paper_trades_status_exit_time ON paper_trades (status, exit_time DESC);
