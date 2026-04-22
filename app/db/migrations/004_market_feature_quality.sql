ALTER TABLE market_features_1m
ADD COLUMN IF NOT EXISTS quality_score NUMERIC(5, 2);

ALTER TABLE market_features_1m
ADD COLUMN IF NOT EXISTS is_tradeable_minute BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE market_features_1m
ADD COLUMN IF NOT EXISTS quality_flags JSONB NOT NULL DEFAULT '[]'::jsonb;

CREATE INDEX IF NOT EXISTS idx_market_features_1m_quality
ON market_features_1m (symbol, is_tradeable_minute, open_time DESC);
