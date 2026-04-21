CREATE TABLE IF NOT EXISTS journal_entries (
    id BIGSERIAL PRIMARY KEY,
    entry_date DATE NOT NULL,
    entry_type TEXT NOT NULL,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    metrics_json JSONB NOT NULL,
    llm_model TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (entry_date, entry_type)
);

CREATE INDEX IF NOT EXISTS idx_journal_entries_date_type ON journal_entries (entry_date DESC, entry_type);
