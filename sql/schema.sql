CREATE TABLE IF NOT EXISTS transaction_scores (
    ingestion_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id TEXT NOT NULL UNIQUE,
    score DOUBLE PRECISION NOT NULL CHECK (score BETWEEN 0 AND 1),
    fraud_flag SMALLINT NOT NULL CHECK (fraud_flag IN (0, 1)),
    model_version TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    us_state TEXT NOT NULL DEFAULT 'unknown',
    merch TEXT NOT NULL DEFAULT 'unknown',
    cat_id TEXT NOT NULL DEFAULT 'unknown',
    amount DOUBLE PRECISION CHECK (amount >= 0)
);
CREATE INDEX IF NOT EXISTS scores_processed_at ON transaction_scores(processed_at);
CREATE INDEX IF NOT EXISTS scores_segments_time ON transaction_scores(us_state, merch, processed_at);
