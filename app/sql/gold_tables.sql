CREATE TABLE IF NOT EXISTS gold.global_stats (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    total_count BIGINT NOT NULL DEFAULT 0,
    total_sum NUMERIC(18, 2) NOT NULL DEFAULT 0,
    min_price NUMERIC(18, 2),
    max_price NUMERIC(18, 2),
    last_silver_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.load_log (
    id BIGSERIAL PRIMARY KEY,
    layer TEXT NOT NULL,
    file_name TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    records BIGINT NOT NULL DEFAULT 0,
    min_price NUMERIC(18, 2),
    avg_price NUMERIC(18, 2),
    max_price NUMERIC(18, 2),
    chunk_size INTEGER,
    status TEXT NOT NULL,
    details TEXT
);

CREATE INDEX IF NOT EXISTS idx_load_log_layer ON gold.load_log (layer, loaded_at DESC);
