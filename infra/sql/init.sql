CREATE TABLE IF NOT EXISTS tasks (
    id BIGINT PRIMARY KEY,  -- Snowflake ID (time-ordered 64-bit)
    task_type TEXT NOT NULL,
    status TEXT NOT NULL,
    input_text TEXT NOT NULL,
    output_text TEXT,
    error TEXT,
    queued_at TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
