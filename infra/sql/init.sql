-- Tasks table (Snowflake BIGINT IDs for time-ordered, globally unique identifiers)
CREATE TABLE IF NOT EXISTS tasks (
    id           BIGINT PRIMARY KEY,   -- Snowflake ID (time-ordered 64-bit)
    task_type    TEXT        NOT NULL,
    status       TEXT        NOT NULL,
    input_text   TEXT        NOT NULL,
    output_text  TEXT,
    error        TEXT,
    queued_at    TIMESTAMP,
    started_at   TIMESTAMP,
    completed_at TIMESTAMP,
    created_at   TIMESTAMP   NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP   NOT NULL DEFAULT NOW()
);

-- Index: efficient filtering by status (e.g. WHERE status = 'FAILED')
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks (status);

-- Index: efficient recent-task queries (ORDER BY created_at DESC)
CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks (created_at DESC);

-- Auto-update updated_at on every row change
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS set_tasks_updated_at ON tasks;
CREATE TRIGGER set_tasks_updated_at
    BEFORE UPDATE ON tasks
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
