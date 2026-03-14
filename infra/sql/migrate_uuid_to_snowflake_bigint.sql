-- Migration: tasks.id from UUID to BIGINT (Snowflake IDs)
-- WARNING: This drops the tasks table and recreates it. All existing task rows will be lost.
--
-- If you see: "operator does not exist: uuid = bigint" then your DB still has the old UUID schema.
--
-- Run migration (from repo root):
--   docker exec -i sentinel_postgres psql -U sentinel -d sentinel_db < infra/sql/migrate_uuid_to_snowflake_bigint.sql
--
-- Or with local psql:
--   PGPASSWORD=sentinel psql -h localhost -U sentinel -d sentinel_db -f infra/sql/migrate_uuid_to_snowflake_bigint.sql

DROP TABLE IF EXISTS tasks;

CREATE TABLE tasks (
    id BIGINT PRIMARY KEY,
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
