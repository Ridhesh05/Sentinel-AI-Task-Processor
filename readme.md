# Sentinel AI Task Processor 🚦⚙️🤖

A distributed AI background job system with **Redis Streams**, **PostgreSQL**, **Google Gemini**, and **real-time monitoring** (Redis Pub/Sub → **Server-Sent Events**).

---

## What is this?

Sentinel is a **task orchestration backend** for AI-heavy work (summarize, classify, tag, or any custom `task_type`). The API returns a **`task_id` immediately**; a **worker** consumes jobs from a Redis Stream, runs Gemini, and persists results in PostgreSQL.

---

## Why it exists

Doing inference inside synchronous HTTP requests under load causes timeouts, overload, and crashes. Sentinel **decouples**:

**API ingestion (Redis)** → **queue (Redis Streams + consumer group)** → **worker (Gemini + Postgres)**

---

## Architecture (how it really works)

### High-level flow

1. Client **POST /tasks** → API validates input and applies **per-client rate limiting** (Redis).
2. API generates a **Snowflake ID** (`task_id`), **XADD**s the full payload to **`ai_task_queue`**, and **SETEX**s metadata in Redis (`task:meta:{id}`) so **GET /tasks/{id}** works before the worker writes Postgres.
3. **No PostgreSQL write on create** — the hot path stays Redis-only for scale.
4. **Worker** reads via **XREADGROUP**, **materializes** the row in Postgres on first consume, then **PROCESSING** → Gemini → **COMPLETED** or **FAILED**.
5. Worker **PUBLISH**es JSON events to Redis channel **`task_events`** (includes **`event_id`** + **`task_id`**).
6. **Monitor** subscribes to Pub/Sub and streams events to browsers via **SSE** (`GET /events`).
7. **Frontend** polls the API and listens to the SSE stream for live updates.

### Status lifecycle

```
QUEUED → PROCESSING → COMPLETED
                    ↘ FAILED  (retry via POST /tasks/{id}/retry when FAILED)
```

---

## Components

| Component | Role | Tech |
|-----------|------|------|
| **Frontend** | Submit tasks, history, status, live events | Static HTML + Nginx |
| **API** | Create/list/get/retry tasks, rate limits, health, Prometheus | FastAPI |
| **Worker** | Stream consumer, Gemini, DB updates, retries, metrics | Python + `google-genai` |
| **Redis** | Stream queue, consumer group, rate limits, task cache, Pub/Sub | Redis 7 |
| **PostgreSQL** | Durable tasks (`id` = **BIGINT** Snowflake), indexes, `updated_at` trigger | Postgres 15 |
| **Monitor** | Pub/Sub → **SSE** fan-out, `/metrics` | FastAPI |
| **Adminer** | Optional DB UI | Adminer |

---

## Key features

- **Redis-first task creation** — stream + cache; worker inserts into Postgres.
- **Snowflake IDs** — time-ordered 64-bit `task_id` (and per-event `event_id` on Pub/Sub).
- **Consumer group** + **XPENDING / XCLAIM** for stuck messages (crash recovery).
- **Idempotency** — worker skips tasks already **COMPLETED** or **FAILED**.
- **Retries** — configurable Gemini/DB **retry with backoff** in the worker; **POST /tasks/{id}/retry** for failed jobs.
- **Rate limiting** — Redis counters; **429** when over limit. Client key: **`X-Client-ID`** → **`X-Forwarded-For`** (first IP) → **`request.client.host`**.
- **Input validation** — `task_type` / `input_text` length limits (see **`INPUT_TEXT_MAX_LENGTH`**).
- **Health** — **`GET /health`** on API returns **200** only if Redis **and** Postgres are up (**503** otherwise).
- **Prometheus** — **`GET /metrics`** on API and Monitor; worker exposes **`http://<worker>:9100/metrics`**.
- **Structured logging** — `LOG_LEVEL` env (see **`.env.example`**).
- **SSE monitoring** — **`GET http://localhost:9000/events`** (not WebSocket).

---

## Ports (Docker Compose)

| Service | Port | Notes |
|---------|------|--------|
| API | **8000** | REST + `/health` + `/metrics` |
| Monitor | **9000** | `/health`, `/metrics`, **`/events`** (SSE) |
| Worker metrics | **9100** | Prometheus scrape (mapped in compose) |
| Frontend | **3000** | Nginx |
| Adminer | **8080** | DB admin |
| Postgres | **5432** | |
| Redis | **6379** | |

---

## Quick start (Docker)

### Requirements

- Docker + Docker Compose v2

### 1. Environment

Copy the template and set your Gemini key:

```bash
cp .env.example .env
# Edit .env — set GEMINI_API_KEY (required for the worker)
```

> Do **not** commit `.env` (it is gitignored).

### 2. Run the stack

From the **repository root**:

```bash
docker compose -f docker-compose.full.yml up --build
```

### 3. Try it

- **Frontend:** http://localhost:3000  
- **API docs:** http://localhost:8000/docs  
- **API health:** http://localhost:8000/health  
- **SSE events:** http://localhost:9000/events (open in browser or `curl -N`)  
- **Adminer:** http://localhost:8080 (server: `postgres`, user/db: `sentinel`)

---

## API summary

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Redis + Postgres OK → 200, else 503 |
| GET | `/metrics` | Prometheus text format |
| POST | `/tasks` | Create task → `{ "task_id", "status": "QUEUED" }` |
| GET | `/tasks` | Recent tasks (`limit`) |
| GET | `/tasks/{task_id}` | Task detail (404 if unknown id) |
| POST | `/tasks/{task_id}/retry` | Re-queue **FAILED** task (400 if not retryable) |

Creating a task requires **Redis**. Reads may use **Postgres** + **Redis** cache; **503** if a dependency is down after retries.

---

## Database schema

- Table **`tasks`**: **`id BIGINT PRIMARY KEY`** (Snowflake), `task_type`, `status`, `input_text`, `output_text`, `error`, timestamps, `created_at` / `updated_at`.
- Initialized by **`infra/sql/init.sql`** on first Postgres container start.

### Migrating from old UUID schema

If you see **`operator does not exist: uuid = bigint`**, your DB still has UUID `id`. Apply:

```bash
docker exec -i sentinel_postgres psql -U sentinel -d sentinel_db < infra/sql/migrate_uuid_to_snowflake_bigint.sql
```

**Warning:** that migration **drops and recreates** `tasks` (data loss). See script header for details.

---

## Local development (without full stack)

Install dependencies per service (paths from repo root):

```bash
pip install -r requirements/api.txt    # API
pip install -r requirements/worker.txt # Worker
pip install -r requirements/monitor.txt # Monitor
pip install -r loadtest/requirements.txt
```

Run Redis and Postgres locally (or use Docker only for those), set env vars to match **`.env.example`**, then start **uvicorn** / **worker** as in each service’s Dockerfile `CMD`.

---

## Load testing

Rate limits are **per client key** (see Architecture). Example:

```bash
pip install -r loadtest/requirements.txt
python loadtest/attack.py --url http://localhost:8000 --num-clients 5 --requests-per-client 20 --concurrent 25
```

Use **`X-Client-ID`** (or multiple clients) to simulate separate buckets and observe **200** vs **429**.

---

## Project layout

```
├── api/                 # FastAPI app (main.py, app/)
├── worker/              # Stream consumer + Gemini
├── monitor/             # SSE + Redis Pub/Sub bridge
├── frontend/            # Static UI
├── infra/sql/           # init.sql, UUID→BIGINT migration
├── requirements/        # Consolidated pip requirements (used by Dockerfiles)
├── loadtest/            # Optional load script
├── tests/               # pytest (when run locally/CI)
├── docker-compose.full.yml
├── .env.example
└── readme.md
```

---

## Redis Streams crash recovery

If a worker dies after reading a message but before **XACK**, the entry stays **pending**. Another consumer can recover it with **XPENDING** + **XCLAIM** (implemented in the worker). Finished tasks are not re-run thanks to the **idempotency** check.

---

