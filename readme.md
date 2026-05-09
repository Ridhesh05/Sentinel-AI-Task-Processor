# Sentinel AI Task Processor

A distributed AI background job system with **Redis Streams**, **PostgreSQL**, **Google Gemini**, and **real-time monitoring** (Redis Pub/Sub → **Server-Sent Events**).

---

## Quick Start

```bash
cp .env.example .env
# Edit .env — set GEMINI_API_KEY

docker compose -f docker-compose.dev.yml up -d
```

**URLs:**
- Frontend: http://localhost:3000
- API docs: http://localhost:8000/docs
- Health: http://localhost:8000/health
- SSE stream: http://localhost:9000/events

---

## Architecture

```
Client POST /tasks
       │
       ▼
API (FastAPI)
  ├── Rate Limit Check (Redis)
  ├── Generate Snowflake ID
  ├── XADD to Redis Stream
  └── Return task_id immediately

Worker (XREADGROUP loop)
  ├── Read from Stream
  ├── Materialise row in PostgreSQL
  ├── Call Gemini
  └── PUBLISH events to Redis Pub/Sub

Monitor (SSE)
  └── Subscribe to Pub/Sub → stream to browsers
```

---

## Makefile Commands

```bash
make help           # Show all commands
make install        # Install dependencies
make lint           # Run linter
make fmt            # Format code
make docker-up      # Start services
make docker-down    # Stop services
make docker-logs    # View logs
make clean          # Clean cache
```

---

## Project Structure

```
core/               # Shared library
  config/           # Pydantic settings (env vars + validation)
  db.py             # PostgreSQL client
  redis.py          # Redis client (sync + async)
  snowflake.py      # 64-bit ID generator
  exceptions.py     # Exception hierarchy
  logging.py        # Structured logging
  metrics/          # Prometheus metrics

services/
  api/              # FastAPI REST service (Port 8000)
  worker/           # Redis Streams consumer (Port 9100)
  monitor/          # SSE streaming (Port 9000)

infra/sql/          # Database schema
frontend/           # Static HTML + Nginx
```

---

## Configuration

All via environment variables (see `.env.example`):

| Variable | Description |
|----------|-------------|
| `GEMINI_API_KEY` | Required for worker |
| `DB_HOST/PORT/NAME/USER/PASSWORD` | PostgreSQL |
| `REDIS_HOST/PORT` | Redis |
| `RATE_LIMIT / RATE_WINDOW` | API rate limiting |
| `LOG_LEVEL` | DEBUG/INFO/WARNING/ERROR |
| `LOG_FORMAT` | text/json (production) |

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check (Redis + Postgres) |
| GET | `/health/live` | Liveness probe |
| GET | `/health/ready` | Readiness probe |
| GET | `/metrics` | Prometheus metrics |
| POST | `/tasks` | Create task |
| GET | `/tasks` | List recent tasks |
| GET | `/tasks/{id}` | Get task details |
| POST | `/tasks/{id}/retry` | Retry failed task |
| GET | `/events` | SSE stream (monitor) |

---

## Docker Services

| Service | Port | Image |
|---------|------|-------|
| api | 8000 | FastAPI |
| worker | 9100 | Python (Prometheus metrics) |
| monitor | 9000 | FastAPI (SSE) |
| frontend | 3000 | Nginx |
| postgres | 5432 | postgres:15 |
| redis | 6379 | redis:7 |
| adminer | 8080 | DB admin UI |