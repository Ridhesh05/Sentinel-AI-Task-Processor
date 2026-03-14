# Sentinel AI Task Processor 🚦⚙️🤖  
A distributed AI background job system with real-time monitoring using **Redis Streams + PostgreSQL + Gemini**.

---

## 🚀 What is this?
Sentinel is a production-style **task orchestration backend** where users submit AI-heavy tasks like:

- Summarization  
- Classification  
- Tagging  

The API responds instantly with a `task_id`, and the task is processed asynchronously by worker services.

---

## ✅ Why this exists (Problem Solved)
If 10,000 users hit an AI endpoint at the same time, doing AI inference inside the API request will:

❌ slow down API  
❌ cause timeouts  
❌ overload server  
❌ crash services  

✅ Sentinel avoids this by decoupling:
**API ingestion** → **Queue** → **Worker execution**

This makes the system stable, scalable, and observable.

---

## 🏗️ Architecture (How it works)

### High-Level Flow
1. User submits task to API  
2. API stores task in PostgreSQL (`CREATED`)  
3. API queues `task_id` in Redis Streams (`QUEUED`)  
4. Worker consumes task from stream (`PROCESSING`)  
5. Worker runs Gemini inference  
6. Worker stores output in PostgreSQL (`COMPLETED` / `FAILED`)  
7. Worker emits live events via Redis Pub/Sub  
8. Monitor forwards events to UI using WebSocket  

---

## 🧩 Components

| Component | Responsibility | Tech |
|----------|----------------|------|
| Frontend | Submit task + show status + logs + results + history | HTML + Nginx |
| API Service | Create/get/retry tasks + rate limiting | FastAPI |
| Redis Streams | Task queue + consumer groups | Redis 7 |
| Worker | Processes tasks using Gemini + updates DB | Python |
| PostgreSQL | Durable task history + outputs + timelines | Postgres 15 |
| Monitor | Pub/Sub → WebSocket real-time dashboard | FastAPI WS |

---

## ⭐ Key Features
✅ Async AI task processing  
✅ Redis Streams Consumer Groups for scaling  
✅ Crash recovery using **XPENDING + XCLAIM**  
✅ At-least-once delivery + idempotency guard  
✅ Task Retry feature for failed tasks  
✅ Real-time monitoring via Redis Pub/Sub + WebSocket  
✅ Timeline tracking (`queued_at`, `started_at`, `completed_at`)  
✅ Rate limiting using Redis (`429 Too Many Requests`)  
✅ Responsive UI + task history + status filtering  

---

## 🔥 Redis Streams Crash Recovery (Industrial concept)
If a worker crashes mid-task, the message stays in Redis as **pending**.

Another worker can claim it using:

- `XPENDING` → detect stuck messages  
- `XCLAIM` → reassign message to healthy worker  

This is similar to how Kafka-style systems handle failures.

---

## 📦 Setup (Run Locally)

### ✅ Requirements
- Docker
- Docker Compose

---

### 1️⃣ Create `.env` file in project root
> **Do NOT commit this file to GitHub.**

```env
GEMINI_API_KEY=YOUR_GEMINI_KEY
```

### 2️⃣ Load testing (traffic / rate-limit check)
Rate limiting is per **client**. The API uses `request.client.host` by default, or **`X-Client-ID`** / **`X-Forwarded-For`** when set (e.g. for load tests or behind a proxy).

From the repo root:
```bash
pip install -r loadtest/requirements.txt
python loadtest/attack.py --url http://localhost:8000 --num-clients 5 --requests-per-client 20 --concurrent 25
```
- `--num-clients`: virtual clients (each has its own rate-limit bucket).
- Increase clients to see total traffic the API can handle; you’ll see 200 vs 429 counts and RPS.
