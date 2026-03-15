"""
Tests for the Sentinel API service.

Run with:
    pip install -r requirements/dev.txt
    pytest tests/ -v
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock


# ---------------------------------------------------------------------------
# Provide mock env vars before importing the app so startup checks pass
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True, scope="session")
def mock_env(monkeysession):
    pass  # env patching done at import time below; tests mock the deps directly


# We patch dependencies at the module boundary before importing the app.
with (
    patch("app.redis_client.redis.Redis"),
    patch("app.db.psycopg2.connect"),
):
    from main import app

client = TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class TestHealth:
    def test_health_both_up(self):
        with (
            patch("main.redis_ping"),
            patch("main.get_db_connection") as mock_conn,
        ):
            mock_cur = MagicMock()
            mock_cur.execute.return_value = None
            mock_conn.return_value.__enter__ = lambda s: mock_conn.return_value
            mock_conn.return_value.cursor.return_value = mock_cur
            mock_conn.return_value.close.return_value = None
            mock_cur.close.return_value = None

            resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"

    def test_health_redis_down(self):
        from app.exceptions import RedisUnavailableError
        with (
            patch("main.redis_ping", side_effect=RedisUnavailableError("down")),
            patch("main.get_db_connection") as mock_conn,
        ):
            mock_cur = MagicMock()
            mock_conn.return_value.cursor.return_value = mock_cur
            mock_conn.return_value.close.return_value = None
            mock_cur.close.return_value = None

            resp = client.get("/health")
        assert resp.status_code == 503
        assert resp.json()["detail"]["status"] == "unhealthy"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

class TestCreateTask:
    def test_create_task_success(self):
        with patch("main.check_rate_limit", return_value=(True, 9, 60)):
            with patch("main.create_task", return_value="123456789"):
                resp = client.post(
                    "/tasks",
                    json={"task_type": "summarize", "input_text": "Hello world"},
                )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "QUEUED"
        assert "task_id" in data

    def test_create_task_rate_limited(self):
        with patch("main.check_rate_limit", return_value=(False, 0, 30)):
            resp = client.post(
                "/tasks",
                json={"task_type": "summarize", "input_text": "Hello world"},
            )
        assert resp.status_code == 429

    def test_create_task_missing_fields(self):
        resp = client.post("/tasks", json={"task_type": "summarize"})
        assert resp.status_code == 422

    def test_create_task_input_too_long(self):
        with patch("main.check_rate_limit", return_value=(True, 9, 60)):
            resp = client.post(
                "/tasks",
                json={"task_type": "summarize", "input_text": "x" * 6000},
            )
        assert resp.status_code == 422


class TestGetTask:
    def test_get_task_not_found(self):
        with patch("main.get_task", return_value=None):
            resp = client.get("/tasks/999999999999999999")
        assert resp.status_code == 404

    def test_get_task_found(self):
        mock_task = {
            "id": "123",
            "task_type": "summarize",
            "status": "COMPLETED",
            "output_text": "Summary here",
            "error": None,
            "queued_at": None,
            "started_at": None,
            "completed_at": None,
        }
        with patch("main.get_task", return_value=mock_task):
            resp = client.get("/tasks/123")
        assert resp.status_code == 200
        assert resp.json()["status"] == "COMPLETED"


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

class TestMetrics:
    def test_metrics_returns_prometheus_format(self):
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert "text/plain" in resp.headers["content-type"]
        assert b"api_requests_total" in resp.content or b"# HELP" in resp.content


# ---------------------------------------------------------------------------
# List tasks
# ---------------------------------------------------------------------------

class TestListTasks:
    def test_list_tasks(self):
        with patch("main.get_recent_tasks", return_value={"tasks": []}):
            resp = client.get("/tasks")
        assert resp.status_code == 200
        assert "tasks" in resp.json()
