# # check api is running 
# from fastapi import FastAPI
# app = FastAPI(title="Sentinel AI API")
# @app.get("/health")
# def health():
#     return {"status": "ok"}

from api.app.db import get_db_connection
from api.app.redis_client import get_redis_client
from api.app.tasks import create_task
from api.app.tasks import get_task
from fastapi import FastAPI
app = FastAPI(title="Sentinel AI API")
@app.get("/db-check")
def db_check():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1;")
    result = cur.fetchone()
    cur.close()
    conn.close()
    return {"db": "connected", "result": result[0]}

@app.get("/redis-check")
def redis_check():
    r = get_redis_client()
    pong = r.ping()
    return {"redis": "connected", "ping": pong}
@app.post("/tasks")
def create_task_api(task_type: str, input_text: str):
    task_id = create_task(task_type, input_text)
    return {
        "task_id": str(task_id),
        "status": "CREATED"
    }

@app.get("/tasks/{task_id}")
def get_task_api(task_id: str):
    task = get_task(task_id)

    if task is None:
        return {"error": "Task not found"}

    return task
