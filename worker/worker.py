import time
import redis
import psycopg2

STREAM_NAME = "ai_task_queue"
GROUP_NAME = "ai_workers"
CONSUMER_NAME = "worker-1"

# Redis connection

redis_client = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True
)

def reclaim_stuck_tasks():
    reclaimed = []

    pending = redis_client.xpending_range(
        STREAM_NAME,
        GROUP_NAME,
        min='-',
        max='+',
        count=10
    )

    for entry in pending:
        message_id = entry['message_id']
        idle_time = entry['time_since_delivered']

        if idle_time > 10000:
            messages = redis_client.xclaim(
                STREAM_NAME,
                GROUP_NAME,
                CONSUMER_NAME,
                min_idle_time=10000,
                message_ids=[message_id]
            )

            reclaimed.extend(messages)

    return reclaimed


# PostgreSQL connection helper
def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="sentinel_db",
        user="sentinel",
        password="sentinel"
    )

# Create consumer group (idempotent)
try:
    redis_client.xgroup_create(
        STREAM_NAME,
        GROUP_NAME,
        id="0",
        mkstream=True
    )
except redis.exceptions.ResponseError:
    # Group already exists
    pass

print("Worker started. Waiting for tasks...")

print("Worker started. Waiting for tasks...")

def process_task(message_id, data):
    task_id = data["task_id"]
    print(f"Processing task {task_id}")

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        "UPDATE tasks SET status=%s WHERE id=%s",
        ("PROCESSING", task_id)
    )
    conn.commit()

    time.sleep(3)  # simulate work

    cur.execute(
        "UPDATE tasks SET status=%s WHERE id=%s",
        ("COMPLETED", task_id)
    )
    conn.commit()

    cur.close()
    conn.close()

    redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)

    print(f"Completed task {task_id}")

while True:
    # 1️⃣ Reclaim and PROCESS stuck tasks
    reclaimed = reclaim_stuck_tasks()
    for message_id, data in reclaimed:
        process_task(message_id, data)

    # 2️⃣ Read NEW tasks
    messages = redis_client.xreadgroup(
        GROUP_NAME,
        CONSUMER_NAME,
        {STREAM_NAME: ">"},
        count=1,
        block=5000
    )

    if not messages:
        continue

    for stream, entries in messages:
        for message_id, data in entries:
            process_task(message_id, data)
            task_id = data["task_id"]
            print(f"Processing task {task_id}")

            conn = get_db_connection()
            cur = conn.cursor()

            # Mark task as PROCESSING
            cur.execute(
                "UPDATE tasks SET status=%s WHERE id=%s",
                ("PROCESSING", task_id)
            )
            conn.commit()

            # Simulate work
            time.sleep(3)

            # Mark task as COMPLETED
            cur.execute(
                "UPDATE tasks SET status=%s WHERE id=%s",
                ("COMPLETED", task_id)
            )
            conn.commit()

            cur.close()
            conn.close()

            # Acknowledge message
            redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)

            print(f"Completed task {task_id}")
