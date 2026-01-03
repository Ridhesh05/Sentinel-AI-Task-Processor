import redis

STREAM_NAME = "ai_task_queue"

def get_redis_client():
    return redis.Redis(
        host="localhost",
        port=6379,
        decode_responses=True
    )
