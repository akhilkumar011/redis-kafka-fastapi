import redis.asyncio as redis
import json

redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)
QUEUE_NAME = "llm-jobs"


async def set_redis_value(key: str, value: str):
    await redis_client.set(key, value, ex=300)  # 5 mins TTL

async def get_redis_value(key: str):
    return await redis_client.get(key)

async def push_job(job_id: str, prompt: str):
    await redis_client.set(f"job:{job_id}", json.dumps({
        "status": "queued",
        "prompt": prompt
    }))
    await redis_client.rpush(QUEUE_NAME, job_id)

async def pop_job():
    result = await redis_client.blpop(QUEUE_NAME, timeout=5)
    if result:
        _, job_id = result
        return job_id
    return None

async def set_job_status(job_id: str, status: str, result: str = None):
    raw = await redis_client.get(f"job:{job_id}")
    if not raw:
        print("❌ No data found in Redis!")
        return
    data = json.loads(raw)
    data["status"] = status
    print(data,'data is set redis')
    if result:
        data["result"] = result
    await redis_client.set(f"job:{job_id}", json.dumps(data))

async def get_job_status(job_id: str):
    raw = await redis_client.get(f"job:{job_id}")
    if raw:
        return json.loads(raw)
    return {"error": "Job not found"}

async def create_job(job_id: str, prompt: str):
    job_data = {
        "status": "queued",
        "prompt": prompt
    }
    await redis_client.set(f"job:{job_id}", json.dumps(job_data), ex=3600)

async def job_exists(job_id: str) -> bool:
    return await redis_client.exists(f"job:{job_id}") == 1
