import json
from app.redis_client import pop_job, set_job_status, redis_client
import asyncio


async def process_job(job_id: str, prompt: str):
    await set_job_status(job_id, "processing")
    await asyncio.sleep(20)  # Simulate LLM call
    result = f"Processed: {prompt}"
    await set_job_status(job_id, "done", result)

async def worker_loop():
    print("👷 Worker waiting for jobs...")
    while True:
        job_id = await pop_job()
        if job_id:
            job_data = await redis_client.get(f"job:{job_id}")
            if job_data:
                prompt = json.loads(job_data)["prompt"]
                await process_job(job_id, prompt)
        else:
            await asyncio.sleep(1)


async def stop_redis_worker():
    global worker_should_stop
    worker_should_stop = True
