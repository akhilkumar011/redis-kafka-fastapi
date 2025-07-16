import uuid
from app.llm_worker import stop_redis_worker, worker_loop
from fastapi import FastAPI
from app.kafka_producer import send_job_to_kafka, send_message, start_kafka_producer
from app.redis_client import get_job_status, push_job, set_job_status, set_redis_value, get_redis_value
from app.kafka_consumer import consume_messages, stop_kafka_consumer
import asyncio


app = FastAPI()

@app.on_event("startup")
async def startup_event():
    await start_kafka_producer()

    print("🚀 Starting Kafka consumer...")
    asyncio.create_task(consume_messages())

    print("🔁 Starting Redis task worker...")
    asyncio.create_task(worker_loop())


@app.get("/")
def read_root():
    return {"message": "Kafka + Redis + FastAPI"}

@app.post("/send/{msg}")
async def send_kafka(msg: str):
    await send_message(msg)
    return {"status": "sent"}

@app.post("/redis/{key}/{value}")
async def write_redis(key: str, value: str):
    await set_redis_value(key, value)
    return {"status": "stored"}

@app.get("/redis/{key}")
async def read_redis(key: str):
    val = await get_redis_value(key)
    return {"value": val}

@app.post("/llm/{prompt}")
async def cached_llm(prompt: str):
    cached = await get_redis_value(prompt)
    if cached:
        return {"cached": True, "response": cached}
    
    # Simulate heavy processing like calling OpenAI
    response = f"LLM output for: {prompt}"
    
    await set_redis_value(prompt, response)
    return {"cached": False, "response": response}


@app.post("/llm/job/{prompt}")
async def queue_llm_prompt(prompt: str):
    job_id = str(uuid.uuid4())
    await push_job(job_id, prompt)
    return {"job_id": job_id}

@app.get("/llm/status/{job_id}")
async def get_llm_status(job_id: str):
    status = await get_job_status(job_id)
    return status


@app.on_event("shutdown")
async def shutdown_event():
    print("🛑 Shutting down background services...")

    # Optionally notify Kafka consumer to stop
    await stop_kafka_consumer()

    # Notify Redis worker to stop
    await stop_redis_worker()

    print("✅ Shutdown complete.")


@app.post("/kafka/job/{prompt}")
async def queue_kafka_job(prompt: str):
    job_id = str(uuid.uuid4())
    await push_job(job_id, prompt)
    await send_job_to_kafka(prompt, job_id)
    return {"job_id": job_id}
