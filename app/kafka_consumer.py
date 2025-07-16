from aiokafka import AIOKafkaConsumer
import asyncio
import json
from app.redis_client import set_job_status

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
TOPIC_NAME = "llm-jobs"

consumer = None

async def consume_messages():
    global consumer
    consumer = AIOKafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="llm-consumer-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    await consumer.start()
    try:
        print("🟢 Kafka consumer started...")
        async for msg in consumer:
            data = msg.value
            if "job_id" in data and "prompt" in data:
                print(f"📥 Got job: {data}")
                await process_job(data["job_id"], data["prompt"])
            else:
                print(f"⚠️ Invalid message format: {data}")
    finally:
        await consumer.stop()

async def stop_kafka_consumer():
    global consumer
    if consumer:
        await consumer.stop()
        print("🛑 Kafka consumer stopped.")

# ✅ Mock LLM processing
async def process_job(job_id: str, prompt: str):
    await set_job_status(job_id, "processing")
    await asyncio.sleep(30)  # Simulated work
    result = f"✅ Kafka processed: {prompt}"
    await set_job_status(job_id, "completed", result=result)
