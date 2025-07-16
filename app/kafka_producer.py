import json
from aiokafka import AIOKafkaProducer
import asyncio

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"

TOPIC_NAME = "llm-jobs"

producer = None

async def start_kafka_producer():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start()
    print("🚀 Kafka producer ready")

async def send_job_to_kafka(prompt: str, job_id: str):
    await producer.send_and_wait(TOPIC_NAME, {
        "job_id": job_id,
        "prompt": prompt
    })
    print(f"📤 Sent job to Kafka: {prompt}")


async def send_message(message: str, topic: str = "fastapi-topic"):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        await producer.send_and_wait(topic, message.encode('utf-8'))
    finally:
        await producer.stop()
