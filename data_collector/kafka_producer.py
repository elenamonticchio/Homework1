import os, json
from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC_FLIGHTS", "to-alert-system")

producer_conf = {
    "bootstrap.servers": BOOTSTRAP,
    "acks": "all",
    "retries": 3,
    "linger.ms": 10,
}

producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err:
        print(f"[Kafka] Delivery failed: {err}")
    else:
        print(f"[Kafka] Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def publish_flights_update(message: dict):
    producer.produce(
        TOPIC,
        value=json.dumps(message).encode("utf-8"),
        callback=delivery_report
    )
    producer.poll(0)

def flush_producer(timeout: int = 5):
    producer.flush(timeout)

