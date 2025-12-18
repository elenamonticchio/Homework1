import os, json, time
from confluent_kafka import Consumer, Producer, KafkaError
from db import get_connection

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_IN = os.getenv("KAFKA_TOPIC_IN", "to-alert-system")
TOPIC_OUT = os.getenv("KAFKA_TOPIC_OUT", "to-notifier")

consumer_conf = {
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "alert-system",
    "auto.offset.reset": "latest",
    "enable.auto.commit": False
}

producer_conf = {
    "bootstrap.servers": BOOTSTRAP,
    "linger.ms": 100
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)

BATCH_SIZE = 10
processed_in_batch = 0

def delivery_report(err, msg):
    if err:
        print(f"[Kafka] Failed to produce to {TOPIC_OUT}: {err}")
    else:
        print(f"[Kafka] Alert sent to {TOPIC_OUT} offset={msg.offset()}")

def publish_alert(alert: dict):
    producer.produce(
        TOPIC_OUT,
        value=json.dumps(alert).encode("utf-8"),
        callback=delivery_report
    )
    producer.poll(0)

def process_message(message: dict):
    airport = message.get("airport")
    if not airport:
        return

    arrivals = int(message.get("arrivals", 0) or 0)
    departures = int(message.get("departures", 0) or 0)
    timestamp = message.get("timestamp")
    value = arrivals + departures

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT email, high_value, low_value FROM interests WHERE airport = %s", (airport,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    for email, high_value, low_value in rows:
        if high_value is not None and value > int(high_value):
            publish_alert({
                "email": email,
                "airport": airport,
                "condition": "HIGH_THRESHOLD_EXCEEDED",
                "value": value,
                "high_value": int(high_value),
                "timestamp": timestamp
            })
        if low_value is not None and value != 0 and value < int(low_value):
            publish_alert({
                "email": email,
                "airport": airport,
                "condition": "LOW_THRESHOLD_BREACHED",
                "value": value,
                "low_value": int(low_value),
                "timestamp": timestamp
            })

def main():
    global processed_in_batch
    consumer.subscribe([TOPIC_IN])
    print(f"AlertSystem started. Consuming {TOPIC_IN} -> producing {TOPIC_OUT}")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                if processed_in_batch > 0:
                    producer.flush(5)
                    consumer.commit(asynchronous=False)
                    processed_in_batch = 0
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Consumer error: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode("utf-8"))
                process_message(data)
                processed_in_batch += 1

                if processed_in_batch >= BATCH_SIZE:
                    producer.flush(5)
                    consumer.commit(asynchronous=False)
                    processed_in_batch = 0

            except json.JSONDecodeError as e:
                print(f"Malformed JSON at offset {msg.offset()}: {e}")
                consumer.commit(asynchronous=False)

            except Exception as e:
                print(f"Processing error at offset {msg.offset()}: {e}")
                time.sleep(1)

    except KeyboardInterrupt:
        pass

    finally:
        if processed_in_batch > 0:
            try:
                producer.flush(10)
                consumer.commit(asynchronous=False)
            except Exception as e:
                print("Commit finale fallito:", e)

        print("Flushing producer...")
        producer.flush(10)
        consumer.close()

if __name__ == "__main__":
    main()
