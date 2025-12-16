from confluent_kafka import Consumer
import os, json, time
from mailer import send_email

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC_OUT", "to-notifier")

consumer_conf = {
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "alert-notifier",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}

consumer = Consumer(consumer_conf)

def build_email(alert: dict):
    airport = alert.get("airport", "UNKNOWN")
    condition = alert.get("condition", "UNKNOWN")
    value = alert.get("value")
    hi = alert.get("high_value")
    lo = alert.get("low_value")

    subject = airport
    if condition == "HIGH_THRESHOLD_EXCEEDED":
        body = f"Soglia alta superata: valore={value}, high_value={hi}"
    elif condition == "LOW_THRESHOLD_BREACHED":
        body = f"Soglia bassa violata: valore={value}, low_value={lo}"
    else:
        body = f"Condizione: {condition}, valore={value}"
    return subject, body

def main():
    consumer.subscribe([TOPIC])
    print(f"Notifier started. Consuming {TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error:", msg.error())
                continue

            try:
                alert = json.loads(msg.value().decode("utf-8"))
                email = alert.get("email")
                if not email:
                    consumer.commit(asynchronous=False)
                    continue

                subject, body = build_email(alert)
                send_email(email, subject, body)

                consumer.commit(asynchronous=False)

            except Exception as e:
                print(f"Notifier error at offset {msg.offset()}: {e}")
                time.sleep(1)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
