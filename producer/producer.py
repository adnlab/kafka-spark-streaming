import json
import time
import random
import os
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = "transactions"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

SPECIAL = {3: "negative_amount", 5: "late", 7: "bad_timestamp", 9: "bad_source",
           11: "too_large", 13: "late", 15: "duplicate", 17: "late"}

def make_event():
    return {
        "user_id": f"U{random.randint(10000, 99999)}",
        "amount": random.randint(1000, 5_000_000),
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": random.choice(["mobile", "web", "pos"])
    }

def generate_transactions():
    last_event = None
    n = 0

    while True:
        n += 1
        kind = SPECIAL.get(n % 20, "valid")
        event = make_event()

        if kind == "negative_amount":
            event["amount"] = -5000
        elif kind == "too_large":
            event["amount"] = 50_000_000
        elif kind == "bad_timestamp":
            event["timestamp"] = "not-a-date"
        elif kind == "bad_source":
            event["source"] = "atm"
        elif kind == "duplicate" and last_event:
            event = dict(last_event)
        elif kind == "late":
            old = datetime.now(timezone.utc) - timedelta(minutes=random.randint(5, 10))
            event["timestamp"] = old.strftime("%Y-%m-%dT%H:%M:%SZ")

        if kind == "valid":
            last_event = event

        producer.send(TOPIC_NAME, event)
        print(f"[{kind}] {event}")
        time.sleep(random.uniform(1, 2))

def main():
    print(f"Menghubungkan ke Kafka broker {KAFKA_BROKER}...")
    print(f"Mulai mengirim transaksi ke topik '{TOPIC_NAME}'...")
    generate_transactions()

if __name__ == "__main__":
    main()
