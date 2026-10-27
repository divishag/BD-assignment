import json
from pathlib import Path
from kafka import KafkaProducer

INPUT_FILE = Path.home() / "food-ordering" / "order_metrics.jsonl"
TOPIC = "order_metrics"
BOOTSTRAP_SERVERS = ["172.25.244.150:9092"]

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=5
)

sent = 0

try:
    with INPUT_FILE.open("r", encoding="utf-8") as file:
        for line in file:
            if line.strip():
                record = json.loads(line)
                producer.send(TOPIC, value=record).get(timeout=30)
                sent += 1

    producer.flush()
    print(f"PRODUCER 2 COMPLETED: {sent} records sent to '{TOPIC}'")

finally:
    producer.close()
