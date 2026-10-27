
import json
import os
from pathlib import Path
from kafka import KafkaProducer

BROKERS = os.getenv(
    "KAFKA_BROKERS",
    "localhost:9092"
).split(",")

TOPIC = "orders"
INPUT_FILE = Path("order.jsonl")


def main():
    producer = KafkaProducer(
        bootstrap_servers=BROKERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        bootstrap_timeout_ms=60000,
        request_timeout_ms=60000,
        max_block_ms=120000
    )

    sent = 0

    try:
        with INPUT_FILE.open("r", encoding="utf-8") as file:
            for line in file:
                if not line.strip():
                    continue

                record = json.loads(line)

                producer.send(
                    TOPIC,
                    value=record
                ).get(timeout=120)

                sent += 1

        producer.flush()
        print(f"Successfully published {sent} orders to '{TOPIC}'")

    finally:
        producer.close()


if __name__ == "__main__":
    main()

