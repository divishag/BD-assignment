import json
import os
import sys
from pathlib import Path
from kafka import KafkaConsumer, TopicPartition

BROKERS = os.environ.get(
    "KAFKA_BROKERS", "localhost:9092"
).split(",")

TOPIC = "order_metrics"
OUTPUT = Path("order_metrics_offsets.json")


def main():
    consumer = KafkaConsumer(
        bootstrap_servers=BROKERS,
        request_timeout_ms=10000
    )

    try:
        partitions = consumer.partitions_for_topic(TOPIC)

        if not partitions:
            raise RuntimeError(
                f"Topic '{TOPIC}' not found or unavailable"
            )

        topic_partitions = [
            TopicPartition(TOPIC, p)
            for p in sorted(partitions)
        ]

        offsets = consumer.end_offsets(topic_partitions)

        saved = {
            str(tp.partition): offsets[tp]
            for tp in topic_partitions
        }

        OUTPUT.write_text(
            json.dumps(saved, indent=2),
            encoding="utf-8"
        )

        print(f"Captured offsets for '{TOPIC}':")
        print(json.dumps(saved, indent=2))
        print(f"Saved to {OUTPUT}")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
