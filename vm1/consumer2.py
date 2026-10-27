import json
import os
import uuid
from pathlib import Path
from kafka import KafkaConsumer, TopicPartition

BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092").split(",")
TOPIC = "order_metrics"
OUTPUT = Path("order_metrics_landed.jsonl")
OFFSETS_FILE = Path("order_metrics_offsets.json")

def main():
    expected = int(os.environ["EXPECTED_COUNT"])
    if expected <= 0:
        raise ValueError("EXPECTED_COUNT must be positive")

    consumer = KafkaConsumer(
        #TOPIC,
        bootstrap_servers=BROKERS,
        group_id=f"team42-vm1-{uuid.uuid4().hex}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=15000,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    count = 0

    try:
        if os.environ.get("USE_SAVED_OFFSETS") == "1":
            if not OFFSETS_FILE.exists():
                raise FileNotFoundError(
                    f"Missing {OFFSETS_FILE}. Capture offsets first."
                )

            saved = json.loads(
                OFFSETS_FILE.read_text(encoding="utf-8")
            )

            partitions = consumer.partitions_for_topic(TOPIC)
            if not partitions:
                raise RuntimeError("Kafka topic unavailable")

            expected_keys = {str(p) for p in partitions}
            if set(saved) != expected_keys:
                raise RuntimeError(
                    "Saved partition list does not match topic"
                )

            topic_partitions = [
                TopicPartition(TOPIC, p)
                for p in sorted(partitions)
            ]

            consumer.assign(topic_partitions)

            beginning = consumer.beginning_offsets(topic_partitions)
            end = consumer.end_offsets(topic_partitions)

            for tp in topic_partitions:
                offset = int(saved[str(tp.partition)])

                if not beginning[tp] <= offset <= end[tp]:
                    raise RuntimeError(
                        f"Invalid saved offset {offset} "
                        f"for partition {tp.partition}"
                    )

                consumer.seek(tp, offset)

            print("Using saved Kafka offsets:", saved)

        else:
            consumer.subscribe([TOPIC])
            print("Using original earliest-offset mode")
        with OUTPUT.open("w", encoding="utf-8") as f:
            for message in consumer:
                f.write(json.dumps(message.value) + "\n")
                count += 1

                if count >= expected:
                    break
    finally:
        consumer.close()

    print(f"Received {count}/{expected} records")

    if count != expected:
        raise RuntimeError("Incomplete Kafka batch")


if __name__ == "__main__":
    main()
