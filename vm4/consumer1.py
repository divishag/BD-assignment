import json
import os
import sys
import time
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

BOOTSTRAP_SERVERS = ["172.25.244.150:9092"]
TOPIC = "orders"
EXPECTED_RECORDS = 62

OUTPUT_FILE = Path.home() / "food-ordering" / "orders_landed.jsonl"

WAIT_TIMEOUT_SECONDS = 1800


# --------------------------------------------------
# CONSUMER 1
# --------------------------------------------------

def main():
    consumer = None

    try:
        print("Connecting to Kafka...", flush=True)
        print(f"Bootstrap servers: {BOOTSTRAP_SERVERS}", flush=True)
        print(f"Topic: {TOPIC}", flush=True)

        consumer = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            enable_auto_commit=False,
            group_id=None,
            auto_offset_reset="latest",
            request_timeout_ms=30000,
        )

        # Discover the topic's partitions.
        partition_ids = consumer.partitions_for_topic(TOPIC)

        if not partition_ids:
            raise RuntimeError(
                f"Topic '{TOPIC}' was not found or has no available partitions."
            )

        partitions = [
            TopicPartition(TOPIC, partition_id)
            for partition_id in sorted(partition_ids)
        ]

        # Assign all partitions directly.
        consumer.assign(partitions)

        # Capture the current end offsets before the new batch is published.
        end_offsets = consumer.end_offsets(partitions)

        # Skip all messages that existed before we started.
        for partition in partitions:
            consumer.seek(partition, end_offsets[partition])

        print("\n======================================", flush=True)
        print("CONSUMER 1 IS READY!", flush=True)
        print("======================================", flush=True)
        print(f"Topic: {TOPIC}", flush=True)
        print(f"Partitions: {sorted(partition_ids)}", flush=True)
        print(f"Starting offsets: {end_offsets}", flush=True)
        print(f"Expected records: {EXPECTED_RECORDS}", flush=True)
        print(f"Output file: {OUTPUT_FILE}", flush=True)

        print(
            "\nAsk Divisha to publish the 62 orders now.",
            flush=True,
        )
        print("Keep this program running.", flush=True)

        records = []
        deadline = time.monotonic() + WAIT_TIMEOUT_SECONDS

        # Collect the next 62 records.
        while len(records) < EXPECTED_RECORDS:

            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Timed out: received {len(records)} of "
                    f"{EXPECTED_RECORDS} records."
                )

            batches = consumer.poll(
                timeout_ms=1000,
                max_records=EXPECTED_RECORDS - len(records),
            )

            for messages in batches.values():
                for message in messages:

                    if len(records) >= EXPECTED_RECORDS:
                        break

                    if message.value is None:
                        raise ValueError(
                            f"Empty Kafka message at partition "
                            f"{message.partition}, offset {message.offset}."
                        )

                    try:
                        decoded = message.value.decode("utf-8")
                        order = json.loads(decoded)

                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        raise ValueError(
                            f"Invalid JSON at partition {message.partition}, "
                            f"offset {message.offset}: {exc}"
                        ) from exc

                    if not isinstance(order, dict):
                        raise ValueError(
                            f"Expected a JSON object at partition "
                            f"{message.partition}, offset {message.offset}."
                        )

                    records.append(order)

                    print(
                        f"Received {len(records)}/{EXPECTED_RECORDS} "
                        f"| partition={message.partition} "
                        f"| offset={message.offset}",
                        flush=True,
                    )

        # Write the output only after all 62 records have been collected.
        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

        temp_file = OUTPUT_FILE.with_suffix(".jsonl.tmp")

        with temp_file.open("w", encoding="utf-8") as output:
            for order in records:
                output.write(
                    json.dumps(order, ensure_ascii=False) + "\n"
                )

        # Replace the final output file atomically.
        os.replace(temp_file, OUTPUT_FILE)

        print("\n======================================", flush=True)
        print("SUCCESS: CONSUMER 1 COMPLETED", flush=True)
        print("======================================", flush=True)
        print(f"Records saved: {len(records)}", flush=True)
        print(f"File: {OUTPUT_FILE}", flush=True)

    except (
        KafkaError,
        RuntimeError,
        TimeoutError,
        ValueError,
        OSError,
    ) as exc:
        print(f"\nERROR: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)

    finally:
        if consumer is not None:
            consumer.close()


if __name__ == "__main__":
    main()

