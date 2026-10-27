import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    unix_timestamp,
    when,
    greatest,
    lit,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

BASE_DIR = Path.home() / "food-ordering"
INPUT_FILE = BASE_DIR / "orders_landed.jsonl"
OUTPUT_FILE = BASE_DIR / "order_metrics.jsonl"


# --------------------------------------------------
# INPUT SCHEMA
# --------------------------------------------------

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("restaurant_id", StringType(), True),
    StructField("delivery_partner_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("accepted_timestamp", StringType(), True),
    StructField("picked_up_timestamp", StringType(), True),
    StructField("expected_delivery_timestamp", StringType(), True),
    StructField("delivered_timestamp", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("delivery_fee", DoubleType(), True),
    StructField("status", StringType(), True),
])


# --------------------------------------------------
# SPARK SESSION
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("FoodOrderingSparkJob1")
    .master("local[*]")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# LOAD INPUT
# --------------------------------------------------

if not INPUT_FILE.exists():
    print(f"ERROR: Input file not found: {INPUT_FILE}", file=sys.stderr)
    spark.stop()
    sys.exit(1)

print(f"Reading: {INPUT_FILE}")

raw_df = spark.read.text(str(INPUT_FILE))

parsed_df = (
    raw_df
    .select(from_json(col("value"), schema).alias("order"))
    .select("order.*")
)


# --------------------------------------------------
# PARSE TIMESTAMPS
# --------------------------------------------------

timestamp_columns = [
    "order_timestamp",
    "accepted_timestamp",
    "picked_up_timestamp",
    "expected_delivery_timestamp",
    "delivered_timestamp",
]

df = parsed_df

for name in timestamp_columns:
    df = df.withColumn(
        name,
        to_timestamp(col(name))
    )


# --------------------------------------------------
# CLEAN DATA
# --------------------------------------------------

required_columns = [
    "order_id",
    "customer_id",
    "restaurant_id",
    "delivery_partner_id",
    "city",
    "order_timestamp",
    "accepted_timestamp",
    "picked_up_timestamp",
    "expected_delivery_timestamp",
    "delivered_timestamp",
    "distance_km",
    "order_amount",
    "delivery_fee",
    "status",
]

df = df.dropna(subset=required_columns)

df = df.filter(
    (col("distance_km") > 0)
    & (col("order_amount") > 0)
    & (col("delivery_fee") >= 0)
    & (col("status") == "DELIVERED")
)

# Remove exact duplicate records.
df = df.dropDuplicates()


# --------------------------------------------------
# CALCULATE METRICS
# --------------------------------------------------

df = (
    df
    .withColumn(
        "preparation_time_min",
        (
            unix_timestamp("picked_up_timestamp")
            - unix_timestamp("accepted_timestamp")
        ) / 60.0
    )
    .withColumn(
        "delivery_time_min",
        (
            unix_timestamp("delivered_timestamp")
            - unix_timestamp("picked_up_timestamp")
        ) / 60.0
    )
    .withColumn(
        "fulfillment_time_min",
        (
            unix_timestamp("delivered_timestamp")
            - unix_timestamp("order_timestamp")
        ) / 60.0
    )
    .withColumn(
        "delivery_speed_kmph",
        col("distance_km") / (col("delivery_time_min") / 60.0)
    )
    .withColumn(
        "delay_min",
        greatest(
            lit(0.0),
            (
                unix_timestamp("delivered_timestamp")
                - unix_timestamp("expected_delivery_timestamp")
            ) / 60.0
        )
    )
)

# Reject invalid time intervals and avoid division by zero.
df = df.filter(
    (col("preparation_time_min") >= 0)
    & (col("delivery_time_min") > 0)
    & (col("fulfillment_time_min") >= 0)
)

df = df.withColumn(
    "delivery_status",
    when(col("delay_min") == 0, "ON_TIME").otherwise("DELAYED")
)

df = df.withColumn(
    "delay_category",
    when(col("delay_min") == 0, "ON_TIME")
    .when(col("delay_min") <= 15, "SLIGHT_DELAY")
    .when(col("delay_min") <= 30, "MODERATE_DELAY")
    .otherwise("SEVERE_DELAY")
)


# --------------------------------------------------
# SELECT FINAL OUTPUT SCHEMA
# --------------------------------------------------

metrics_df = df.select(
    "order_id",
    "customer_id",
    "restaurant_id",
    "delivery_partner_id",
    "city",
    "order_amount",
    "delivery_fee",
    "distance_km",
    "preparation_time_min",
    "delivery_time_min",
    "fulfillment_time_min",
    "delivery_speed_kmph",
    "delay_min",
    "delivery_status",
    "delay_category",
)


# --------------------------------------------------
# WRITE JSONL OUTPUT
# --------------------------------------------------

records = [
    json.loads(row)
    for row in metrics_df.toJSON().collect()
]

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

with OUTPUT_FILE.open("w", encoding="utf-8") as output:
    for record in records:
        output.write(json.dumps(record, ensure_ascii=False) + "\n")

print("\n======================================")
print("SPARK JOB 1 COMPLETED")
print("======================================")
print(f"Input records: {raw_df.count()}")
print(f"Valid output records: {len(records)}")
print(f"Output file: {OUTPUT_FILE}")

spark.stop()
