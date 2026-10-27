from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT = Path("order_metrics_landed.jsonl").resolve().as_uri()


def save_csv(df, filename):
    temp_dir = filename + "_temp"

    df.coalesce(1).write.mode("overwrite").option(
        "header", "true"
    ).csv(Path(temp_dir).resolve().as_uri())

    part_file = next(Path(temp_dir).glob("part-*.csv"))
    part_file.replace(filename)

    import shutil
    shutil.rmtree(temp_dir)


def main():
    spark = SparkSession.builder \
        .appName("Team42_VM1_Analytics") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    orders = spark.read.json(INPUT).dropDuplicates()

    delayed = orders.withColumn(
        "is_delayed",
        F.when(F.col("delivery_status") == "DELAYED", 1).otherwise(0)
    )

    restaurant = delayed.groupBy("restaurant_id").agg(
        F.count("*").alias("order_count"),
        F.round(F.avg("order_amount"), 2).alias("avg_order_amount"),
        F.round(F.avg("preparation_time_min"), 2).alias("avg_preparation_time_min"),
        F.round(F.avg("delivery_time_min"), 2).alias("avg_delivery_time_min"),
        F.sum("is_delayed").alias("delayed_order_count")
    ).withColumn(
        "delay_rate_pct",
        F.round(
            F.col("delayed_order_count") * 100 / F.col("order_count"), 2
        )
    ).orderBy("restaurant_id")

    city = delayed.groupBy("city").agg(
        F.count("*").alias("order_count"),
        F.round(F.avg("order_amount"), 2).alias("avg_order_amount"),
        F.round(F.avg("delivery_time_min"), 2).alias("avg_delivery_time_min"),
        F.sum("is_delayed").alias("delayed_order_count")
    ).withColumn(
        "delay_rate_pct",
        F.round(
            F.col("delayed_order_count") * 100 / F.col("order_count"), 2
        )
    ).orderBy("city")

    save_csv(restaurant, "restaurant_summary.csv")
    save_csv(city, "city_summary.csv")

    print("Created restaurant_summary.csv")
    print("Created city_summary.csv")

    spark.stop()


if __name__ == "__main__":
    main()
