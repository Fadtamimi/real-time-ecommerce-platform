"""Create a Gold category summary with a local PySpark DataFrame."""

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, sum, count
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("category", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("total_amount", DoubleType(), False),
    ]
)


def create_spark_session():
    """Create the Spark driver that coordinates this local batch job."""
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("ecommerce-gold-summary")
        .getOrCreate()
    )


def create_category_summary(spark, silver_path):
    """Read Silver, filter purchases, and aggregate them with Spark."""
    silver = spark.read.schema(SILVER_SCHEMA).json(str(silver_path))
    purchases = silver.filter(col("event_type") == "purchase")
    return (
        purchases.groupBy("category")
        .agg(
            count("event_id").alias("purchase_count"),
            sum("quantity").alias("units_sold"),
            round(sum("total_amount"), 2).alias("revenue"),
        )
        .orderBy("category")
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--silver",
        type=Path,
        default=PROJECT_ROOT / "data" / "processed" / "silver_events.jsonl",
    )
    arguments = parser.parse_args()

    spark = create_spark_session()
    try:
        summary = create_category_summary(spark, arguments.silver)
        summary.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
