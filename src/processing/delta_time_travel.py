"""Demonstrate Delta Lake append, table versions, and time travel."""

from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def create_spark_session():
    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName("ecommerce-delta-time-travel")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def run_demo(spark, silver_path, table_path):
    """Create two table versions and read the original version back."""
    silver = spark.read.json(str(silver_path))
    silver.write.format("delta").mode("overwrite").save(str(table_path))
    initial_count = spark.read.format("delta").load(str(table_path)).count()

    silver.limit(2).write.format("delta").mode("append").save(str(table_path))
    current_count = spark.read.format("delta").load(str(table_path)).count()
    version_zero_count = (
        spark.read.format("delta").option("versionAsOf", 0).load(str(table_path)).count()
    )

    print(f"Version 0 rows: {initial_count}")
    print(f"Current rows after append: {current_count}")
    print(f"Time-travel version 0 rows: {version_zero_count}")
    print("Table history:")
    spark.sql(f"DESCRIBE HISTORY delta.`{table_path}`").select(
        "version", "operation"
    ).show(truncate=False)


def main():
    spark = create_spark_session()
    try:
        run_demo(
            spark,
            PROJECT_ROOT / "data" / "processed" / "silver_events.jsonl",
            PROJECT_ROOT / "data" / "processed" / "delta" / "time_travel_demo",
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
