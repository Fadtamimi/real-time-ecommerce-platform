"""Write Silver events to a local Delta Lake table and read them back."""

import argparse
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def create_spark_session():
    """Create Spark with Delta Lake extensions enabled."""
    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName("ecommerce-delta-demo")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def write_and_read_delta(spark, silver_path, delta_path):
    """Convert Silver JSONL into a versioned Delta table and read its current version."""
    silver = spark.read.json(str(silver_path))
    silver.write.format("delta").mode("overwrite").save(str(delta_path))
    current_table = spark.read.format("delta").load(str(delta_path))
    current_table.groupBy("category").count().orderBy("category").show()
    print(f"Delta rows: {current_table.count()}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--silver",
        type=Path,
        default=PROJECT_ROOT / "data" / "processed" / "silver_events.jsonl",
    )
    parser.add_argument(
        "--delta",
        type=Path,
        default=PROJECT_ROOT / "data" / "processed" / "delta" / "silver_events",
    )
    arguments = parser.parse_args()

    spark = create_spark_session()
    try:
        write_and_read_delta(spark, arguments.silver, arguments.delta)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
