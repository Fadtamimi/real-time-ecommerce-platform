"""AWS Glue job: build Bronze, Silver, and Gold layers in Amazon S3."""

import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_timestamp, round, sum, to_date, to_timestamp, trim, upper

args = getResolvedOptions(sys.argv, ["DATA_LAKE_BUCKET"])
bucket = args["DATA_LAKE_BUCKET"]
raw = f"s3://{bucket}/raw"
bronze = f"s3://{bucket}/bronze"
silver = f"s3://{bucket}/silver"
gold = f"s3://{bucket}/gold"
spark = SparkSession.builder.getOrCreate()

# Bronze preserves the source shape and records ingestion time.
bronze_customers = spark.read.option("header", True).csv(f"{raw}/customers.csv").withColumn("_ingested_at", current_timestamp())
bronze_products = spark.read.option("header", True).csv(f"{raw}/products.csv").withColumn("_ingested_at", current_timestamp())
bronze_events = spark.read.option("multiLine", True).json(f"{raw}/events.json").withColumn("_ingested_at", current_timestamp())
for name, frame in {"customers": bronze_customers, "products": bronze_products, "events": bronze_events}.items():
    frame.write.mode("overwrite").parquet(f"{bronze}/{name}")

# Silver cleans types, keys, and invalid records.
silver_customers = (
    bronze_customers.select(trim(col("customer_id")).alias("customer_id"), trim(col("name")).alias("customer_name"), trim(col("country")).alias("country"), to_date("signup_date").alias("signup_date"), col("_ingested_at"))
    .dropDuplicates(["customer_id"]).filter(col("customer_id").isNotNull())
)
silver_products = (
    bronze_products.select(trim(col("product_id")).alias("product_id"), trim(col("product_name")).alias("product_name"), trim(col("category")).alias("category"), col("price").cast("decimal(10,2)").alias("price"), col("_ingested_at"))
    .dropDuplicates(["product_id"]).filter(col("product_id").isNotNull() & col("price").isNotNull())
)
silver_events = (
    bronze_events.select(trim(col("event_id")).alias("event_id"), trim(col("customer_id")).alias("customer_id"), trim(col("product_id")).alias("product_id"), upper(trim(col("event_type"))).alias("event_type"), to_timestamp("timestamp").alias("event_timestamp"), col("quantity").cast("int").alias("quantity"), col("_ingested_at"))
    .dropDuplicates(["event_id"])
    .filter(col("event_id").isNotNull() & col("event_timestamp").isNotNull() & (col("quantity") > 0))
)
for name, frame in {"customers": silver_customers, "products": silver_products, "events": silver_events}.items():
    frame.write.mode("overwrite").parquet(f"{silver}/{name}")

# Gold produces analytics-friendly, Athena-queryable facts.
purchases = (silver_events.filter(col("event_type") == "PURCHASE").join(silver_customers, "customer_id", "left").join(silver_products, "product_id", "left").withColumn("revenue", col("quantity") * col("price")))
sales_by_country = purchases.groupBy("country").agg(count("event_id").alias("purchase_count"), sum("quantity").alias("units_sold"), round(sum("revenue"), 2).alias("revenue"))
sales_by_category = purchases.groupBy("category").agg(count("event_id").alias("purchase_count"), sum("quantity").alias("units_sold"), round(sum("revenue"), 2).alias("revenue"))
top_products = purchases.groupBy("product_id", "product_name", "category").agg(count("event_id").alias("purchase_count"), sum("quantity").alias("units_sold"), round(sum("revenue"), 2).alias("revenue"))
for name, frame in {"sales_by_country": sales_by_country, "sales_by_category": sales_by_category, "top_products": top_products}.items():
    frame.write.mode("overwrite").parquet(f"{gold}/{name}")

print("AWS data lake pipeline complete: Bronze, Silver, and Gold Parquet layers written.")
