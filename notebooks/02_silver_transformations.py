# Databricks notebook source
from pyspark.sql.functions import col, to_date, to_timestamp, trim, upper

customers = spark.table("workspace.ecommerce.bronze_customers")
products = spark.table("workspace.ecommerce.bronze_products")
events = spark.table("workspace.ecommerce.bronze_events")

silver_customers = customers.select(
    trim(col("customer_id")).alias("customer_id"),
    trim(col("name")).alias("customer_name"),
    trim(col("country")).alias("country"),
    to_date("signup_date").alias("signup_date"),
    col("_ingested_at"),
).dropDuplicates(["customer_id"]).filter(col("customer_id").isNotNull())

silver_products = products.select(
    trim(col("product_id")).alias("product_id"),
    trim(col("product_name")).alias("product_name"),
    trim(col("category")).alias("category"),
    col("price").cast("decimal(10,2)").alias("price"),
    col("_ingested_at"),
).dropDuplicates(["product_id"]).filter(
    col("product_id").isNotNull() & col("price").isNotNull()
)

silver_events = events.select(
    trim(col("event_id")).alias("event_id"),
    trim(col("customer_id")).alias("customer_id"),
    trim(col("product_id")).alias("product_id"),
    upper(trim(col("event_type"))).alias("event_type"),
    to_timestamp("timestamp").alias("event_timestamp"),
    col("quantity").cast("int").alias("quantity"),
    col("_ingested_at"),
).dropDuplicates(["event_id"]).filter(
    col("event_id").isNotNull()
    & col("event_timestamp").isNotNull()
    & (col("quantity") > 0)
)

silver_customers.write.format("delta").mode("overwrite").saveAsTable("workspace.ecommerce.silver_customers")
silver_products.write.format("delta").mode("overwrite").saveAsTable("workspace.ecommerce.silver_products")
silver_events.write.format("delta").mode("overwrite").saveAsTable("workspace.ecommerce.silver_events")
