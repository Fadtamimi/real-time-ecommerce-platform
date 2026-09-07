# Databricks notebook source
from pyspark.sql.functions import current_timestamp

RAW_PATH = "/Volumes/workspace/ecommerce/raw/raw"

bronze_customers = spark.read.option("header", True).csv(
    f"{RAW_PATH}/customers.csv"
).withColumn("_ingested_at", current_timestamp())
bronze_products = spark.read.option("header", True).csv(
    f"{RAW_PATH}/products.csv"
).withColumn("_ingested_at", current_timestamp())
bronze_events = spark.read.option("multiLine", True).json(
    f"{RAW_PATH}/events.json"
).withColumn("_ingested_at", current_timestamp())

bronze_customers.write.format("delta").mode("overwrite").saveAsTable("workspace.ecommerce.bronze_customers")
bronze_products.write.format("delta").mode("overwrite").saveAsTable("workspace.ecommerce.bronze_products")
bronze_events.write.format("delta").mode("overwrite").saveAsTable("workspace.ecommerce.bronze_events")
