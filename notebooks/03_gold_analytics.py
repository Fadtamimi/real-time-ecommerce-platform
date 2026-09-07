# Databricks notebook source
from pyspark.sql.functions import col, count, round, sum

customers = spark.table("workspace.ecommerce.silver_customers")
products = spark.table("workspace.ecommerce.silver_products")
events = spark.table("workspace.ecommerce.silver_events")

purchases = (
    events.filter(col("event_type") == "PURCHASE")
    .join(customers.select("customer_id", "country"), "customer_id")
    .join(products.select("product_id", "product_name", "category", "price"), "product_id")
    .withColumn("sales_amount", col("quantity") * col("price"))
)

gold_sales_by_country = purchases.groupBy("country").agg(
    count("event_id").alias("purchase_count"),
    sum("quantity").alias("units_sold"),
    round(sum("sales_amount"), 2).alias("revenue"),
).orderBy(col("revenue").desc())

gold_sales_by_category = purchases.groupBy("category").agg(
    count("event_id").alias("purchase_count"),
    sum("quantity").alias("units_sold"),
    round(sum("sales_amount"), 2).alias("revenue"),
).orderBy(col("revenue").desc())

gold_top_products = purchases.groupBy("product_id", "product_name", "category").agg(
    count("event_id").alias("purchase_count"),
    sum("quantity").alias("units_sold"),
    round(sum("sales_amount"), 2).alias("revenue"),
).orderBy(col("revenue").desc())

gold_sales_by_country.write.format("delta").mode("overwrite").saveAsTable("workspace.ecommerce.gold_sales_by_country")
gold_sales_by_category.write.format("delta").mode("overwrite").saveAsTable("workspace.ecommerce.gold_sales_by_category")
gold_top_products.write.format("delta").mode("overwrite").saveAsTable("workspace.ecommerce.gold_top_products")
