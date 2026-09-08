# Screenshot checklist

These screenshots make the repository easy to trust in a recruiter or
interview review. Never include access keys, credentials, personal email, or
unrelated browser tabs.

Save each file in this folder using the exact name below.

| # | Filename | Capture | What it proves |
| ---: | --- | --- | --- |
| 01 | `01-project-structure.png` | VS Code Explorer with `data`, `notebooks`, `terraform`, `tests`, README | Real project organisation |
| 02 | `02-raw-data.png` | `data/raw` files open or listed | Source data exists |
| 03 | `03-databricks-bronze.png` | Databricks Bronze query result | Ingestion worked |
| 04 | `04-databricks-silver.png` | Databricks Silver query result | Cleaning and typing worked |
| 05 | `05-databricks-gold-results.png` | Databricks `gold_top_products` | Analytics output works |
| 06 | `06-terraform-apply.png` | CloudShell `Apply complete!` | Infrastructure was deployed from code |
| 07 | `07-glue-job-success.png` | Glue run state `SUCCEEDED` | Managed ETL ran |
| 08 | `08-athena-gold-query.png` | Athena `gold_top_products` result | Gold data is queryable |
| 09 | `09-s3-medallion-layers.png` | S3 prefixes raw/bronze/silver/gold | Medallion lake layout exists |

## Databricks queries

```sql
SELECT 'bronze_customers' AS table_name, COUNT(*) AS rows FROM workspace.ecommerce.bronze_customers
UNION ALL
SELECT 'bronze_products', COUNT(*) FROM workspace.ecommerce.bronze_products
UNION ALL
SELECT 'bronze_events', COUNT(*) FROM workspace.ecommerce.bronze_events;
```

```sql
SELECT product_id, product_name, category, purchase_count, units_sold, revenue
FROM workspace.ecommerce.gold_top_products
ORDER BY revenue DESC;
```

## Athena query

```sql
SELECT product_id, product_name, category, purchase_count, units_sold, revenue
FROM gold_top_products
ORDER BY revenue DESC;
```

After you capture the images, commit the PNG files and remove the comment
markers around the image links in the root `README.md`.
