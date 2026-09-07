# Databricks Runbook

## Workspace objects

- Schema: `workspace.ecommerce`
- Volume: `workspace.ecommerce.raw`
- Raw-file path: `/Volumes/workspace/ecommerce/raw/raw/`

## Verified milestones

### Bronze

`01_bronze_ingestion` creates `bronze_customers`, `bronze_products`, and
`bronze_events`, with 5, 5, and 6 rows respectively.

### Silver

`02_silver_transformations` standardizes text, converts dates, timestamps,
prices, and quantities to correct types, removes duplicate keys, and filters
invalid rows. It creates `silver_customers`, `silver_products`, and
`silver_events`, with 5, 5, and 6 rows respectively.

### Gold

`03_gold_analytics` creates the business-facing tables:

- `gold_sales_by_country`
- `gold_sales_by_category`
- `gold_top_products`

The verified top-product output is:

| Product | Units sold | Revenue |
| --- | ---: | ---: |
| USB-C Laptop Hub | 2 | 298.00 |
| Insulated Water Bottle | 3 | 255.00 |
| Data Engineering Handbook | 1 | 159.00 |

## Free Edition note

Unity Catalog does not support `input_file_name()`. Use `_metadata.file_path`
when source-file lineage is required.
