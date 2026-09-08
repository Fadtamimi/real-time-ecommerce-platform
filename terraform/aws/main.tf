locals {
  name_prefix = "${var.project_name}-${random_id.suffix.hex}"
  common_tags = { Project = var.project_name, Environment = "learning", ManagedBy = "terraform" }
  raw_sources = {
    "raw/customers.csv" = "${path.module}/../../data/raw/customers.csv"
    "raw/products.csv"  = "${path.module}/../../data/raw/products.csv"
    "raw/events.json"   = "${path.module}/../../data/raw/events.json"
  }
}

resource "random_id" "suffix" { byte_length = 3 }

# Durable S3 lake: every layer gets a private, encrypted prefix.
resource "aws_s3_bucket" "lake" {
  bucket = "${local.name_prefix}-lake"
  tags   = local.common_tags
}
resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration { status = "Enabled" }
}
resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "expire-noncurrent-learning-data"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration { noncurrent_days = 30 }
  }
}

resource "aws_s3_bucket" "athena_results" {
  bucket = "${local.name_prefix}-athena-results"
  tags   = local.common_tags
}
resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
resource "aws_s3_object" "lake_prefixes" {
  for_each = toset(["raw/", "bronze/", "silver/", "gold/", "scripts/"])
  bucket   = aws_s3_bucket.lake.id
  key      = each.value
}

# Terraform uploads the sample input and exact Glue script version it deploys.
resource "aws_s3_object" "raw_source" {
  for_each = local.raw_sources
  bucket   = aws_s3_bucket.lake.id
  key      = each.key
  source   = each.value
  etag     = filemd5(each.value)
}
resource "aws_s3_object" "pipeline_script" {
  bucket = aws_s3_bucket.lake.id
  key    = "scripts/ecommerce_pipeline.py"
  source = "${path.module}/scripts/ecommerce_pipeline.py"
  etag   = filemd5("${path.module}/scripts/ecommerce_pipeline.py")
}

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "glue" {
  name               = "${local.name_prefix}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
  tags               = local.common_tags
}
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}
data "aws_iam_policy_document" "glue_lake" {
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
  }
}
resource "aws_iam_role_policy" "glue_lake" {
  name   = "${local.name_prefix}-lake-access"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_lake.json
}

resource "aws_glue_catalog_database" "ecommerce" { name = replace("${var.project_name}_analytics", "-", "_") }

# Glue Catalog external tables make the Parquet Gold layer queryable in Athena.
resource "aws_glue_catalog_table" "gold_sales_by_country" {
  name          = "gold_sales_by_country"
  database_name = aws_glue_catalog_database.ecommerce.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "parquet" }
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/gold/sales_by_country/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info { serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe" }
    columns {
      name = "country"
      type = "string"
    }
    columns {
      name = "purchase_count"
      type = "bigint"
    }
    columns {
      name = "units_sold"
      type = "bigint"
    }
    columns {
      name = "revenue"
      type = "double"
    }
  }
}
resource "aws_glue_catalog_table" "gold_sales_by_category" {
  name          = "gold_sales_by_category"
  database_name = aws_glue_catalog_database.ecommerce.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "parquet" }
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/gold/sales_by_category/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info { serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe" }
    columns {
      name = "category"
      type = "string"
    }
    columns {
      name = "purchase_count"
      type = "bigint"
    }
    columns {
      name = "units_sold"
      type = "bigint"
    }
    columns {
      name = "revenue"
      type = "double"
    }
  }
}
resource "aws_glue_catalog_table" "gold_top_products" {
  name          = "gold_top_products"
  database_name = aws_glue_catalog_database.ecommerce.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "parquet" }
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/gold/top_products/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info { serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe" }
    columns {
      name = "product_id"
      type = "string"
    }
    columns {
      name = "product_name"
      type = "string"
    }
    columns {
      name = "category"
      type = "string"
    }
    columns {
      name = "purchase_count"
      type = "bigint"
    }
    columns {
      name = "units_sold"
      type = "bigint"
    }
    columns {
      name = "revenue"
      type = "double"
    }
  }
}

resource "aws_athena_workgroup" "analytics" {
  name = "${local.name_prefix}-analytics"
  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    result_configuration { output_location = "s3://${aws_s3_bucket.athena_results.bucket}/" }
  }
  tags = local.common_tags
}
resource "aws_cloudwatch_log_group" "glue" {
  name              = "/aws-glue/jobs/${local.name_prefix}"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_glue_job" "pipeline" {
  name              = "${local.name_prefix}-medallion-pipeline"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 10
  max_retries       = 0
  execution_property { max_concurrent_runs = 1 }
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lake.bucket}/scripts/ecommerce_pipeline.py"
    python_version  = "3"
  }
  default_arguments = {
    "--job-language"                 = "python"
    "--DATA_LAKE_BUCKET"             = aws_s3_bucket.lake.bucket
    "--enable-job-bookmark"          = "true"
    "--enable-continuous-log-filter" = "true"
    "--continuous-log-logGroup"      = aws_cloudwatch_log_group.glue.name
  }
  tags       = local.common_tags
  depends_on = [aws_s3_object.pipeline_script]
}
resource "aws_cloudwatch_metric_alarm" "glue_failures" {
  alarm_name          = "${local.name_prefix}-glue-failures"
  alarm_description   = "Glue pipeline reported failed tasks. Inspect CloudWatch logs."
  namespace           = "Glue"
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  dimensions          = { JobName = aws_glue_job.pipeline.name }
  tags                = local.common_tags
}

data "aws_iam_policy_document" "eventbridge_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "eventbridge_glue" {
  name               = "${local.name_prefix}-eventbridge-glue"
  assume_role_policy = data.aws_iam_policy_document.eventbridge_assume_role.json
}
resource "aws_iam_role_policy" "eventbridge_glue" {
  name   = "start-glue-pipeline"
  role   = aws_iam_role.eventbridge_glue.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow", Action = ["glue:StartJobRun"], Resource = aws_glue_job.pipeline.arn }] })
}

# Created disabled: turn it on only after a verified manual run and cost check.
resource "aws_cloudwatch_event_rule" "daily_pipeline" {
  name                = "${local.name_prefix}-daily-pipeline"
  description         = "Runs the ecommerce Glue pipeline daily at 03:00 UTC."
  schedule_expression = "cron(0 3 * * ? *)"
  is_enabled          = var.enable_schedule
  tags                = local.common_tags
}
resource "aws_cloudwatch_event_target" "daily_pipeline" {
  rule      = aws_cloudwatch_event_rule.daily_pipeline.name
  target_id = "glue-medallion-pipeline"
  arn       = aws_glue_job.pipeline.arn
  role_arn  = aws_iam_role.eventbridge_glue.arn
}

# This budget appears in Billing. An alert e-mail can be added later if desired.
resource "aws_budgets_budget" "monthly_guardrail" {
  name         = "${local.name_prefix}-monthly-guardrail"
  budget_type  = "COST"
  limit_amount = var.monthly_budget_limit_usd
  limit_unit   = "USD"
  time_unit    = "MONTHLY"
}

# Optional streaming tier: intentionally off because MSK has standing cost.
resource "aws_vpc" "streaming" {
  count                = var.enable_msk ? 1 : 0
  cidr_block           = "10.42.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags                 = merge(local.common_tags, { Name = "${local.name_prefix}-streaming" })
}
data "aws_availability_zones" "available" { state = "available" }
resource "aws_subnet" "msk" {
  count             = var.enable_msk ? 2 : 0
  vpc_id            = aws_vpc.streaming[0].id
  cidr_block        = cidrsubnet("10.42.0.0/16", 4, count.index)
  availability_zone = data.aws_availability_zones.available.names[count.index]
  tags              = merge(local.common_tags, { Name = "${local.name_prefix}-msk-${count.index}" })
}
resource "aws_security_group" "msk" {
  count  = var.enable_msk ? 1 : 0
  name   = "${local.name_prefix}-msk"
  vpc_id = aws_vpc.streaming[0].id
  ingress {
    from_port   = 9098
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = ["10.42.0.0/16"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
resource "aws_msk_serverless_cluster" "events" {
  count        = var.enable_msk ? 1 : 0
  cluster_name = "${local.name_prefix}-events"
  vpc_config {
    subnet_ids         = aws_subnet.msk[*].id
    security_group_ids = [aws_security_group.msk[0].id]
  }
  client_authentication {
    sasl {
      iam { enabled = true }
    }
  }
  tags = local.common_tags
}
