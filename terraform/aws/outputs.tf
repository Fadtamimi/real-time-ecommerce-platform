output "data_lake_bucket" { value = aws_s3_bucket.lake.bucket }
output "athena_results_bucket" { value = aws_s3_bucket.athena_results.bucket }
output "athena_workgroup" { value = aws_athena_workgroup.analytics.name }
output "glue_database" { value = aws_glue_catalog_database.ecommerce.name }
output "pipeline_glue_job" { value = aws_glue_job.pipeline.name }
output "cloudwatch_log_group" { value = aws_cloudwatch_log_group.glue.name }
output "msk_cluster_arn" { value = var.enable_msk ? aws_msk_serverless_cluster.events[0].arn : null }
