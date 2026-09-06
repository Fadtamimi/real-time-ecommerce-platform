output "data_bucket_name" {
  description = "Cloud Storage bucket for Bronze, Silver, and Gold files."
  value       = google_storage_bucket.data_lake.name
}

output "bigquery_dataset_id" {
  description = "BigQuery dataset for Gold analytics."
  value       = google_bigquery_dataset.gold.dataset_id
}