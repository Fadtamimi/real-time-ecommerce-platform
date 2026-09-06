variable "project_id" {
  description = "GCP project ID where resources would be created."
  type        = string
}

variable "region" {
  description = "GCP region for regional resources."
  type        = string
  default     = "us-central1"
}

variable "data_bucket_name" {
  description = "Globally unique bucket name for lake data."
  type        = string
}

variable "bigquery_dataset_id" {
  description = "BigQuery dataset ID for Gold analytics."
  type        = string
  default     = "ecommerce_gold"
}