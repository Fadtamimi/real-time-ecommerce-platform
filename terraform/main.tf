resource "google_storage_bucket" "data_lake" {
  name                        = var.data_bucket_name
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }

}

resource "google_bigquery_dataset" "gold" {
  dataset_id                 = var.bigquery_dataset_id
  location                   = var.region
  delete_contents_on_destroy = false
}