# -------------------------
# GCS bucket
# -------------------------
resource "google_storage_bucket" "data_bucket" {
  name                        = var.bucket_name
  location                    = var.bucket_location
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 365
    }
  }

  labels = {
    created_by  = "terraform"
    environment = "dev"
  }
}

# create "folder" object for source_data/ (zero-byte object with trailing slash)
resource "google_storage_bucket_object" "source_folder" {
  name   = "source_data/"                # trailing slash means folder
  bucket = google_storage_bucket.data_bucket.name
  content = "placeholder"                           # zero-byte object
  # optionally set content_type = "application/x-directory"
}

# create "folder" object for transformed_xml_files/
resource "google_storage_bucket_object" "transformed_xml_folder" {
  name   = "transformed_xml_files/"
  bucket = google_storage_bucket.data_bucket.name
  content = "placeholder"
}

# -------------------------
# BigQuery dataset: staging
# -------------------------
resource "google_bigquery_dataset" "staging" {
  dataset_id                  = var.staging_dataset_id
  project                     = var.project
  friendly_name               = "Staging dataset"
  description                 = "Staging dataset for raw data"
  location                    = var.dataset_location
  default_table_expiration_ms = null

  labels = {
    layer = "bronze"
  }
}

# -----------------------------
# BigQuery dataset: transformed
# -----------------------------
resource "google_bigquery_dataset" "transformed" {
  dataset_id                  = var.transformed_dataset_id
  project                     = var.project
  friendly_name               = "Transformed dataset"
  description                 = "Transformed / curated dataset"
  location                    = var.dataset_location
  default_table_expiration_ms = null

  labels = {
    layer = "silver_gold"
  }
}