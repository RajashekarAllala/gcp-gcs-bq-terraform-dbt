output "bucket_name" {
  description = "GCS bucket name"
  value       = google_storage_bucket.data_bucket.name
}

output "staging_dataset" {
  description = "Staging dataset resource-id"
  value       = "${google_bigquery_dataset.staging.project}:${google_bigquery_dataset.staging.dataset_id}"
}

output "transformed_dataset" {
  description = "Transformed dataset resource-id"
  value       = "${google_bigquery_dataset.transformed.project}:${google_bigquery_dataset.transformed.dataset_id}"
}