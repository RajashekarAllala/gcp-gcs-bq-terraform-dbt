variable "project" {
  description = "GCP project_id"
  type        = string
}

variable "region" {
  description = "GCP region for resources (e.g., US, EU or a region like us-central1 for buckets)"
  type        = string
  default     = "US"
}

variable "credentials_file" {
  description = "Path to service account JSON key file used for provider authentication"
  type        = string
}

variable "service_account_email" {
  description = "Email of the service account to grant IAM on bucket/datasets (e.g., my-sa@project.iam.gserviceaccount.com)"
  type        = string
}

variable "bucket_name" {
  description = "Name of the GCS bucket to create (must be globally unique)"
  type        = string
}

variable "bucket_location" {
  description = "Location for the GCS bucket (region or multi-region). Example: US, EU, us-central1"
  type        = string
  default     = "US"
}

variable "staging_dataset_id" {
  description = "BigQuery dataset id for staging (no project prefix). Example: staging"
  type        = string
  default     = "staging"
}

variable "transformed_dataset_id" {
  description = "BigQuery dataset id for transformed (no project prefix). Example: transformed"
  type        = string
  default     = "transformed"
}

variable "dataset_location" {
  description = "BigQuery dataset location (e.g., US, EU)"
  type        = string
  default     = "US"
}
variable "zone" {
  description = "GCP zone for resources (e.g., us-central1-a)"
  type        = string
  default     = "us-central1-a"
}