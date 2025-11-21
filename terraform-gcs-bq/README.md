# Terraform: GCS bucket + BigQuery datasets
# -----------------------------------------

Creates:
- One GCS bucket
- Two BigQuery datasets: `staging` and `transformed`

Authentication:
- Uses a service account JSON key file. Provide the path in `terraform.tfvars`.

Usage:
1. `terraform init`
2. `terraform plan -var-file="terraform.tfvars"`
3. `terraform apply -var-file="terraform.tfvars"`

Note: Do NOT commit your service account key file or `terraform.tfvars` with secrets to version control.

Providided a sample terraform tfvars file --> sample-terraform-tfvars. Rename this to terraform.tfvars and replace the values.
