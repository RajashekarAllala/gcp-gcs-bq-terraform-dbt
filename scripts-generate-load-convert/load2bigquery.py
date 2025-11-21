#!/usr/bin/env python3
"""
gcs_to_bigquery_load.py

Load a CSV from GCS into BigQuery.

Defaults:
  PROJECT = student-00332
  DATASET = CL_STAGING
  TABLE   = loans
  GCS_URI = gs://ikl-finance-bucket-002/source_data/loans.csv

Requirements:
  pip install google-cloud-bigquery google-cloud-storage

Authentication:
  - Set GOOGLE_APPLICATION_CREDENTIALS to a service account key that has:
      - BigQuery Data Editor / BigQuery Admin (for writes & dataset creation)
      - Storage Object Viewer (to read the CSV)
  OR
  - Use ADC: gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform
"""

import argparse
import sys
from google.cloud import bigquery
from google.api_core.exceptions import Conflict, NotFound

# Explicit schema to match generated CSV
SCHEMA = [
    bigquery.SchemaField("Loan_ID", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Cust_Name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Loan_Amount", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("Int_Rate", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Instalments", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("Start_Date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("End_Date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("Status", "STRING", mode="NULLABLE"),
]

def ensure_dataset(client: bigquery.Client, dataset_id: str, location: str = "US"):
    """Create dataset if not exists."""
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = location
    try:
        client.create_dataset(dataset)
        print(f"Created dataset {dataset_id}")
    except Conflict:
        print(f"Dataset {dataset_id} already exists")

def load_csv_from_gcs(client: bigquery.Client, gcs_uri: str, dest_table_id: str,
                      schema=None, write_disposition="WRITE_TRUNCATE", skip_leading_rows=1):
    """
    Load a CSV from GCS into BigQuery table.

    write_disposition: WRITE_TRUNCATE | WRITE_APPEND | WRITE_EMPTY
    """
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = skip_leading_rows
    job_config.autodetect = False
    if schema:
        job_config.schema = schema
    job_config.write_disposition = write_disposition
    # optional: set field delimiter, max bad records etc.
    # job_config.field_delimiter = ","
    # job_config.max_bad_records = 0

    print(f"Starting load job: {gcs_uri} -> {dest_table_id}")
    load_job = client.load_table_from_uri(gcs_uri, dest_table_id, job_config=job_config)
    load_job.result()  # wait for completion
    destination_table = client.get_table(dest_table_id)
    print(f"Loaded {destination_table.num_rows} rows into {dest_table_id}")

def main(argv=None):
    parser = argparse.ArgumentParser(description="Load CSV from GCS into BigQuery")
    parser.add_argument("--project", "-p", default="student-00332", help="GCP project id")
    parser.add_argument("--dataset", "-d", default="CL_STAGING", help="BigQuery dataset")
    parser.add_argument("--table", "-t", default="loans", help="BigQuery table name")
    parser.add_argument("--gcs_uri", "-g", default="gs://ikl-finance-bucket-002/source_data/loans.csv", help="GCS URI of CSV")
    parser.add_argument("--location", "-l", default="US", help="BigQuery dataset location (default US)")
    parser.add_argument("--write_disposition", "-w", default="WRITE_TRUNCATE",
                        choices=["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"],
                        help="Write disposition for the load job")
    args = parser.parse_args(argv)

    project = args.project
    dataset = args.dataset
    table = args.table
    gcs_uri = args.gcs_uri
    location = args.location
    dest_table_id = f"{project}.{dataset}.{table}"

    # Create BigQuery client (uses ADC or GOOGLE_APPLICATION_CREDENTIALS)
    client = bigquery.Client(project=project)

    # Ensure dataset exists
    dataset_id_full = f"{project}.{dataset}"
    try:
        ensure_dataset(client, dataset_id_full, location=location)
    except Exception as e:
        print(f"Failed to ensure dataset {dataset_id_full}: {e}", file=sys.stderr)
        sys.exit(2)

    # Perform load
    try:
        load_csv_from_gcs(client, gcs_uri, dest_table_id, schema=SCHEMA, write_disposition=args.write_disposition)
    except NotFound as e:
        print(f"Resource not found error: {e}", file=sys.stderr)
        print("Check that the GCS URI and project/dataset names are correct and that the credentials have access.", file=sys.stderr)
        sys.exit(3)
    except Exception as e:
        print(f"Load job failed: {e}", file=sys.stderr)
        sys.exit(4)

    print("Load completed successfully.")

if __name__ == "__main__":
    main()
