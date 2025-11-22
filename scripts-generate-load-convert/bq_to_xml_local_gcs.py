#!/usr/bin/env python3
"""
bq_to_xml.py

Read a BigQuery table and convert rows to an XML file.
Optionally upload the XML file to a GCS bucket under `transformed_xml_files/`.

Features:
- Uses Application Default Credentials (ADC) via google.auth.default()
  (works with `gcloud auth application-default login` or
  GOOGLE_APPLICATION_CREDENTIALS pointing to a service account key).
- Supports requester-pays buckets with --billing-project
- Prints detailed diagnostics and full tracebacks on error

Requirements:
    pip install google-cloud-bigquery google-cloud-storage google-auth

Examples:
    # Export only
    python bq_to_xml_local_gcs.py --project student-00571 --dataset CL_TRANSFORMED --table defaulters --out defaulters.xml

    # Export and upload (normal)
    python bq_to_xml.py --project student-00571 --dataset CL_TRANSFORMED --table defaulters --upload --bucket ikl-finance-bucket-002

    # If bucket is requester-pays, provide your billing project
    python bq_to_xml.py --project student-00571 --dataset CL_TRANSFORMED --table defaulters --upload \
        --bucket ikl-finance-bucket-002 --billing-project my-billing-project
"""
from __future__ import annotations
import argparse
import sys
import traceback
from datetime import datetime, timezone
from typing import Optional, Any

import google.auth
from google.cloud import bigquery, storage
from xml.sax.saxutils import escape as xml_escape


def now_iso_z() -> str:
    """Return an ISO-8601 UTC timestamp with trailing 'Z' (no offset)."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def safe_format_value(value: Any) -> str:
    """Convert a BigQuery/Python value to a safe string for XML content."""
    if value is None:
        return ""
    # Handle datetime objects
    if isinstance(value, datetime):
        dt = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    # bytes-like
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8")
        except Exception:
            return str(value)
    # collections (list/dict/tuple/set) -> string
    if isinstance(value, (list, dict, tuple, set)):
        return str(value)
    # fallback
    return str(value)


class Clients:
    """Container for BigQuery and Storage clients created from ADC (or billing override)."""

    def __init__(self, project: Optional[str] = None, creds=None):
        # If creds/project are None, clients will use ADC defaults
        self.creds = creds
        self.project = project
        # create clients lazily
        self._bq_client: Optional[bigquery.Client] = None
        self._storage_client: Optional[storage.Client] = None

    def bq_client(self) -> bigquery.Client:
        if self._bq_client is None:
            if self.creds:
                self._bq_client = bigquery.Client(project=self.project, credentials=self.creds)
            else:
                self._bq_client = bigquery.Client(project=self.project)
        return self._bq_client

    def storage_client(self, project_override: Optional[str] = None) -> storage.Client:
        # For requester-pays, user may pass a billing project; we create a client with that project.
        effective_project = project_override if project_override is not None else self.project
        if project_override is not None:
            # create a new client for the billing project each time (safe)
            if self.creds:
                return storage.Client(project=effective_project, credentials=self.creds)
            return storage.Client(project=effective_project)
        if self._storage_client is None:
            if self.creds:
                self._storage_client = storage.Client(project=self.project, credentials=self.creds)
            else:
                self._storage_client = storage.Client(project=self.project)
        return self._storage_client


def stream_table_to_xml_file(clients: Clients, project: str, dataset: str, table: str, out_path: str) -> str:
    """Stream BigQuery rows and write to an XML file; returns out_path on success."""
    bq = clients.bq_client()
    table_ref = f"{project}.{dataset}.{table}"
    print(f"[{now_iso_z()}] Querying table: {table_ref}")

    table_obj = bq.get_table(table_ref)  # may raise exception if not found
    rows = bq.list_rows(table_obj)  # RowIterator

    cols = [field.name for field in rows.schema]
    print(f"[{now_iso_z()}] Columns: {cols}")

    written = 0
    with open(out_path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write("<Defaulters>\n")

        for row_idx, row in enumerate(rows):
            f.write("  <Defaulter>\n")
            for col in cols:
                # Support dict-like Row.get and item access; fallback to attribute
                value = None
                try:
                    if hasattr(row, "get"):
                        value = row.get(col)
                    else:
                        value = row[col]
                except Exception:
                    try:
                        value = row[col]
                    except Exception:
                        value = getattr(row, col, None)

                if value is None:
                    f.write(f"    <{col}></{col}>\n")
                else:
                    sval = safe_format_value(value)
                    f.write(f"    <{col}>{xml_escape(sval)}</{col}>\n")

            f.write("  </Defaulter>\n")
            written += 1
            if written % 1000 == 0:
                print(f"[{now_iso_z()}] Processed {written} rows...")

        f.write("</Defaulters>\n")

    print(f"[{now_iso_z()}] XML file written: {out_path} (rows: {written})")
    return out_path


def upload_file_to_gcs(clients: Clients, local_path: str, bucket_name: str, dest_path: Optional[str] = None,
                       billing_project: Optional[str] = None) -> str:
    """
    Upload local_path to gs://bucket_name/dest_path.
    If billing_project is set (requester-pays), create storage client with that project.
    Returns the GCS URI on success.
    """
    try:
        # Choose storage client: billing_project override creates a temporary client with that project
        client = clients.storage_client(project_override=billing_project) if billing_project else clients.storage_client()
        # Check bucket existence / accessibility
        bucket = client.lookup_bucket(bucket_name)
        if bucket is None:
            # More diagnostic: might still be that the account can't see the bucket
            raise RuntimeError(f"Bucket '{bucket_name}' not found or not accessible with current credentials.")

        if dest_path is None:
            dest_path = local_path.replace("\\", "/").split("/")[-1]

        blob = bucket.blob(dest_path)
        print(f"[{now_iso_z()}] Uploading {local_path} -> gs://{bucket_name}/{dest_path}")
        blob.upload_from_filename(local_path)
        gcs_uri = f"gs://{bucket_name}/{dest_path}"
        print(f"[{now_iso_z()}] Uploaded to GCS: {gcs_uri}")
        return gcs_uri

    except Exception as e:
        print(f"[{now_iso_z()}] ERROR uploading to GCS: {e}", file=sys.stderr)
        traceback.print_exc()
        raise


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Export BigQuery table to XML and optionally upload to GCS")
    p.add_argument("--project", "-p", default=None, help="GCP project id (default: uses ADC or explicit)")
    p.add_argument("--dataset", "-d", required=True, help="BigQuery dataset")
    p.add_argument("--table", "-t", required=True, help="BigQuery table")
    p.add_argument("--out", "-o", default=None, help="Output XML filename (default: <table>_<ts>.xml)")
    p.add_argument("--upload", action="store_true", help="Upload generated XML to GCS")
    p.add_argument("--bucket", "-b", default=None, help="GCS bucket to upload to (required if --upload)")
    p.add_argument("--gcs-path", default=None, help="GCS destination path (e.g. transformed_xml_files/defaulters.xml)")
    p.add_argument("--billing-project", default=None, help="Billing project id for requester-pays buckets (optional)")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    # Acquire ADC (credentials + default project)
    creds, adc_project = google.auth.default()
    # Determine effective project for BigQuery client if not explicitly provided
    effective_project = args.project if args.project else adc_project
    if not effective_project:
        print("ERROR: No GCP project provided and ADC did not provide a default project. Use --project or set gcloud config project.", file=sys.stderr)
        sys.exit(3)

    # Build clients container
    clients = Clients(project=effective_project, creds=creds)

    # Prepare out file name
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_name = args.out if args.out else f"{args.table}_{ts}.xml"
    out_path = out_name

    # Step 1: stream table to xml
    try:
        stream_table_to_xml_file(clients, effective_project, args.dataset, args.table, out_path)
    except Exception as ex:
        print(f"[{now_iso_z()}] Failed to export table to XML: {ex}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)

    # Step 2: upload if requested
    if args.upload:
        if not args.bucket:
            print("ERROR: --upload requires --bucket to be set.", file=sys.stderr)
            sys.exit(4)
        dest_path = args.gcs_path if args.gcs_path else f"transformed_xml_files/{out_name}"
        try:
            upload_file_to_gcs(clients, out_path, args.bucket, dest_path, billing_project=args.billing_project)
            print(f"[{now_iso_z()}] Upload complete.")
        except Exception as ex:
            print(f"[{now_iso_z()}] Upload failed: {ex}", file=sys.stderr)
            # traceback already printed inside upload_file_to_gcs
            sys.exit(2)


if __name__ == "__main__":
    main()