#!/usr/bin/env python3
"""
bq_to_gcs_xml.py

Stream a BigQuery table directly to a GCS object (XML) using binary writes only.
This avoids TextIOWrapper/flush issues and works across google-cloud-storage versions.

Requirements:
    pip install google-cloud-bigquery google-cloud-storage google-auth

Usage example:
    python bq_to_gcs_xml.py \
      --project student-00380 \
      --dataset CL_TRANSFORMED \
      --table defaulters \
      --upload \
      --bucket ikl-finance-bucket-002 \
      --gcs-path transformed_xml_files/defaulters.xml

If bucket uses requester-pays:
    add: --billing-project my-billing-project
"""
from __future__ import annotations
import argparse
import sys
import time
import io
import traceback
from datetime import datetime, timezone
from typing import Optional, Any

import google.auth
from google.cloud import bigquery, storage
from xml.sax.saxutils import escape as xml_escape

# Helpers
def now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def safe_format_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        dt = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8")
        except Exception:
            return str(value)
    if isinstance(value, (list, dict, tuple, set)):
        return str(value)
    return str(value)

# Core: binary streaming + fallback
def stream_table_to_gcs_binary_with_fallback(
    storage_client: storage.Client,
    bq_client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    bucket_name: str,
    object_name: str,
    retries: int = 3,
    backoff: int = 2
) -> str:
    """
    Stream BigQuery rows directly into gs://bucket_name/object_name using binary writes.
    Falls back to in-memory upload_from_string if streaming isn't possible.
    Returns the gs:// URI on success.
    """
    table_ref = f"{project}.{dataset}.{table}"
    print(f"[{now_z()}] Preparing export for table: {table_ref}")

    # get table and schema once
    table_obj = bq_client.get_table(table_ref)
    schema_columns = [f.name for f in table_obj.schema]
    print(f"[{now_z()}] Columns: {schema_columns}")

    bucket = storage_client.lookup_bucket(bucket_name)
    if bucket is None:
        raise RuntimeError(f"Bucket '{bucket_name}' not found or not accessible with current credentials.")
    blob = bucket.blob(object_name)
    gcs_uri = f"gs://{bucket_name}/{object_name}"

    attempt = 0
    while attempt < retries:
        attempt += 1
        try:
            print(f"[{now_z()}] Streaming attempt {attempt}/{retries} to {gcs_uri}")

            # fresh RowIterator
            rows = bq_client.list_rows(table_obj)

            # open fresh raw binary stream
            try:
                raw_stream = blob.open("wb")
            except Exception as e:
                raw_stream = None

            if raw_stream is None:
                # streaming not supported by client -> goto fallback
                raise RuntimeError("blob.open('wb') not supported by storage client (cannot stream)")

            written = 0
            try:
                # Write bytes directly
                header = '<?xml version="1.0" encoding="UTF-8"?>\n'
                raw_stream.write(header.encode("utf-8"))
                raw_stream.write("<Defaulters>\n".encode("utf-8"))

                for row in rows:
                    raw_stream.write("  <Defaulter>\n".encode("utf-8"))
                    for col in schema_columns:
                        try:
                            value = row.get(col) if hasattr(row, "get") else row[col]
                        except Exception:
                            try:
                                value = row[col]
                            except Exception:
                                value = getattr(row, col, None)

                        if value is None:
                            raw_stream.write(f"    <{col}></{col}>\n".encode("utf-8"))
                        else:
                            sval = safe_format_value(value)
                            raw_stream.write(f"    <{col}>{xml_escape(sval)}</{col}>\n".encode("utf-8"))

                    raw_stream.write("  </Defaulter>\n".encode("utf-8"))
                    written += 1
                    if written % 1000 == 0:
                        print(f"[{now_z()}] Processed {written} rows...")

                raw_stream.write("</Defaulters>\n".encode("utf-8"))

            finally:
                # finalize upload
                try:
                    raw_stream.close()
                except Exception:
                    pass

            print(f"[{now_z()}] Successfully streamed XML to {gcs_uri} (rows: {written})")
            return gcs_uri

        except Exception as exc:
            print(f"[{now_z()}] Streaming attempt {attempt} failed: {exc}", file=sys.stderr)
            traceback.print_exc()
            # ensure any raw_stream is closed (avoid reusing closed streams)
            try:
                raw_stream.close()
            except Exception:
                pass

            if attempt >= retries:
                print(f"[{now_z()}] Streaming failed after {attempt} attempts; falling back to in-memory upload.", file=sys.stderr)
                break

            wait = backoff ** attempt
            print(f"[{now_z()}] Retrying streaming in {wait}s...", file=sys.stderr)
            time.sleep(wait)

    # Fallback: build XML in memory and upload_from_string
    print(f"[{now_z()}] Falling back to in-memory XML build (may use significant memory).", file=sys.stderr)
    sio = io.StringIO()
    written = 0
    sio.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    sio.write("<Defaulters>\n")

    # fresh RowIterator for fallback
    rows = bq_client.list_rows(table_obj)
    for row in rows:
        sio.write("  <Defaulter>\n")
        for col in schema_columns:
            try:
                value = row.get(col) if hasattr(row, "get") else row[col]
            except Exception:
                try:
                    value = row[col]
                except Exception:
                    value = getattr(row, col, None)
            if value is None:
                sio.write(f"    <{col}></{col}>\n")
            else:
                sval = safe_format_value(value)
                sio.write(f"    <{col}>{xml_escape(sval)}</{col}>\n")
        sio.write("  </Defaulter>\n")
        written += 1
        if written % 1000 == 0:
            print(f"[{now_z()}] Built {written} rows in memory...")

    sio.write("</Defaulters>\n")
    content_bytes = sio.getvalue().encode("utf-8")

    attempt = 0
    while attempt < retries:
        attempt += 1
        try:
            blob.upload_from_string(content_bytes, content_type="application/xml")
            print(f"[{now_z()}] Successfully uploaded XML to {gcs_uri} (rows: {written}) via upload_from_string")
            return gcs_uri
        except Exception as exc:
            print(f"[{now_z()}] upload_from_string attempt {attempt} failed: {exc}", file=sys.stderr)
            traceback.print_exc()
            if attempt >= retries:
                raise
            wait = backoff ** attempt
            print(f"[{now_z()}] Retrying upload in {wait}s...", file=sys.stderr)
            time.sleep(wait)

    raise RuntimeError("Failed to upload XML to GCS after retries and fallback.")

# CLI
def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Stream BigQuery table to GCS as XML (binary streaming).")
    p.add_argument("--project", "-p", default=None, help="GCP project id (default: use ADC project)")
    p.add_argument("--dataset", "-d", required=True, help="BigQuery dataset")
    p.add_argument("--table", "-t", required=True, help="BigQuery table")
    p.add_argument("--upload", action="store_true", help="Stream directly to GCS (required to upload)")
    p.add_argument("--bucket", "-b", default=None, help="GCS bucket (required if --upload)")
    p.add_argument("--gcs-path", default=None, help="GCS object path (e.g. transformed_xml_files/defaulters.xml)")
    p.add_argument("--billing-project", default=None, help="Billing project for requester-pays (optional)")
    p.add_argument("--retries", type=int, default=3, help="Retries for streaming/upload")
    return p.parse_args(argv)

def main(argv=None):
    args = parse_args(argv)

    creds, adc_project = google.auth.default()
    project = args.project if args.project else adc_project
    if not project:
        print("ERROR: no GCP project. Use --project or set ADC project.", file=sys.stderr)
        sys.exit(2)

    if args.upload and not args.bucket:
        print("ERROR: --upload requires --bucket", file=sys.stderr)
        sys.exit(3)

    bq_client = bigquery.Client(project=project, credentials=creds)
    storage_client = storage.Client(project=(args.billing_project if args.billing_project else project), credentials=creds)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    default_name = f"{args.table}_{ts}.xml"
    object_name = args.gcs_path if args.gcs_path else default_name

    if args.upload:
        gcs_uri = stream_table_to_gcs_binary_with_fallback(
            storage_client, bq_client, project, args.dataset, args.table,
            args.bucket, object_name, retries=args.retries, backoff=2
        )
        print(f"[{now_z()}] Upload complete: {gcs_uri}")
    else:
        print("No --upload specified. This script is optimized for streaming. Use --upload to push to GCS.")

if __name__ == "__main__":
    main()