#!/usr/bin/env python3
"""
generatedata_in_gcs.py

Generates a loans CSV and uploads it directly to a GCS bucket under the source_data/ folder.
No local file is created.

Default bucket: ikl-finance-bucket-002
"""
from __future__ import annotations
import argparse
import random
import sys
import time
import csv
import io
from datetime import datetime, timedelta

try:
    from google.cloud import storage
    GCLOUD_AVAILABLE = True
except Exception:
    GCLOUD_AVAILABLE = False

DEFAULT_BUCKET_NAME = "ikl-finance-bucket-002"
DEFAULT_GCS_DEST_PREFIX = "source_data"


def add_months(start_date: datetime, months: int) -> datetime:
    year = start_date.year + (start_date.month - 1 + months) // 12
    month = (start_date.month - 1 + months) % 12 + 1
    mdays = [31,
             29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
             31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    day = min(start_date.day, mdays[month - 1])
    return datetime(year, month, day)


def generate_loans_rows(n_rows=200, seed=None):
    """Yield dict rows for CSV (matching your previous schema)."""
    if seed is not None:
        random.seed(seed)

    first_names = ["Asha", "Ravi", "Priya", "Suresh", "Kiran", "Neha", "Amit", "Sanjay", "Anita", "Rahul",
                   "Deepa", "Vikram", "Meera", "Arjun", "Lakshmi", "Kavita", "Ramesh", "Anjali", "Manoj", "Pooja"]
    last_names = ["Sharma", "Patel", "Rao", "Kumar", "Singh", "Gupta", "Nair", "Iyer", "Menon",
                  "Chowdhury", "Desai", "Kapoor", "Joshi", "Varma", "Khan", "Naik"]
    statuses = ["Active", "Closed", "Default"]
    start_base = datetime(2018, 1, 1)
    end_base = datetime(2025, 10, 1)

    for i in range(1, n_rows + 1):
        loan_id = f"L{i:06d}"
        cust_name = f"{random.choice(first_names)} {random.choice(last_names)}"
        loan_amount = round(random.uniform(5000, 500000), 2)
        int_rate = round(random.uniform(6.0, 22.0), 2)
        instalments = random.choice([12, 24, 36, 48, 60, 72, 84, 96])
        start_days = random.randint(0, (end_base - start_base).days)
        start_date = start_base + timedelta(days=start_days)
        end_date = add_months(start_date, instalments)
        status = random.choices(statuses, weights=[0.7, 0.25, 0.05])[0]

        yield {
            "Loan_ID": loan_id,
            "Cust_Name": cust_name,
            "Loan_Amount": f"{loan_amount:.2f}",
            "Int_Rate": f"{int_rate:.2f}",
            "Instalments": instalments,
            "Start_Date": start_date.strftime("%Y-%m-%d"),
            "End_Date": end_date.strftime("%Y-%m-%d"),
            "Status": status
        }


def stream_csv_to_gcs(bucket_name: str, object_name: str, rows_iter, retries=3, backoff=2) -> str:
    """
    Stream CSV rows to gs://bucket_name/object_name using blob.open("wb") and TextIO wrapper.
    If blob.open() isn't supported by client, fall back to building in-memory string and upload_from_string.
    Returns the GCS URI on success.
    """
    if not GCLOUD_AVAILABLE:
        raise RuntimeError("google-cloud-storage not installed. Run: pip install google-cloud-storage")

    client = storage.Client()
    bucket = client.lookup_bucket(bucket_name)
    if bucket is None:
        raise RuntimeError(f"Bucket '{bucket_name}' not found or not accessible with current credentials.")

    blob = bucket.blob(object_name)
    gcs_uri = f"gs://{bucket_name}/{object_name}"

    # Try streaming write
    try:
        raw_stream = blob.open("wb")  # binary stream
    except TypeError:
        raw_stream = None

    if raw_stream is not None:
        attempt = 0
        while attempt < retries:
            try:
                with raw_stream:
                    # text wrapper so csv.writer writes strings; newline='' to avoid extra newlines
                    with io.TextIOWrapper(raw_stream, encoding="utf-8", newline="") as text_stream:
                        writer = csv.writer(text_stream)
                        # header
                        header = ["Loan_ID", "Cust_Name", "Loan_Amount", "Int_Rate", "Instalments", "Start_Date", "End_Date", "Status"]
                        writer.writerow(header)
                        written = 0
                        for r in rows_iter:
                            writer.writerow([r[h] for h in header])
                            written += 1
                            if written % 1000 == 0:
                                print(f"[{datetime.utcnow().isoformat()}Z] Generated {written} rows...")
                        text_stream.flush()
                print(f"[{datetime.utcnow().isoformat()}Z] Successfully streamed CSV to {gcs_uri} (rows: {written})")
                return gcs_uri
            except Exception as e:
                attempt += 1
                if attempt >= retries:
                    print(f"Streaming upload failed after {attempt} attempts: {e}", file=sys.stderr)
                    raise
                wait = backoff ** attempt
                print(f"Streaming upload failed (attempt {attempt}), retrying in {wait}s...", file=sys.stderr)
                time.sleep(wait)

    # Fallback: assemble CSV in memory and upload_from_string
    print("WARNING: Storage client does not support blob.open() — falling back to in-memory upload (may use significant memory).", file=sys.stderr)
    sio = io.StringIO()
    writer = csv.writer(sio, lineterminator="\n")
    header = ["Loan_ID", "Cust_Name", "Loan_Amount", "Int_Rate", "Instalments", "Start_Date", "End_Date", "Status"]
    writer.writerow(header)
    written = 0
    for r in rows_iter:
        writer.writerow([r[h] for h in header])
        written += 1
        if written % 1000 == 0:
            print(f"[{datetime.utcnow().isoformat()}Z] Generated {written} rows...")
    content = sio.getvalue()
    attempt = 0
    while attempt < retries:
        try:
            blob.upload_from_string(content.encode("utf-8"), content_type="text/csv")
            print(f"[{datetime.utcnow().isoformat()}Z] Successfully uploaded CSV to {gcs_uri} (rows: {written})")
            return gcs_uri
        except Exception:
            attempt += 1
            if attempt >= retries:
                print("Upload failed after retries.", file=sys.stderr)
                raise
            wait = backoff ** attempt
            print(f"Upload failed (attempt {attempt}), retrying in {wait}s...", file=sys.stderr)
            time.sleep(wait)


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Generate loans CSV and upload directly to GCS (no local files).")
    p.add_argument("--rows", "-r", type=int, default=200, help="Number of rows to generate")
    p.add_argument("--bucket", "-b", default=DEFAULT_BUCKET_NAME, help=f"GCS bucket name (default: {DEFAULT_BUCKET_NAME})")
    p.add_argument("--dest", "-d", default=None, help="Destination object name in GCS (e.g. source_data/loans.csv). If omitted defaults to source_data/loans_<ts>.csv")
    p.add_argument("--seed", type=int, default=None, help="Random seed")
    p.add_argument("--retries", type=int, default=3, help="Upload retries for transient failures")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    default_name = f"loans.csv" # If needed with timestamp f"loans_{ts}.csv"
    dest = args.dest if args.dest else f"{DEFAULT_GCS_DEST_PREFIX}/{default_name}"

    print(f"Generating {args.rows} rows and streaming to gs://{args.bucket}/{dest} ...")

    rows_iter = generate_loans_rows(n_rows=args.rows, seed=args.seed)

    try:
        gcs_uri = stream_csv_to_gcs(args.bucket, dest, rows_iter, retries=args.retries)
        print(f"Upload successful: {gcs_uri}")
    except Exception as e:
        print("Upload failed:", e, file=sys.stderr)
        if "Permission" in str(e):
            print("Hint: Check IAM permissions and GOOGLE_APPLICATION_CREDENTIALS", file=sys.stderr)
        sys.exit(3)


if __name__ == "__main__":
    main()