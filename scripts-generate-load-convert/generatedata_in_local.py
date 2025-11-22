#!/usr/bin/env python3
"""
generatedata.py

Generates a loans CSV and uploads it to a GCS bucket under source_data/ folder.

Default bucket: ikl-finance-bucket-002
"""

import argparse
import random
import os
import sys
import time
from datetime import datetime, timedelta
import pandas as pd

try:
    from google.cloud import storage
    GCLOUD_AVAILABLE = True
except Exception:
    GCLOUD_AVAILABLE = False

DEFAULT_BUCKET_NAME = "ikl-finance-bucket-002"
DEFAULT_GCS_DEST_PREFIX = "source_data"

def add_months(start_date, months):
    year = start_date.year + (start_date.month - 1 + months) // 12
    month = (start_date.month - 1 + months) % 12 + 1
    mdays = [31,
             29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
             31,30,31,30,31,31,30,31,30,31]
    day = min(start_date.day, mdays[month - 1])
    return datetime(year, month, day)

def generate_loans_csv(path="loans.csv", n_rows=200, seed=None):
    if seed is not None:
        random.seed(seed)

    first_names = ["Asha","Ravi","Priya","Suresh","Kiran","Neha","Amit","Sanjay","Anita","Rahul",
                   "Deepa","Vikram","Meera","Arjun","Lakshmi","Kavita","Ramesh","Anjali","Manoj","Pooja"]
    last_names = ["Sharma","Patel","Rao","Kumar","Singh","Gupta","Nair","Iyer","Menon",
                  "Chowdhury","Desai","Kapoor","Joshi","Varma","Khan","Naik"]
    statuses = ["Active", "Closed", "Default"]
    start_base = datetime(2018, 1, 1)
    end_base = datetime(2025, 10, 1)

    rows = []
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

        rows.append({
            "Loan_ID": loan_id,
            "Cust_Name": cust_name,
            "Loan_Amount": f"{loan_amount:.2f}",
            "Int_Rate": f"{int_rate:.2f}",
            "Instalments": instalments,
            "Start_Date": start_date.strftime("%Y-%m-%d"),
            "End_Date": end_date.strftime("%Y-%m-%d"),
            "Status": status
        })

    df = pd.DataFrame(rows)

    dir_name = os.path.dirname(path)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)

    df.to_csv(path, index=False)
    return path

def upload_to_gcs(local_path, bucket_name, destination_blob_name=None, retries=3, backoff=2):
    if not GCLOUD_AVAILABLE:
        raise RuntimeError("google-cloud-storage not installed. Run: pip install google-cloud-storage")

    if destination_blob_name is None:
        destination_blob_name = os.path.basename(local_path)

    client = storage.Client()  # uses ADC or GOOGLE_APPLICATION_CREDENTIALS
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    attempt = 0
    while attempt < retries:
        try:
            blob.upload_from_filename(local_path)
            return f"gs://{bucket_name}/{destination_blob_name}"
        except Exception as e:
            attempt += 1
            if attempt >= retries:
                raise
            time.sleep(backoff ** attempt)

def main(argv=None):
    parser = argparse.ArgumentParser(description="Generate loans CSV and upload to GCS")
    parser.add_argument("--path", "-p", default="loans.csv", help="Local file path for CSV")
    parser.add_argument("--rows", "-r", type=int, default=200, help="Number of rows to generate")
    parser.add_argument("--bucket", "-b", default=DEFAULT_BUCKET_NAME,
                        help=f"GCS bucket name (default: {DEFAULT_BUCKET_NAME})")
    parser.add_argument("--dest", "-d", default=None, help="Destination filename in GCS (object name)")
    parser.add_argument("--seed", type=int, default=None, help="Random seed")

    args = parser.parse_args(argv)

    out_path = generate_loans_csv(path=args.path, n_rows=args.rows, seed=args.seed)
    print(f"Generated CSV: {out_path}")

    # if user didn't provide dest, use source_data/<local_basename>
    if args.dest:
        dest_name = args.dest
    else:
        dest_name = f"{DEFAULT_GCS_DEST_PREFIX}/{os.path.basename(out_path)}"

    print(f"Uploading to bucket: {args.bucket} as {dest_name}")
    try:
        gcs_uri = upload_to_gcs(out_path, args.bucket, destination_blob_name=dest_name)
        print(f"Upload successful: {gcs_uri}")
    except Exception as e:
        print("Upload failed:", e, file=sys.stderr)
        if "Permission" in str(e):
            print("Hint: Check IAM permissions and GOOGLE_APPLICATION_CREDENTIALS", file=sys.stderr)
        sys.exit(3)

if __name__ == "__main__":
    main()