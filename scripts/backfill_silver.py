#!/usr/bin/env python3
"""
Silver Layer Backfill Script

Runs the Bronze → Silver transformation locally without Lambda timeout constraints.
Use this for initial backfills or reprocessing large amounts of data.

Usage:
    # Process all series
    python scripts/backfill_silver.py --bucket fx-rate-pipeline-dev

    # Process specific series only
    python scripts/backfill_silver.py --bucket fx-rate-pipeline-dev --series FXUSDCAD FXEURCAD

    # Dry run (show what would be processed)
    python scripts/backfill_silver.py --bucket fx-rate-pipeline-dev --dry-run
"""

import argparse
import gzip
import json
import sys
import time
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import boto3
import pandas as pd

# Configuration

BOC_FX_SERIES = [
    "FXUSDCAD", "FXEURCAD", "FXGBPCAD", "FXJPYCAD", "FXAUDCAD",
    "FXCHFCAD", "FXCNYCAD", "FXHKDCAD", "FXMXNCAD", "FXINRCAD",
    "FXNZDCAD", "FXSARCAD", "FXSGDCAD", "FXZARCAD", "FXKRWCAD",
    "FXSEKCAD", "FXNOKCAD", "FXTRYCAD", "FXBRLCAD", "FXRUBCAD",
    "FXIDRCAD", "FXTWDCAD", "FXMYRCAD",
]

# Core Functions (from Lambda handler)

def list_bronze_files(s3_client, bucket: str, series_id: str) -> list[dict]:
    """List all Bronze files for a given series."""
    prefix = f"bronze/source=BoC/series={series_id}/"
    
    paginator = s3_client.get_paginator("list_objects_v2")
    files = []
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("observations.json.gz"):
                parts = {p.split("=")[0]: p.split("=")[1] 
                        for p in key.split("/") 
                        if "=" in p}
                files.append({
                    "key": key,
                    "ingest_date": parts.get("ingest_date"),
                    "ingest_ts": parts.get("ingest_ts"),
                })
    
    return files


def read_bronze_file(s3_client, bucket: str, file_info: dict) -> tuple[list[dict], dict]:
    """Read and parse a single Bronze file."""
    key = file_info["key"]
    
    response = s3_client.get_object(Bucket=bucket, Key=key)
    compressed = response["Body"].read()
    raw_json = gzip.decompress(compressed).decode("utf-8")
    
    data = json.loads(raw_json)
    observations = data.get("observations", [])
    
    lineage = {
        "raw_s3_key": key,
        "ingest_ts": file_info["ingest_ts"],
    }
    
    return observations, lineage


def read_bronze_metadata(s3_client, bucket: str, file_info: dict) -> dict:
    """Read the _meta.json file to get ingested_at timestamp."""
    meta_key = file_info["key"].replace("observations.json.gz", "_meta.json")
    
    try:
        response = s3_client.get_object(Bucket=bucket, Key=meta_key)
        meta = json.loads(response["Body"].read().decode("utf-8"))
        return meta
    except Exception as e:
        print(f"    Warning: Could not read metadata {meta_key}: {e}")
        return {}


def parse_series_id(series_id: str) -> tuple[str, str]:
    """Parse series_id into base and quote currency."""
    if series_id.startswith("FX") and len(series_id) == 8:
        base = series_id[2:5]
        quote = series_id[5:8]
        return base, quote
    return None, None


def parse_observations(
    observations: list[dict], 
    series_id: str, 
    lineage: dict,
    ingested_at: str,
    run_id: str,
) -> list[dict]:
    """Parse BoC observations into clean Silver records."""
    records = []
    base_currency, quote_currency = parse_series_id(series_id)
    
    for obs in observations:
        date_str = obs.get("d")
        rate_value = obs.get(series_id, {})
        
        if isinstance(rate_value, dict):
            value = rate_value.get("v")
        else:
            value = rate_value
        
        if date_str and value:
            records.append({
                "obs_date": date_str,
                "series_id": series_id,
                "value": float(value),
                "base_currency": base_currency,
                "quote_currency": quote_currency,
                "source": "bankofcanada_valet",
                "ingested_at": ingested_at,
                "run_id": run_id,
                "raw_s3_key": lineage["raw_s3_key"],
            })
    
    return records


def process_series(
    s3_client, 
    bucket: str, 
    series_id: str, 
    run_id: str,
    dry_run: bool = False,
) -> dict:
    """Process all Bronze files for a series into Silver."""
    
    start_time = time.time()
    
    # 1. List all Bronze files
    bronze_files = list_bronze_files(s3_client, bucket, series_id)
    
    if not bronze_files:
        return {
            "series_id": series_id,
            "status": "no_files",
            "records": 0,
            "duration_seconds": time.time() - start_time,
        }
    
    print(f"    Found {len(bronze_files)} Bronze files")
    
    # 2. Read and parse all files
    all_records = []
    for file_info in bronze_files:
        try:
            observations, lineage = read_bronze_file(s3_client, bucket, file_info)
            meta = read_bronze_metadata(s3_client, bucket, file_info)
            ingested_at = meta.get("retrieved_at_utc", file_info["ingest_ts"])
            
            records = parse_observations(
                observations=observations,
                series_id=series_id,
                lineage=lineage,
                ingested_at=ingested_at,
                run_id=run_id,
            )
            all_records.extend(records)
        except Exception as e:
            print(f"    Error reading {file_info['key']}: {e}")
            continue
    
    if not all_records:
        return {
            "series_id": series_id,
            "status": "no_records",
            "records": 0,
            "duration_seconds": time.time() - start_time,
        }
    
    # 3. Create DataFrame
    df = pd.DataFrame(all_records)
    df["obs_date"] = pd.to_datetime(df["obs_date"]).dt.date
    
    print(f"    Parsed {len(df)} raw observations")
    
    # 4. Deduplicate: keep latest ingestion for each obs_date
    df = df.sort_values("ingested_at", ascending=False)
    df = df.drop_duplicates(subset=["series_id", "obs_date"], keep="first")
    df = df.sort_values("obs_date")
    
    print(f"    Deduplicated to {len(df)} unique observations")
    
    # 5. Add processing timestamp
    df["processed_at"] = datetime.now(timezone.utc).isoformat()
    
    if dry_run:
        return {
            "series_id": series_id,
            "status": "dry_run",
            "records": len(df),
            "unique_dates": df["obs_date"].nunique(),
            "date_range": {
                "min": str(df["obs_date"].min()),
                "max": str(df["obs_date"].max()),
            },
            "duration_seconds": time.time() - start_time,
        }
    
    # 6. Write to Silver, partitioned by obs_date (matches Lambda behavior)
    records_written = 0
    for obs_date, group in df.groupby("obs_date"):
        parquet_buffer = BytesIO()
        group.to_parquet(parquet_buffer, index=False, engine="pyarrow")
        parquet_buffer.seek(0)
        
        ds = obs_date.isoformat()
        key = f"silver/source=BoC/series={series_id}/ds={ds}/data.parquet"
        
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=parquet_buffer.getvalue(),
            ContentType="application/octet-stream",
        )
        records_written += len(group)

    # 7. Create watermark so Lambda knows where to continue from
    latest_ingest_ts = max(f["ingest_ts"] for f in bronze_files)
    watermark = {
        "last_ingest_ts": latest_ingest_ts,
        "last_obs_date": str(df["obs_date"].max()),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    watermark_key = f"silver/source=BoC/series={series_id}/_watermark.json"
    s3_client.put_object(
        Bucket=bucket,
        Key=watermark_key,
        Body=json.dumps(watermark, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"  ✓ Watermark created: s3://{bucket}/{watermark_key}")
    print(f"   Last ingest_ts: {latest_ingest_ts}")
    print(f"   Last obs_date: {df['obs_date'].max()}")
    print(f"   Updated at: {datetime.now(timezone.utc).isoformat()}")
    
    return {
        "series_id": series_id,
        "status": "success",
        "records": records_written,
        "unique_dates": df["obs_date"].nunique(),
        "date_range": {
            "min": str(df["obs_date"].min()),
            "max": str(df["obs_date"].max()),
        },
        "duration_seconds": time.time() - start_time,
    }

# Main

def run_silver_backfill(
    bucket: str,
    series_list: list[str],
    dry_run: bool = False,
) -> dict:
    """Run the Silver backfill for all specified series."""
    
    s3_client = boto3.client("s3")
    run_id = f"silver_backfill_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    
    print("=" * 60)
    print("Silver Layer Backfill")
    print("=" * 60)
    print(f"  Bucket:    {bucket}")
    print(f"  Series:    {len(series_list)} to process")
    print(f"  Run ID:    {run_id}")
    print(f"  Dry run:   {dry_run}")
    print("=" * 60)
    
    if dry_run:
        print("⚠️  DRY RUN MODE - No data will be written to S3\n")
    
    results = []
    total_records = 0
    
    for i, series_id in enumerate(series_list, 1):
        print(f"\n[{i}/{len(series_list)}] Processing {series_id}...")
        
        try:
            result = process_series(
                s3_client=s3_client,
                bucket=bucket,
                series_id=series_id,
                run_id=run_id,
                dry_run=dry_run,
            )
            results.append(result)
            
            if result["status"] in ("success", "dry_run"):
                total_records += result["records"]
                print(f"    ✓ {result['records']:,} records ({result['duration_seconds']:.1f}s)")
            else:
                print(f"    ⚠ {result['status']}")
                
        except Exception as e:
            print(f"    ✗ Error: {e}")
            results.append({
                "series_id": series_id,
                "status": "error",
                "error": str(e),
            })
    
    # Summary
    success_count = sum(1 for r in results if r.get("status") in ("success", "dry_run"))
    failed_count = len(results) - success_count
    
    print("\n" + "=" * 60)
    print("SILVER BACKFILL COMPLETE")
    print("=" * 60)
    print(f"  ✓ Success:  {success_count}/{len(series_list)}")
    print(f"  ✗ Failed:   {failed_count}/{len(series_list)}")
    print(f"  📊 Records: {total_records:,}")
    
    if failed_count > 0:
        print(f"\n⚠️  Failed series:")
        for r in results:
            if r.get("status") not in ("success", "dry_run"):
                print(f"    - {r['series_id']}: {r.get('error', r.get('status'))}")
    
    return {
        "ok": failed_count == 0,
        "run_id": run_id,
        "results": results,
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Backfill Silver layer from Bronze data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument(
        "--bucket",
        required=True,
        help="S3 bucket name",
    )
    
    parser.add_argument(
        "--series",
        nargs="+",
        default=BOC_FX_SERIES,
        help="Series IDs to process (default: all 23)",
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be processed without writing to S3",
    )
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    result = run_silver_backfill(
        bucket=args.bucket,
        series_list=args.series,
        dry_run=args.dry_run,
    )
    
    sys.exit(0 if result["ok"] else 1)


if __name__ == "__main__":
    main()