import os
import json
import gzip
from datetime import datetime
from decimal import Decimal
from io import BytesIO

import boto3
import pandas as pd

s3 = boto3.client("s3")


def list_bronze_files(bucket: str, series_id: str) -> list[dict]:
    """
    List all Bronze files for a given series.
    Returns list of dicts with key and metadata extracted from path.
    """
    prefix = f"bronze/source=BoC/series={series_id}/"
    
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("observations.json.gz"):
                # Extract metadata from path
                # Format: bronze/source=BoC/series=FXUSDCAD/ingest_date=.../ingest_ts=.../observations.json.gz
                parts = {p.split("=")[0]: p.split("=")[1] 
                        for p in key.split("/") 
                        if "=" in p}
                
                files.append({
                    "key": key,
                    "ingest_date": parts.get("ingest_date"),
                    "ingest_ts": parts.get("ingest_ts"),
                })
    
    print(f"Found {len(files)} Bronze files for {series_id}")
    return files


def read_bronze_file(bucket: str, file_info: dict) -> tuple[list[dict], dict]:
    """
    Read and parse a single Bronze file.
    Returns observations and metadata for lineage.
    """
    key = file_info["key"]
    
    # Download and decompress
    response = s3.get_object(Bucket=bucket, Key=key)
    compressed = response["Body"].read()
    raw_json = gzip.decompress(compressed).decode("utf-8")
    
    # Parse JSON
    data = json.loads(raw_json)
    observations = data.get("observations", [])
    
    # Build lineage metadata
    lineage = {
        "raw_s3_key": key,
        "ingest_ts": file_info["ingest_ts"],
    }
    
    return observations, lineage


def read_bronze_metadata(bucket: str, file_info: dict) -> dict:
    """
    Read the _meta.json file to get ingested_at timestamp.
    """
    # Replace observations.json.gz with _meta.json
    meta_key = file_info["key"].replace("observations.json.gz", "_meta.json")
    
    try:
        response = s3.get_object(Bucket=bucket, Key=meta_key)
        meta = json.loads(response["Body"].read().decode("utf-8"))
        return meta
    except Exception as e:
        print(f"Warning: Could not read metadata {meta_key}: {e}")
        return {}


def parse_series_id(series_id: str) -> tuple[str, str]:
    """
    Parse series_id into base and quote currency.
    
    i.e.
    FXUSDCAD → (USD, CAD)
    FXEURCAD → (EUR, CAD)
    """
    # BoC format: FX{BASE}{QUOTE} where both are 3-letter codes
    if series_id.startswith("FX") and len(series_id) == 8:
        base = series_id[2:5]
        quote = series_id[5:8]
        return base, quote
    else:
        # Unknown format, return None
        return None, None


def parse_observations(
    observations: list[dict], 
    series_id: str, 
    lineage: dict,
    ingested_at: str,
    run_id: str,
) -> list[dict]:
    """
    Parse BoC observations into clean Silver records.
    """
    records = []
    
    # Parse currency pair
    base_currency, quote_currency = parse_series_id(series_id)
    
    for obs in observations:
        date_str = obs.get("d")  # BoC uses "d" for date
        rate_value = obs.get(series_id, {})
        
        # Rate can be nested like {"v": "1.3456"} or direct
        if isinstance(rate_value, dict):
            value = rate_value.get("v")
        else:
            value = rate_value
        
        if date_str and value:
            records.append({
                # Core observation
                "obs_date": date_str,
                "series_id": series_id,
                "value": float(value),  # Will convert to Decimal in DataFrame
                
                # Parsed convenience columns
                "base_currency": base_currency,
                "quote_currency": quote_currency,
                
                # Source
                "source": "bankofcanada_valet",
                
                # Lineage
                "ingested_at": ingested_at,
                "run_id": run_id,
                "raw_s3_key": lineage["raw_s3_key"],
            })
    
    return records


def process_series(bucket: str, series_id: str, run_id: str) -> dict:
    """
    Process all Bronze files for a series into Silver.
    """
    # 1. List all Bronze files
    bronze_files = list_bronze_files(bucket, series_id)
    
    if not bronze_files:
        return {"series_id": series_id, "status": "no_files", "records": 0}
    
    # 2. Read and parse all files
    all_records = []
    for file_info in bronze_files:
        try:
            # Read observations
            observations, lineage = read_bronze_file(bucket, file_info)
            
            # Read metadata for ingested_at
            meta = read_bronze_metadata(bucket, file_info)
            ingested_at = meta.get("retrieved_at_utc", file_info["ingest_ts"])
            
            # Parse into records
            records = parse_observations(
                observations=observations,
                series_id=series_id,
                lineage=lineage,
                ingested_at=ingested_at,
                run_id=run_id,
            )
            all_records.extend(records)
            
        except Exception as e:
            print(f"Error reading {file_info['key']}: {e}")
            continue
    
    if not all_records:
        return {"series_id": series_id, "status": "no_records", "records": 0}
    
    # 3. Create DataFrame
    df = pd.DataFrame(all_records)
    df["obs_date"] = pd.to_datetime(df["obs_date"]).dt.date
    
    # 4. Deduplicate: keep latest ingestion for each obs_date
    #    Sort by ingested_at descending, keep first occurrence
    df = df.sort_values("ingested_at", ascending=False)
    df = df.drop_duplicates(subset=["series_id", "obs_date"], keep="first")
    df = df.sort_values("obs_date")
    
    # 5. Add processing timestamp
    df["processed_at"] = datetime.utcnow().isoformat()
    
    print(f"Deduplicated to {len(df)} unique observations for {series_id}")
    
    # 6. Write to Silver, partitioned by obs_date
    records_written = 0
    for obs_date, group in df.groupby("obs_date"):
        # Convert to Parquet
        parquet_buffer = BytesIO()
        group.to_parquet(parquet_buffer, index=False, engine="pyarrow")
        parquet_buffer.seek(0)
        
        # S3 key
        ds = obs_date.isoformat()
        key = f"silver/source=BoC/series={series_id}/ds={ds}/data.parquet"
        
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=parquet_buffer.getvalue(),
            ContentType="application/octet-stream",
        )
        records_written += len(group)
    
    return {
        "series_id": series_id,
        "status": "success",
        "records": records_written,
        "unique_dates": df["obs_date"].nunique(),
        "date_range": {
            "min": str(df["obs_date"].min()),
            "max": str(df["obs_date"].max()),
        },
    }


def lambda_handler(event, context):
    """
    Silver transformation Lambda.
    
    Reads all Bronze files, deduplicates by observation date,
    and writes clean Parquet files to Silver layer.
    
    Event parameters:
        - run_id (optional): Identifier for this run. Defaults to timestamp.
    """
    bucket = os.environ["BUCKET"]
    series_ids = os.getenv("SERIES_IDS").split(",")
    
    # Generate run_id if not provided
    run_id = event.get("run_id", f"silver_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}")
    
    print(f"Starting Silver processing, run_id={run_id}")
    
    results = []
    
    for series_id in series_ids:
        series_id = series_id.strip()
        print(f"Processing {series_id}...")
        
        try:
            result = process_series(bucket, series_id, run_id)
            results.append(result)
            print(f"  ✓ {series_id}: {result.get('records', 0)} records")
        except Exception as e:
            print(f"  ✗ {series_id}: {str(e)}")
            results.append({
                "series_id": series_id,
                "status": "error",
                "error": str(e),
            })
    
    return {
        "ok": all(r.get("status") == "success" for r in results),
        "run_id": run_id,
        "processed_at": datetime.utcnow().isoformat(),
        "results": results,
    }
