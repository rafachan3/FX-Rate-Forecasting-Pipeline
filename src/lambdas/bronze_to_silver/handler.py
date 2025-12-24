import os
import json
import gzip
from datetime import datetime, timezone
from io import BytesIO

import boto3
import pandas as pd

s3 = boto3.client("s3")


# =============================================================================
# WATERMARK TRACKING
# =============================================================================

def get_watermark(bucket: str, series_id: str) -> dict:
    """
    Read the watermark file to get last processed state.
    
    Returns:
        {
            "last_ingest_ts": "20250115T120000Z",  # Latest Bronze ingest_ts processed
            "last_obs_date": "2025-01-15",          # Latest observation date in Silver
            "updated_at": "2025-01-15T12:30:00Z"
        }
    """
    key = f"silver/source=BoC/series={series_id}/_watermark.json"
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return {}
    except Exception as e:
        print(f"Warning: Could not read watermark: {e}")
        return {}


def update_watermark(bucket: str, series_id: str, last_ingest_ts: str, last_obs_date: str):
    """
    Update the watermark file after successful processing.
    """
    key = f"silver/source=BoC/series={series_id}/_watermark.json"
    
    watermark = {
        "last_ingest_ts": last_ingest_ts,
        "last_obs_date": last_obs_date,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(watermark, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    
    print(f"Updated watermark: {watermark}")


# =============================================================================
# BRONZE FILE DISCOVERY
# =============================================================================

def list_bronze_files(bucket: str, series_id: str, after_ingest_ts: str = None) -> list[dict]:
    """
    List Bronze files for a given series, optionally filtering to only new files.
    
    Args:
        bucket: S3 bucket name
        series_id: e.g., "FXUSDCAD"
        after_ingest_ts: If provided, only return files with ingest_ts > this value
    
    Returns:
        List of dicts with key and metadata extracted from path, sorted by ingest_ts
    """
    prefix = f"bronze/source=BoC/series={series_id}/"
    
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("observations.json.gz"):
                # Extract metadata from path
                parts = {p.split("=")[0]: p.split("=")[1] 
                        for p in key.split("/") 
                        if "=" in p}
                
                ingest_ts = parts.get("ingest_ts", "")
                
                # Filter: only include files newer than watermark
                if after_ingest_ts and ingest_ts <= after_ingest_ts:
                    continue
                
                files.append({
                    "key": key,
                    "ingest_date": parts.get("ingest_date"),
                    "ingest_ts": ingest_ts,
                })
    
    # Sort by ingest_ts to process in order
    files.sort(key=lambda x: x["ingest_ts"])
    
    print(f"Found {len(files)} Bronze files for {series_id}" + 
          (f" (after {after_ingest_ts})" if after_ingest_ts else " (full scan)"))
    
    return files


# =============================================================================
# BRONZE FILE READING
# =============================================================================

def read_bronze_file(bucket: str, file_info: dict) -> tuple[list[dict], dict]:
    """
    Read and parse a single Bronze file.
    Returns observations and metadata for lineage.
    """
    key = file_info["key"]
    
    response = s3.get_object(Bucket=bucket, Key=key)
    compressed = response["Body"].read()
    raw_json = gzip.decompress(compressed).decode("utf-8")
    
    data = json.loads(raw_json)
    observations = data.get("observations", [])
    
    lineage = {
        "raw_s3_key": key,
        "ingest_ts": file_info["ingest_ts"],
    }
    
    return observations, lineage


def read_bronze_metadata(bucket: str, file_info: dict) -> dict:
    """
    Read the _meta.json file to get ingested_at timestamp.
    """
    meta_key = file_info["key"].replace("observations.json.gz", "_meta.json")
    
    try:
        response = s3.get_object(Bucket=bucket, Key=meta_key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"Warning: Could not read metadata {meta_key}: {e}")
        return {}


# =============================================================================
# PARSING
# =============================================================================

def parse_series_id(series_id: str) -> tuple[str, str]:
    """
    Parse series_id into base and quote currency.
    FXUSDCAD → (USD, CAD)
    """
    if series_id.startswith("FX") and len(series_id) == 8:
        return series_id[2:5], series_id[5:8]
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


# =============================================================================
# SILVER READ/WRITE WITH MERGE
# =============================================================================

def read_existing_silver(bucket: str, series_id: str, obs_dates: list[str]) -> pd.DataFrame:
    """
    Read existing Silver data for specific observation dates.
    
    This allows us to merge new data with existing data for the same dates.
    """
    dfs = []
    
    for obs_date in obs_dates:
        key = f"silver/source=BoC/series={series_id}/ds={obs_date}/data.parquet"
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            df = pd.read_parquet(BytesIO(response["Body"].read()))
            dfs.append(df)
        except s3.exceptions.NoSuchKey:
            # No existing data for this date, that's fine
            pass
        except Exception as e:
            print(f"Warning: Could not read {key}: {e}")
    
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()


def write_silver_partitions(bucket: str, series_id: str, df: pd.DataFrame) -> int:
    """
    Write Silver data, partitioned by obs_date.
    Returns number of records written.
    """
    records_written = 0
    
    for obs_date, group in df.groupby("obs_date"):
        parquet_buffer = BytesIO()
        group.to_parquet(parquet_buffer, index=False, engine="pyarrow")
        parquet_buffer.seek(0)
        
        ds = obs_date if isinstance(obs_date, str) else obs_date.isoformat()
        key = f"silver/source=BoC/series={series_id}/ds={ds}/data.parquet"
        
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=parquet_buffer.getvalue(),
            ContentType="application/octet-stream",
        )
        records_written += len(group)
    
    return records_written


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def process_series(bucket: str, series_id: str, run_id: str, full_refresh: bool = False) -> dict:
    """
    Process Bronze files into Silver layer.
    
    Args:
        bucket: S3 bucket
        series_id: e.g., "FXUSDCAD"
        run_id: Unique identifier for this run
        full_refresh: If True, reprocess all Bronze files (ignore watermark)
    """
    # 1. Get watermark (last processed state)
    watermark = {} if full_refresh else get_watermark(bucket, series_id)
    last_ingest_ts = watermark.get("last_ingest_ts")
    
    if full_refresh:
        print(f"[FULL REFRESH] Processing all Bronze files")
    elif last_ingest_ts:
        print(f"[INCREMENTAL] Processing files after {last_ingest_ts}")
    else:
        print(f"[INITIAL LOAD] No watermark found, processing all files")
    
    # 2. List Bronze files (filtered by watermark if incremental)
    bronze_files = list_bronze_files(bucket, series_id, after_ingest_ts=last_ingest_ts)
    
    if not bronze_files:
        return {
            "series_id": series_id,
            "status": "no_new_files",
            "records": 0,
            "message": "No new Bronze files to process"
        }
    
    # 3. Read and parse all new Bronze files
    all_records = []
    latest_ingest_ts = last_ingest_ts or ""
    
    for file_info in bronze_files:
        try:
            observations, lineage = read_bronze_file(bucket, file_info)
            meta = read_bronze_metadata(bucket, file_info)
            ingested_at = meta.get("retrieved_at_utc", file_info["ingest_ts"])
            
            records = parse_observations(
                observations=observations,
                series_id=series_id,
                lineage=lineage,
                ingested_at=ingested_at,
                run_id=run_id,
            )
            all_records.extend(records)
            
            # Track latest ingest_ts for watermark
            if file_info["ingest_ts"] > latest_ingest_ts:
                latest_ingest_ts = file_info["ingest_ts"]
                
        except Exception as e:
            print(f"Error reading {file_info['key']}: {e}")
            continue
    
    if not all_records:
        return {
            "series_id": series_id,
            "status": "no_records",
            "records": 0,
            "message": "Bronze files contained no valid observations"
        }
    
    # 4. Create DataFrame from new records
    new_df = pd.DataFrame(all_records)
    new_df["obs_date"] = pd.to_datetime(new_df["obs_date"]).dt.date
    
    # Get unique obs_dates in new data
    new_obs_dates = [str(d) for d in new_df["obs_date"].unique()]
    print(f"New data covers {len(new_obs_dates)} dates: {min(new_obs_dates)} to {max(new_obs_dates)}")
    
    # 5. Read existing Silver data for these dates (for merge)
    existing_df = read_existing_silver(bucket, series_id, new_obs_dates)
    
    if not existing_df.empty:
        existing_df["obs_date"] = pd.to_datetime(existing_df["obs_date"]).dt.date
        print(f"Found {len(existing_df)} existing records for overlapping dates")
        
        # Combine: new data takes precedence (newer ingestion)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df
    
    # 6. Deduplicate: keep latest ingestion for each obs_date
    combined_df = combined_df.sort_values("ingested_at", ascending=False)
    combined_df = combined_df.drop_duplicates(subset=["series_id", "obs_date"], keep="first")
    combined_df = combined_df.sort_values("obs_date")
    
    # 7. Add processing timestamp
    combined_df["processed_at"] = datetime.now(timezone.utc).isoformat()
    
    print(f"After deduplication: {len(combined_df)} records")
    
    # 8. Write to Silver (only affected partitions)
    records_written = write_silver_partitions(bucket, series_id, combined_df)
    
    # 9. Update watermark
    latest_obs_date = str(combined_df["obs_date"].max())
    update_watermark(bucket, series_id, latest_ingest_ts, latest_obs_date)
    
    return {
        "series_id": series_id,
        "status": "success",
        "mode": "full_refresh" if full_refresh else "incremental",
        "bronze_files_processed": len(bronze_files),
        "records_written": records_written,
        "dates_affected": len(new_obs_dates),
        "date_range": {
            "min": min(new_obs_dates),
            "max": max(new_obs_dates),
        },
        "watermark": {
            "ingest_ts": latest_ingest_ts,
            "obs_date": latest_obs_date,
        },
    }


def lambda_handler(event, context):
    """
    Silver transformation Lambda (incremental).
    
    Reads NEW Bronze files since last run, deduplicates by observation date,
    and writes/updates Silver partitions.
    
    Event parameters:
        - run_id (optional): Identifier for this run
        - full_refresh (optional): If true, reprocess all Bronze files
    """
    bucket = os.environ["BUCKET"]
    series_ids = os.getenv("SERIES_IDS", "FXUSDCAD,FXEURCAD").split(",")
    
    run_id = event.get("run_id", f"silver_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}")
    full_refresh = event.get("full_refresh", False)
    
    print(f"Starting Silver processing, run_id={run_id}, full_refresh={full_refresh}")
    
    results = []
    
    for series_id in series_ids:
        series_id = series_id.strip()
        print(f"\n{'='*60}\nProcessing {series_id}...\n{'='*60}")
        
        try:
            result = process_series(bucket, series_id, run_id, full_refresh)
            results.append(result)
            print(f"✓ {series_id}: {result.get('records_written', 0)} records, {result.get('status')}")
        except Exception as e:
            print(f"✗ {series_id}: {str(e)}")
            results.append({
                "series_id": series_id,
                "status": "error",
                "error": str(e),
            })
    
    return {
        "ok": all(r.get("status") in ("success", "no_new_files") for r in results),
        "run_id": run_id,
        "full_refresh": full_refresh,
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "results": results,
    }
