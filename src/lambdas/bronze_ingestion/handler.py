import os
import json
import gzip
import hashlib
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import boto3

s3 = boto3.client("s3")


def fetch_boc(series_id: str, start_date: str, end_date: str, fmt: str):
    """Fetch observations from Bank of Canada Valet API."""
    base = f"https://www.bankofcanada.ca/valet/observations/{series_id}/{fmt}"
    url = base + "?" + urlencode({"start_date": start_date, "end_date": end_date})
    req = Request(url, headers={"User-Agent": "fx-bronze-lambda/1.0"})
    
    with urlopen(req, timeout=30) as resp:
        body = resp.read()
        status = getattr(resp, "status", 200)
        headers = dict(resp.headers)
    
    return url, status, headers, body


def write_to_s3(bucket: str, series_id: str, fmt: str, url: str, 
                status: int, headers: dict, body: bytes, 
                start_date: str, end_date: str, now: datetime) -> dict:
    """Write raw payload and metadata to S3 Bronze layer."""
    
    # Parse JSON to validate (optional)
    parsed_keys = None
    if fmt == "json":
        parsed = json.loads(body.decode("utf-8"))
        parsed_keys = list(parsed.keys())
    
    # Compute hash and compress
    sha256 = hashlib.sha256(body).hexdigest()
    gz_body = gzip.compress(body)
    
    # Build S3 paths
    ingest_date = now.date().isoformat()
    ingest_ts = now.strftime("%Y%m%dT%H%M%SZ")
    prefix = f"bronze/source=BoC/series={series_id}/ingest_date={ingest_date}/ingest_ts={ingest_ts}"
    
    payload_key = f"{prefix}/observations.{fmt}.gz"
    meta_key = f"{prefix}/_meta.json"
    
    # Build metadata
    meta = {
        "source": "BoC",
        "series_id": series_id,
        "format": fmt,
        "request_url": url,
        "http_status": status,
        "retrieved_at_utc": now.isoformat(),
        "start_date": start_date,
        "end_date": end_date,
        "sha256_raw": sha256,
        "raw_bytes": len(body),
        "gz_bytes": len(gz_body),
        "response_headers_subset": {
            k: headers.get(k) for k in ["Content-Type", "Last-Modified", "Date"]
        },
        "response_keys": parsed_keys,
    }
    
    # Write payload
    s3.put_object(
        Bucket=bucket,
        Key=payload_key,
        Body=gz_body,
        ContentType="application/json" if fmt == "json" else "text/csv",
        ContentEncoding="gzip",
    )
    
    # Write metadata
    s3.put_object(
        Bucket=bucket,
        Key=meta_key,
        Body=json.dumps(meta, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    
    return {"payload_key": payload_key, "meta_key": meta_key, "series_id": series_id}


def lambda_handler(event, context):
    """
    Bronze ingestion Lambda for Bank of Canada FX rates.
    
    Supports both daily incremental runs and one-time backfill via event override.
    
    Daily (default): Pulls last N days (sliding window)
    Backfill: Pass {"backfill": true, "start_date": "2017-01-01"} to load history
    """

    # Configuration from environment
    bucket = os.environ["BUCKET"]
    series_ids = os.getenv("SERIES_IDS").split(",")
    lookback_days = int(os.getenv("LOOKBACK_DAYS", "10"))
    fmt = os.getenv("FORMAT", "json")
    
    # Current timestamp
    now = datetime.now(timezone.utc)
    
    # Determine mode: backfill vs incremental
    if event.get("backfill"):
        # Backfill mode: use provided dates
        start_date = event["start_date"]
        end_date = event.get("end_date", now.date().isoformat())
        print(f"[BACKFILL MODE] {start_date} to {end_date}")
    else:
        # Incremental mode: sliding window
        end_date = now.date().isoformat()
        start_date = (now.date() - timedelta(days=lookback_days)).isoformat()
        print(f"[INCREMENTAL MODE] {start_date} to {end_date}")
    
    # Process each series
    results = []
    errors = []
    
    for series_id in series_ids:
        series_id = series_id.strip()
        print(f"Processing {series_id}...")
        
        try:
            url, status, headers, body = fetch_boc(series_id, start_date, end_date, fmt)
            
            if status != 200:
                errors.append({"series_id": series_id, "error": f"HTTP {status}"})
                print(f"  ✗ {series_id}: HTTP {status}")
                continue
            
            result = write_to_s3(
                bucket, series_id, fmt, url, status, headers, body,
                start_date, end_date, now
            )
            results.append(result)
            print(f"  ✓ {series_id}: {result['payload_key']}")
            
        except Exception as e:
            print(f"  ✗ {series_id}: {str(e)}")
            errors.append({"series_id": series_id, "error": str(e)})
    
    return {
        "ok": len(errors) == 0,
        "ingest_date": now.date().isoformat(),
        "ingest_ts": now.strftime("%Y%m%dT%H%M%SZ"),
        "start_date": start_date,
        "end_date": end_date,
        "results": results,
        "errors": errors,
    }