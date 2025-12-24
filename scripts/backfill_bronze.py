#!/usr/bin/env python3
"""
Bronze Layer Historical Backfill Script

One-time backfill for Bank of Canada FX rates from 2017-01-03 to present.
Designed to run locally or in an EC2/ECS task - NOT in Lambda.

Usage:
    # Dry run (no writes)
    python scripts/backfill_bronze.py --dry-run

    # Full backfill
    python scripts/backfill_bronze.py --bucket fx-rate-pipeline-dev

    # Single series test
    python scripts/backfill_bronze.py --bucket fx-rate-pipeline-dev --series FXUSDCAD

    # Resume from failures (reads state file)
    python scripts/backfill_bronze.py --bucket fx-rate-pipeline-dev --resume
"""

import os
import sys
import json
import gzip
import hashlib
import argparse
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from dataclasses import dataclass, field, asdict
from typing import Optional

import boto3
from botocore.exceptions import ClientError

# Configuration

# All 23 BoC FX series (CAD as quote currency)
BOC_FX_SERIES = [
    "FXUSDCAD",  # US Dollar
    "FXEURCAD",  # Euro
    "FXGBPCAD",  # British Pound
    "FXJPYCAD",  # Japanese Yen
    "FXAUDCAD",  # Australian Dollar
    "FXCHFCAD",  # Swiss Franc
    "FXCNYCAD",  # Chinese Yuan
    "FXHKDCAD",  # Hong Kong Dollar
    "FXMXNCAD",  # Mexican Peso
    "FXINRCAD",  # Indian Rupee
    "FXNZDCAD",  # New Zealand Dollar
    "FXSARCAD",  # Saudi Riyal
    "FXSGDCAD",  # Singapore Dollar
    "FXZARCAD",  # South African Rand
    "FXKRWCAD",  # South Korean Won
    "FXSEKCAD",  # Swedish Krona
    "FXNOKCAD",  # Norwegian Krone
    "FXTRYCAD",  # Turkish Lira
    "FXBRLCAD",  # Brazilian Real
    "FXRUBCAD",  # Russian Ruble
    "FXIDRCAD",  # Indonesian Rupiah
    "FXTWDCAD",  # Taiwan Dollar
]

DEFAULT_START_DATE = "2017-01-03"
DEFAULT_FORMAT = "json"
STATE_FILE = "tmp/backfill_state.json"

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY_BASE = 2  # Exponential backoff: 2, 4, 8 seconds
REQUEST_DELAY = 1.0   # Delay between successful requests

# Data Classes

@dataclass
class SeriesResult:
    """Result of processing a single series."""
    series_id: str
    status: str  # "success", "failed", "skipped"
    payload_key: Optional[str] = None
    meta_key: Optional[str] = None
    raw_bytes: int = 0
    observations_count: int = 0
    error: Optional[str] = None
    retries: int = 0
    duration_seconds: float = 0.0


@dataclass
class BackfillState:
    """Persistent state for resumable backfills."""
    started_at: str
    bucket: str
    start_date: str
    end_date: str
    completed: list = field(default_factory=list)
    failed: list = field(default_factory=list)
    
    def save(self, path: str):
        # Ensure parent directory exists
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(asdict(self), f, indent=2)
    
    @classmethod
    def load(cls, path: str) -> "BackfillState":
        with open(path) as f:
            data = json.load(f)
        return cls(**data)

# Core Functions (adapted from Lambda handler)

def fetch_boc(series_id: str, start_date: str, end_date: str, fmt: str) -> tuple:
    """
    Fetch observations from Bank of Canada Valet API.
    
    Returns: (url, status, headers, body)
    """
    base = f"https://www.bankofcanada.ca/valet/observations/{series_id}/{fmt}"
    url = base + "?" + urlencode({"start_date": start_date, "end_date": end_date})
    req = Request(url, headers={"User-Agent": "fx-bronze-backfill/1.0"})
    
    with urlopen(req, timeout=60) as resp:  # Longer timeout for historical data
        body = resp.read()
        status = getattr(resp, "status", 200)
        headers = dict(resp.headers)
    
    return url, status, headers, body


def fetch_with_retry(series_id: str, start_date: str, end_date: str, fmt: str) -> tuple:
    """
    Fetch with exponential backoff retry logic.
    
    Returns: (url, status, headers, body, retries)
    """
    last_error = None
    
    for attempt in range(MAX_RETRIES):
        try:
            url, status, headers, body = fetch_boc(series_id, start_date, end_date, fmt)
            return url, status, headers, body, attempt
            
        except HTTPError as e:
            last_error = f"HTTP {e.code}: {e.reason}"
            if e.code in (429, 500, 502, 503, 504):  # Retryable errors
                delay = RETRY_DELAY_BASE ** (attempt + 1)
                print(f"    ⚠ {last_error}, retrying in {delay}s...")
                time.sleep(delay)
            else:
                raise  # Non-retryable HTTP error
                
        except URLError as e:
            last_error = f"URL Error: {e.reason}"
            delay = RETRY_DELAY_BASE ** (attempt + 1)
            print(f"    ⚠ {last_error}, retrying in {delay}s...")
            time.sleep(delay)
            
        except TimeoutError:
            last_error = "Request timed out"
            delay = RETRY_DELAY_BASE ** (attempt + 1)
            print(f"    ⚠ {last_error}, retrying in {delay}s...")
            time.sleep(delay)
    
    raise Exception(f"Failed after {MAX_RETRIES} attempts: {last_error}")


def write_to_s3(
    s3_client,
    bucket: str,
    series_id: str,
    fmt: str,
    url: str,
    status: int,
    headers: dict,
    body: bytes,
    start_date: str,
    end_date: str,
    now: datetime,
) -> dict:
    """Write raw payload and metadata to S3 Bronze layer."""
    
    # Parse JSON to get observation count
    observations_count = 0
    parsed_keys = None
    if fmt == "json":
        parsed = json.loads(body.decode("utf-8"))
        parsed_keys = list(parsed.keys())
        observations_count = len(parsed.get("observations", []))
    
    # Compute hash and compress
    sha256 = hashlib.sha256(body).hexdigest()
    gz_body = gzip.compress(body)
    
    # Build S3 paths - use "backfill" marker in path
    ingest_date = now.date().isoformat()
    ingest_ts = now.strftime("%Y%m%dT%H%M%SZ")
    prefix = f"bronze/source=BoC/series={series_id}/ingest_date={ingest_date}/ingest_ts={ingest_ts}_backfill"
    
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
        "observations_count": observations_count,
        "is_backfill": True,
        "response_headers_subset": {
            k: headers.get(k) for k in ["Content-Type", "Last-Modified", "Date"]
        },
        "response_keys": parsed_keys,
    }
    
    # Write payload
    s3_client.put_object(
        Bucket=bucket,
        Key=payload_key,
        Body=gz_body,
        ContentType="application/json" if fmt == "json" else "text/csv",
        ContentEncoding="gzip",
    )
    
    # Write metadata
    s3_client.put_object(
        Bucket=bucket,
        Key=meta_key,
        Body=json.dumps(meta, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    
    return {
        "payload_key": payload_key,
        "meta_key": meta_key,
        "observations_count": observations_count,
        "raw_bytes": len(body),
    }

# Backfill Orchestration

def process_series(
    s3_client,
    bucket: str,
    series_id: str,
    start_date: str,
    end_date: str,
    fmt: str,
    dry_run: bool = False,
) -> SeriesResult:
    """Process a single series with full error handling."""
    
    start_time = time.time()
    
    try:
        # Fetch from BoC API
        url, status, headers, body, retries = fetch_with_retry(
            series_id, start_date, end_date, fmt
        )
        
        if status != 200:
            return SeriesResult(
                series_id=series_id,
                status="failed",
                error=f"HTTP {status}",
                retries=retries,
                duration_seconds=time.time() - start_time,
            )
        
        # Parse to get observation count for reporting
        parsed = json.loads(body.decode("utf-8"))
        obs_count = len(parsed.get("observations", []))
        
        if dry_run:
            return SeriesResult(
                series_id=series_id,
                status="success",
                raw_bytes=len(body),
                observations_count=obs_count,
                retries=retries,
                duration_seconds=time.time() - start_time,
            )
        
        # Write to S3
        now = datetime.now(timezone.utc)
        result = write_to_s3(
            s3_client=s3_client,
            bucket=bucket,
            series_id=series_id,
            fmt=fmt,
            url=url,
            status=status,
            headers=headers,
            body=body,
            start_date=start_date,
            end_date=end_date,
            now=now,
        )
        
        return SeriesResult(
            series_id=series_id,
            status="success",
            payload_key=result["payload_key"],
            meta_key=result["meta_key"],
            raw_bytes=result["raw_bytes"],
            observations_count=result["observations_count"],
            retries=retries,
            duration_seconds=time.time() - start_time,
        )
        
    except Exception as e:
        return SeriesResult(
            series_id=series_id,
            status="failed",
            error=str(e),
            duration_seconds=time.time() - start_time,
        )


def run_backfill(
    bucket: str,
    series_list: list[str],
    start_date: str,
    end_date: str,
    dry_run: bool = False,
    resume: bool = False,
    state_file: str = STATE_FILE,
) -> dict:
    """
    Run the full backfill process with progress tracking.
    """
    
    # Initialize S3 client
    s3_client = None if dry_run else boto3.client("s3")
    
    # Load or create state
    if resume and Path(state_file).exists():
        state = BackfillState.load(state_file)
        print(f"📂 Resuming from state file: {len(state.completed)} completed, {len(state.failed)} failed")
        # Filter out already completed series
        series_to_process = [s for s in series_list if s not in state.completed]
    else:
        state = BackfillState(
            started_at=datetime.now(timezone.utc).isoformat(),
            bucket=bucket,
            start_date=start_date,
            end_date=end_date,
        )
        series_to_process = series_list
    
    total = len(series_list)
    to_process = len(series_to_process)
    
    print("=" * 60)
    print("FX Bronze Layer Backfill")
    print("=" * 60)
    print(f"  Bucket:      {bucket}")
    print(f"  Date range:  {start_date} → {end_date}")
    print(f"  Series:      {to_process} to process ({total} total)")
    print(f"  Dry run:     {dry_run}")
    print("=" * 60)
    
    if dry_run:
        print("⚠️  DRY RUN MODE - No data will be written to S3\n")
    
    results: list[SeriesResult] = []
    
    for i, series_id in enumerate(series_to_process, 1):
        progress = f"[{i}/{to_process}]"
        print(f"{progress} Processing {series_id}...", end=" ", flush=True)
        
        result = process_series(
            s3_client=s3_client,
            bucket=bucket,
            series_id=series_id,
            start_date=start_date,
            end_date=end_date,
            fmt=DEFAULT_FORMAT,
            dry_run=dry_run,
        )
        results.append(result)
        
        if result.status == "success":
            print(f"✓ {result.observations_count:,} obs, {result.raw_bytes:,} bytes ({result.duration_seconds:.1f}s)")
            state.completed.append(series_id)
            if series_id in state.failed:
                state.failed.remove(series_id)
        else:
            print(f"✗ {result.error}")
            if series_id not in state.failed:
                state.failed.append(series_id)
        
        # Save state after each series (for resume capability)
        if not dry_run:
            state.save(state_file)
        
        # Rate limiting - be nice to BoC API
        if i < to_process:
            time.sleep(REQUEST_DELAY)
    
    # Summary
    success_count = sum(1 for r in results if r.status == "success")
    failed_count = sum(1 for r in results if r.status == "failed")
    total_obs = sum(r.observations_count for r in results)
    total_bytes = sum(r.raw_bytes for r in results)
    total_duration = sum(r.duration_seconds for r in results)
    
    print("\n" + "=" * 60)
    print("BACKFILL COMPLETE")
    print("=" * 60)
    print(f"  ✓ Success:       {success_count}/{to_process}")
    print(f"  ✗ Failed:        {failed_count}/{to_process}")
    print(f"  📊 Observations:  {total_obs:,}")
    print(f"  💾 Data fetched:  {total_bytes / 1024 / 1024:.2f} MB")
    print(f"  ⏱  Duration:      {total_duration:.1f}s")
    
    if failed_count > 0:
        print(f"\n⚠️  Failed series:")
        for r in results:
            if r.status == "failed":
                print(f"    - {r.series_id}: {r.error}")
        print(f"\n💡 Run with --resume to retry failed series")
    
    if not dry_run:
        print(f"\n📂 State saved to: {state_file}")
    
    return {
        "success": success_count,
        "failed": failed_count,
        "total_observations": total_obs,
        "results": [asdict(r) for r in results],
    }

# CLI

def parse_args():
    parser = argparse.ArgumentParser(
        description="Backfill Bronze layer with historical BoC FX data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run to see what would be fetched
  python backfill_bronze.py --dry-run --bucket fx-rate-pipeline-dev

  # Full backfill
  python backfill_bronze.py --bucket fx-rate-pipeline-dev

  # Single series for testing
  python backfill_bronze.py --bucket fx-rate-pipeline-dev --series FXUSDCAD

  # Custom date range
  python backfill_bronze.py --bucket fx-rate-pipeline-dev --start-date 2020-01-01

  # Resume after failures
  python backfill_bronze.py --bucket fx-rate-pipeline-dev --resume
        """,
    )
    
    parser.add_argument(
        "--bucket",
        required=True,
        help="S3 bucket name for Bronze layer",
    )
    
    parser.add_argument(
        "--series",
        nargs="+",
        default=BOC_FX_SERIES,
        help="Series IDs to backfill (default: all 23 BoC FX series)",
    )
    
    parser.add_argument(
        "--start-date",
        default=DEFAULT_START_DATE,
        help=f"Start date for backfill (default: {DEFAULT_START_DATE})",
    )
    
    parser.add_argument(
        "--end-date",
        default=datetime.now(timezone.utc).date().isoformat(),
        help="End date for backfill (default: today)",
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but don't write to S3",
    )
    
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from previous run (skip completed series)",
    )
    
    parser.add_argument(
        "--state-file",
        default=STATE_FILE,
        help=f"Path to state file for resume capability (default: {STATE_FILE})",
    )
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Validate bucket access (unless dry run)
    if not args.dry_run:
        try:
            s3 = boto3.client("s3")
            s3.head_bucket(Bucket=args.bucket)
            print(f"✓ Bucket '{args.bucket}' accessible\n")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            print(f"✗ Cannot access bucket '{args.bucket}': {error_code}")
            print("  Check your AWS credentials and bucket permissions.")
            sys.exit(1)
    
    # Run backfill
    result = run_backfill(
        bucket=args.bucket,
        series_list=args.series,
        start_date=args.start_date,
        end_date=args.end_date,
        dry_run=args.dry_run,
        resume=args.resume,
        state_file=args.state_file,
    )
    
    # Exit code based on success
    sys.exit(0 if result["failed"] == 0 else 1)


if __name__ == "__main__":
    main()