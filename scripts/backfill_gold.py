
#!/usr/bin/env python3
"""
Gold Layer Backfill Script

Runs the Silver → Gold transformation locally without Lambda timeout constraints.
Use this for initial backfills or reprocessing large amounts of data.

Usage:
    # Process all series
    python scripts/backfill_gold.py --bucket fx-rate-pipeline-dev

    # Process specific series only
    python scripts/backfill_gold.py --bucket fx-rate-pipeline-dev --series FXUSDCAD FXEURCAD

    # Dry run (show what would be processed)
    python scripts/backfill_gold.py --bucket fx-rate-pipeline-dev --dry-run
"""

import argparse
import sys
import time
from datetime import datetime, timezone
from io import BytesIO

import boto3
import numpy as np
import pandas as pd

# Configuration

BOC_FX_SERIES = [
    "FXUSDCAD", "FXEURCAD", "FXGBPCAD", "FXJPYCAD", "FXAUDCAD",
    "FXCHFCAD", "FXCNYCAD", "FXHKDCAD", "FXMXNCAD", "FXINRCAD",
    "FXNZDCAD", "FXSARCAD", "FXSGDCAD", "FXZARCAD", "FXKRWCAD",
    "FXSEKCAD", "FXNOKCAD", "FXTRYCAD", "FXBRLCAD", "FXRUBCAD",
    "FXIDRCAD", "FXTWDCAD",
]

# Core Functions (from Lambda handler)

def read_silver_data(s3_client, bucket: str, series_id: str) -> pd.DataFrame:
    """Read all Silver data for a series."""
    prefix = f"silver/source=BoC/series={series_id}/"
    
    paginator = s3_client.get_paginator("list_objects_v2")
    dfs = []
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                response = s3_client.get_object(Bucket=bucket, Key=key)
                df = pd.read_parquet(BytesIO(response["Body"].read()))
                dfs.append(df)
    
    if not dfs:
        return pd.DataFrame()
    
    df = pd.concat(dfs, ignore_index=True)
    df["obs_date"] = pd.to_datetime(df["obs_date"])
    df = df.sort_values("obs_date").reset_index(drop=True)
    
    return df


def add_return_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add return-based features for forecasting."""
    df["prev_value"] = df["value"].shift(1)
    df["daily_return"] = (df["value"] - df["prev_value"]) / df["prev_value"]
    df["log_return"] = np.log(df["value"] / df["prev_value"])
    
    for lag in [1, 2, 3, 5, 21]:
        df[f"lag_{lag}d"] = df["value"].shift(lag)
    
    df["return_5d"] = (df["value"] - df["value"].shift(5)) / df["value"].shift(5)
    df["return_21d"] = (df["value"] - df["value"].shift(21)) / df["value"].shift(21)
    
    return df


def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add rolling statistics for trend and volatility."""
    df["rolling_mean_5d"] = df["value"].rolling(window=5, min_periods=1).mean()
    df["rolling_mean_21d"] = df["value"].rolling(window=21, min_periods=1).mean()
    
    df["rolling_std_5d"] = df["value"].rolling(window=5, min_periods=2).std()
    df["rolling_std_21d"] = df["value"].rolling(window=21, min_periods=5).std()
    
    df["volatility_ratio"] = df["rolling_std_5d"] / df["rolling_std_21d"]
    df["ma_crossover"] = (df["rolling_mean_5d"] / df["rolling_mean_21d"]) - 1
    df["distance_from_ma21"] = (df["value"] - df["rolling_mean_21d"]) / df["rolling_mean_21d"]
    
    return df


def add_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add calendar-based features for seasonality."""
    df["day_of_week"] = df["obs_date"].dt.dayofweek
    df["day_of_month"] = df["obs_date"].dt.day
    df["week_of_year"] = df["obs_date"].dt.isocalendar().week.astype(int)
    df["month"] = df["obs_date"].dt.month
    df["quarter"] = df["obs_date"].dt.quarter
    df["year"] = df["obs_date"].dt.year
    
    df["is_month_start"] = df["obs_date"].dt.is_month_start
    df["is_month_end"] = df["obs_date"].dt.is_month_end
    df["is_quarter_end"] = df["obs_date"].dt.is_quarter_end
    df["is_year_start"] = df["obs_date"].dt.is_year_start
    df["is_year_end"] = df["obs_date"].dt.is_year_end
    
    return df


def add_target_variables(df: pd.DataFrame) -> pd.DataFrame:
    """Add forward-looking targets for supervised learning."""
    df["target_return_1d"] = df["log_return"].shift(-1)
    df["target_direction_1d"] = (df["target_return_1d"] > 0).astype(int)
    df["target_return_5d"] = (df["value"].shift(-5) - df["value"]) / df["value"]
    
    return df


def process_series(
    s3_client,
    bucket: str,
    series_id: str,
    run_id: str,
    dry_run: bool = False,
) -> dict:
    """Transform Silver data into Gold with forecasting features."""
    
    start_time = time.time()
    
    # 1. Read Silver data
    df = read_silver_data(s3_client, bucket, series_id)
    
    if df.empty:
        return {
            "series_id": series_id,
            "status": "no_data",
            "records": 0,
            "duration_seconds": time.time() - start_time,
        }
    
    print(f"    Read {len(df)} Silver records")
    
    # 2. Ensure sorted by date
    df = df.sort_values("obs_date").reset_index(drop=True)
    
    # 3. Add features
    df = add_return_features(df)
    df = add_rolling_features(df)
    df = add_calendar_features(df)
    df = add_target_variables(df)
    
    # 4. Add metadata
    df["run_id"] = run_id
    df["processed_at"] = datetime.now(timezone.utc).isoformat()
    
    # 5. Select and order columns
    output_columns = [
        "obs_date", "series_id", "base_currency", "quote_currency",
        "value", "prev_value",
        "daily_return", "log_return", "return_5d", "return_21d",
        "lag_1d", "lag_2d", "lag_3d", "lag_5d", "lag_21d",
        "rolling_mean_5d", "rolling_mean_21d", "rolling_std_5d", "rolling_std_21d",
        "volatility_ratio", "ma_crossover", "distance_from_ma21",
        "day_of_week", "day_of_month", "week_of_year", "month", "quarter", "year",
        "is_month_start", "is_month_end", "is_quarter_end", "is_year_start", "is_year_end",
        "target_return_1d", "target_direction_1d", "target_return_5d",
        "source", "run_id", "processed_at",
    ]
    
    output_columns = [col for col in output_columns if col in df.columns]
    df = df[output_columns]
    
    # 6. Convert obs_date back to date type
    df["obs_date"] = pd.to_datetime(df["obs_date"]).dt.date
    
    print(f"    Generated {len(df)} Gold records with {len(df.columns)} features")
    print(f"    Date range: {df['obs_date'].min()} to {df['obs_date'].max()}")
    
    if dry_run:
        return {
            "series_id": series_id,
            "status": "dry_run",
            "records": len(df),
            "columns": len(df.columns),
            "date_range": {
                "min": str(df["obs_date"].min()),
                "max": str(df["obs_date"].max()),
            },
            "duration_seconds": time.time() - start_time,
        }
    
    # 7. Write to Gold
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_buffer.seek(0)
    
    key = f"gold/source=BoC/series={series_id}/data.parquet"
    
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=parquet_buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    
    return {
        "series_id": series_id,
        "status": "success",
        "records": len(df),
        "columns": len(df.columns),
        "date_range": {
            "min": str(df["obs_date"].min()),
            "max": str(df["obs_date"].max()),
        },
        "output_key": key,
        "duration_seconds": time.time() - start_time,
    }

# Main

def run_gold_backfill(
    bucket: str,
    series_list: list[str],
    dry_run: bool = False,
) -> dict:
    """Run the Gold backfill for all specified series."""
    
    s3_client = boto3.client("s3")
    run_id = f"gold_backfill_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    
    print("=" * 60)
    print("Gold Layer Backfill")
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
    print("GOLD BACKFILL COMPLETE")
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
        description="Backfill Gold layer from Silver data",
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
    
    result = run_gold_backfill(
        bucket=args.bucket,
        series_list=args.series,
        dry_run=args.dry_run,
    )
    
    sys.exit(0 if result["ok"] else 1)


if __name__ == "__main__":
    main()