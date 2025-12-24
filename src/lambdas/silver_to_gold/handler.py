import os
import json
from datetime import datetime, timezone
from io import BytesIO

import boto3
import pandas as pd
import numpy as np

s3 = boto3.client("s3")


# =============================================================================
# WATERMARK TRACKING
# =============================================================================

def get_watermark(bucket: str, series_id: str) -> dict:
    """
    Read the Gold watermark file.
    
    Returns:
        {
            "last_obs_date": "2025-01-15",
            "updated_at": "2025-01-15T12:30:00Z"
        }
    """
    key = f"gold/source=BoC/series={series_id}/_watermark.json"
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return {}
    except Exception as e:
        print(f"Warning: Could not read Gold watermark: {e}")
        return {}


def update_watermark(bucket: str, series_id: str, last_obs_date: str):
    """
    Update the Gold watermark file.
    """
    key = f"gold/source=BoC/series={series_id}/_watermark.json"
    
    watermark = {
        "last_obs_date": last_obs_date,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(watermark, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    
    print(f"Updated Gold watermark: {watermark}")


# =============================================================================
# SILVER DATA READING
# =============================================================================

def list_silver_partitions(bucket: str, series_id: str, after_date: str = None) -> list[str]:
    """
    List all Silver partition dates, optionally filtered.
    
    Args:
        bucket: S3 bucket
        series_id: e.g., "FXUSDCAD"
        after_date: If provided, only return partitions with ds > this date
    
    Returns:
        List of date strings (partition keys), sorted
    """
    prefix = f"silver/source=BoC/series={series_id}/"
    
    paginator = s3.get_paginator("list_objects_v2")
    dates = set()
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for common_prefix in page.get("CommonPrefixes", []):
            # Format: silver/source=BoC/series=FXUSDCAD/ds=2025-01-15/
            path = common_prefix["Prefix"]
            for part in path.split("/"):
                if part.startswith("ds="):
                    ds = part.split("=")[1]
                    if after_date is None or ds > after_date:
                        dates.add(ds)
    
    sorted_dates = sorted(dates)
    print(f"Found {len(sorted_dates)} Silver partitions" + 
          (f" (after {after_date})" if after_date else ""))
    
    return sorted_dates


def read_silver_partition(bucket: str, series_id: str, ds: str) -> pd.DataFrame:
    """
    Read a single Silver partition.
    """
    key = f"silver/source=BoC/series={series_id}/ds={ds}/data.parquet"
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(BytesIO(response["Body"].read()))
        df["obs_date"] = pd.to_datetime(df["obs_date"])
        return df
    except Exception as e:
        print(f"Warning: Could not read {key}: {e}")
        return pd.DataFrame()


def read_silver_data(bucket: str, series_id: str, dates: list[str] = None) -> pd.DataFrame:
    """
    Read Silver data, either specific dates or all.
    """
    if dates is None:
        dates = list_silver_partitions(bucket, series_id)
    
    dfs = []
    for ds in dates:
        df = read_silver_partition(bucket, series_id, ds)
        if not df.empty:
            dfs.append(df)
    
    if not dfs:
        return pd.DataFrame()
    
    combined = pd.concat(dfs, ignore_index=True)
    combined = combined.sort_values("obs_date").reset_index(drop=True)
    
    print(f"Read {len(combined)} Silver records for {series_id}")
    return combined


# =============================================================================
# GOLD DATA READING
# =============================================================================

def read_existing_gold(bucket: str, series_id: str) -> pd.DataFrame:
    """
    Read existing Gold data for the series.
    """
    key = f"gold/source=BoC/series={series_id}/data.parquet"
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(BytesIO(response["Body"].read()))
        df["obs_date"] = pd.to_datetime(df["obs_date"])
        print(f"Read {len(df)} existing Gold records")
        return df
    except s3.exceptions.NoSuchKey:
        print("No existing Gold data found")
        return pd.DataFrame()
    except Exception as e:
        print(f"Warning: Could not read existing Gold: {e}")
        return pd.DataFrame()


# =============================================================================
# FEATURE ENGINEERING
# =============================================================================

def add_return_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add return-based features."""
    df = df.copy()
    
    df["prev_value"] = df["value"].shift(1)
    df["daily_return"] = (df["value"] - df["prev_value"]) / df["prev_value"]
    df["log_return"] = np.log(df["value"] / df["prev_value"])
    
    for lag in [1, 2, 3, 5, 21]:
        df[f"lag_{lag}d"] = df["value"].shift(lag)
    
    df["return_5d"] = (df["value"] - df["value"].shift(5)) / df["value"].shift(5)
    df["return_21d"] = (df["value"] - df["value"].shift(21)) / df["value"].shift(21)
    
    return df


def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add rolling statistics."""
    df = df.copy()
    
    df["rolling_mean_5d"] = df["value"].rolling(window=5, min_periods=1).mean()
    df["rolling_mean_21d"] = df["value"].rolling(window=21, min_periods=1).mean()
    df["rolling_std_5d"] = df["value"].rolling(window=5, min_periods=2).std()
    df["rolling_std_21d"] = df["value"].rolling(window=21, min_periods=5).std()
    
    df["volatility_ratio"] = df["rolling_std_5d"] / df["rolling_std_21d"]
    df["ma_crossover"] = (df["rolling_mean_5d"] / df["rolling_mean_21d"]) - 1
    df["distance_from_ma21"] = (df["value"] - df["rolling_mean_21d"]) / df["rolling_mean_21d"]
    
    return df


def add_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add calendar-based features."""
    df = df.copy()
    
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
    """Add forward-looking targets for ML."""
    df = df.copy()
    
    df["target_return_1d"] = df["log_return"].shift(-1)
    df["target_direction_1d"] = (df["target_return_1d"] > 0).astype(int)
    df["target_return_5d"] = (df["value"].shift(-5) - df["value"]) / df["value"]
    
    return df


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all feature engineering steps."""
    df = df.sort_values("obs_date").reset_index(drop=True)
    
    df = add_return_features(df)
    df = add_rolling_features(df)
    df = add_calendar_features(df)
    df = add_target_variables(df)
    
    return df


# =============================================================================
# OUTPUT COLUMN SELECTION
# =============================================================================

OUTPUT_COLUMNS = [
    # Identifiers
    "obs_date",
    "series_id",
    "base_currency",
    "quote_currency",
    
    # Core value
    "value",
    "prev_value",
    
    # Returns
    "daily_return",
    "log_return",
    "return_5d",
    "return_21d",
    
    # Lags
    "lag_1d",
    "lag_2d",
    "lag_3d",
    "lag_5d",
    "lag_21d",
    
    # Rolling stats
    "rolling_mean_5d",
    "rolling_mean_21d",
    "rolling_std_5d",
    "rolling_std_21d",
    
    # Derived signals
    "volatility_ratio",
    "ma_crossover",
    "distance_from_ma21",
    
    # Calendar features
    "day_of_week",
    "day_of_month",
    "week_of_year",
    "month",
    "quarter",
    "year",
    "is_month_start",
    "is_month_end",
    "is_quarter_end",
    "is_year_start",
    "is_year_end",
    
    # Target variables
    "target_return_1d",
    "target_direction_1d",
    "target_return_5d",
    
    # Lineage
    "source",
    "run_id",
    "processed_at",
]


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def process_series_incremental(bucket: str, series_id: str, run_id: str) -> dict:
    """
    Incremental Gold processing.
    
    Strategy:
    1. Read existing Gold data
    2. Find new Silver partitions (after last Gold obs_date)
    3. For feature computation, we need historical context:
       - Read enough Silver history to compute rolling features (21 days minimum)
       - Compute features on combined data
       - Only KEEP the new rows in final output
    4. Append new rows to Gold
    """
    # 1. Get watermark (last processed obs_date in Gold)
    watermark = get_watermark(bucket, series_id)
    last_gold_date = watermark.get("last_obs_date")
    
    if last_gold_date:
        print(f"[INCREMENTAL] Last Gold obs_date: {last_gold_date}")
    else:
        print(f"[INITIAL LOAD] No Gold data exists, will process all Silver data")
    
    # 2. List new Silver partitions
    new_partitions = list_silver_partitions(bucket, series_id, after_date=last_gold_date)
    
    if not new_partitions:
        return {
            "series_id": series_id,
            "status": "no_new_data",
            "records_added": 0,
            "message": "No new Silver partitions to process"
        }
    
    print(f"Found {len(new_partitions)} new partitions to process: {new_partitions[0]} to {new_partitions[-1]}")
    
    # 3. Read existing Gold data
    existing_gold = read_existing_gold(bucket, series_id)
    
    # 4. Read Silver data for new partitions
    new_silver = read_silver_data(bucket, series_id, dates=new_partitions)
    
    if new_silver.empty:
        return {
            "series_id": series_id,
            "status": "no_new_records",
            "records_added": 0,
            "message": "New Silver partitions contained no data"
        }
    
    # 5. For feature computation, we need historical context
    #    Rolling features need up to 21 days of history
    #    Read additional Silver history if we have existing Gold
    
    if not existing_gold.empty:
        # Get the minimum date in new data
        min_new_date = new_silver["obs_date"].min()
        
        # Read Silver data for context (21+ days before new data)
        all_silver_dates = list_silver_partitions(bucket, series_id)
        
        # Find dates we need for context (before min_new_date)
        context_dates = [d for d in all_silver_dates if d < str(min_new_date.date())]
        
        # Take last 30 days of context (to be safe for 21-day rolling features)
        context_dates = context_dates[-30:] if len(context_dates) > 30 else context_dates
        
        if context_dates:
            context_silver = read_silver_data(bucket, series_id, dates=context_dates)
            print(f"Read {len(context_silver)} context records for feature computation")
            
            # Combine context + new data for feature computation
            combined_for_features = pd.concat([context_silver, new_silver], ignore_index=True)
            combined_for_features = combined_for_features.drop_duplicates(
                subset=["series_id", "obs_date"], keep="last"
            ).sort_values("obs_date").reset_index(drop=True)
        else:
            combined_for_features = new_silver
    else:
        # No existing Gold, process all Silver
        combined_for_features = new_silver
    
    # 6. Compute features on the combined data
    featured_df = compute_features(combined_for_features)
    
    # 7. Filter to only NEW rows (dates that weren't in Gold before)
    if last_gold_date:
        featured_df = featured_df[featured_df["obs_date"] > pd.to_datetime(last_gold_date)]
    
    if featured_df.empty:
        return {
            "series_id": series_id,
            "status": "no_new_records_after_filter",
            "records_added": 0,
        }
    
    # 8. Add metadata
    featured_df["run_id"] = run_id
    featured_df["processed_at"] = datetime.now(timezone.utc).isoformat()
    
    # 9. Select output columns
    output_cols = [col for col in OUTPUT_COLUMNS if col in featured_df.columns]
    featured_df = featured_df[output_cols]
    
    # Convert obs_date to date for storage
    featured_df["obs_date"] = pd.to_datetime(featured_df["obs_date"]).dt.date
    
    print(f"Computed features for {len(featured_df)} new records")
    
    # 10. Combine with existing Gold and write
    if not existing_gold.empty:
        existing_gold["obs_date"] = pd.to_datetime(existing_gold["obs_date"]).dt.date
        # Select same columns to ensure schema match
        existing_cols = [col for col in output_cols if col in existing_gold.columns]
        existing_gold = existing_gold[existing_cols]
        
        final_df = pd.concat([existing_gold, featured_df], ignore_index=True)
        final_df = final_df.drop_duplicates(subset=["series_id", "obs_date"], keep="last")
        final_df = final_df.sort_values("obs_date").reset_index(drop=True)
    else:
        final_df = featured_df
    
    # 11. Write to Gold
    parquet_buffer = BytesIO()
    final_df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_buffer.seek(0)
    
    key = f"gold/source=BoC/series={series_id}/data.parquet"
    
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=parquet_buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    
    print(f"Wrote {len(final_df)} total records to Gold (added {len(featured_df)} new)")
    
    # 12. Update watermark
    latest_obs_date = str(final_df["obs_date"].max())
    update_watermark(bucket, series_id, latest_obs_date)
    
    return {
        "series_id": series_id,
        "status": "success",
        "records_added": len(featured_df),
        "total_records": len(final_df),
        "date_range": {
            "min": str(final_df["obs_date"].min()),
            "max": str(final_df["obs_date"].max()),
        },
        "output_key": key,
    }


def process_series_full_refresh(bucket: str, series_id: str, run_id: str) -> dict:
    """
    Full refresh: recompute Gold from all Silver data.
    """
    # Read all Silver data
    silver_df = read_silver_data(bucket, series_id)
    
    if silver_df.empty:
        return {
            "series_id": series_id,
            "status": "no_data",
            "records": 0,
        }
    
    # Compute features
    featured_df = compute_features(silver_df)
    
    # Add metadata
    featured_df["run_id"] = run_id
    featured_df["processed_at"] = datetime.now(timezone.utc).isoformat()
    
    # Select output columns
    output_cols = [col for col in OUTPUT_COLUMNS if col in featured_df.columns]
    featured_df = featured_df[output_cols]
    featured_df["obs_date"] = pd.to_datetime(featured_df["obs_date"]).dt.date
    
    # Write to Gold
    parquet_buffer = BytesIO()
    featured_df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_buffer.seek(0)
    
    key = f"gold/source=BoC/series={series_id}/data.parquet"
    
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=parquet_buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    
    # Update watermark
    latest_obs_date = str(featured_df["obs_date"].max())
    update_watermark(bucket, series_id, latest_obs_date)
    
    print(f"[FULL REFRESH] Wrote {len(featured_df)} records to Gold")
    
    return {
        "series_id": series_id,
        "status": "success",
        "mode": "full_refresh",
        "records": len(featured_df),
        "date_range": {
            "min": str(featured_df["obs_date"].min()),
            "max": str(featured_df["obs_date"].max()),
        },
        "output_key": key,
    }


def lambda_handler(event, context):
    """
    Gold transformation Lambda (incremental).
    
    Reads NEW Silver partitions, computes features using historical context,
    and appends to existing Gold data.
    
    Event parameters:
        - run_id (optional): Identifier for this run
        - full_refresh (optional): If true, recompute Gold from all Silver
    """
    bucket = os.environ["BUCKET"]
    series_ids = os.getenv("SERIES_IDS", "FXUSDCAD,FXEURCAD").split(",")
    
    run_id = event.get("run_id", f"gold_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}")
    full_refresh = event.get("full_refresh", False)
    
    print(f"Starting Gold processing, run_id={run_id}, full_refresh={full_refresh}")
    print(f"Series to process: {series_ids}")
    
    results = []
    
    for series_id in series_ids:
        series_id = series_id.strip()
        print(f"\n{'='*60}\nProcessing {series_id}...\n{'='*60}")
        
        try:
            if full_refresh:
                result = process_series_full_refresh(bucket, series_id, run_id)
            else:
                result = process_series_incremental(bucket, series_id, run_id)
            
            results.append(result)
            status = result.get("status", "unknown")
            records = result.get("records_added", result.get("records", 0))
            print(f"✓ {series_id}: {status}, {records} records")
            
        except Exception as e:
            print(f"✗ {series_id}: {str(e)}")
            import traceback
            traceback.print_exc()
            results.append({
                "series_id": series_id,
                "status": "error",
                "error": str(e),
            })
    
    return {
        "ok": all(r.get("status") in ("success", "no_new_data", "no_new_records") for r in results),
        "run_id": run_id,
        "full_refresh": full_refresh,
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "results": results,
    }