import os
from datetime import datetime
from io import BytesIO

import boto3
import pandas as pd
import numpy as np

s3 = boto3.client("s3")


def read_silver_data(bucket: str, series_id: str) -> pd.DataFrame:
    """
    Read all Silver data for a series.
    """
    prefix = f"silver/source=BoC/series={series_id}/"
    
    paginator = s3.get_paginator("list_objects_v2")
    dfs = []
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                response = s3.get_object(Bucket=bucket, Key=key)
                df = pd.read_parquet(BytesIO(response["Body"].read()))
                dfs.append(df)
    
    if not dfs:
        return pd.DataFrame()
    
    df = pd.concat(dfs, ignore_index=True)
    df["obs_date"] = pd.to_datetime(df["obs_date"])
    df = df.sort_values("obs_date").reset_index(drop=True)
    
    print(f"Read {len(df)} Silver records for {series_id}")
    return df


def add_return_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add return-based features for forecasting.
    """
    # Basic returns
    df["prev_value"] = df["value"].shift(1)
    df["daily_return"] = (df["value"] - df["prev_value"]) / df["prev_value"]
    df["log_return"] = np.log(df["value"] / df["prev_value"])
    
    # Lagged values (for autoregressive models)
    for lag in [1, 2, 3, 5, 21]:
        df[f"lag_{lag}d"] = df["value"].shift(lag)
    
    # Cumulative returns over periods
    df["return_5d"] = (df["value"] - df["value"].shift(5)) / df["value"].shift(5)
    df["return_21d"] = (df["value"] - df["value"].shift(21)) / df["value"].shift(21)
    
    return df


def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add rolling statistics for trend and volatility.
    """
    # Rolling means (trend indicators)
    df["rolling_mean_5d"] = df["value"].rolling(window=5, min_periods=1).mean()
    df["rolling_mean_21d"] = df["value"].rolling(window=21, min_periods=1).mean()
    
    # Rolling standard deviation (volatility)
    df["rolling_std_5d"] = df["value"].rolling(window=5, min_periods=2).std()
    df["rolling_std_21d"] = df["value"].rolling(window=21, min_periods=5).std()
    
    # Derived signals
    # Volatility ratio: high = recent volatility spike
    df["volatility_ratio"] = df["rolling_std_5d"] / df["rolling_std_21d"]
    
    # MA crossover signal: > 0 means short-term uptrend
    df["ma_crossover"] = (df["rolling_mean_5d"] / df["rolling_mean_21d"]) - 1
    
    # Distance from 21-day MA (mean reversion signal)
    df["distance_from_ma21"] = (df["value"] - df["rolling_mean_21d"]) / df["rolling_mean_21d"]
    
    return df


def add_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add calendar-based features for seasonality.
    """
    df["day_of_week"] = df["obs_date"].dt.dayofweek  # 0=Monday, 4=Friday
    df["day_of_month"] = df["obs_date"].dt.day
    df["week_of_year"] = df["obs_date"].dt.isocalendar().week.astype(int)
    df["month"] = df["obs_date"].dt.month
    df["quarter"] = df["obs_date"].dt.quarter
    df["year"] = df["obs_date"].dt.year
    
    # Special days (often have unusual flows)
    df["is_month_start"] = df["obs_date"].dt.is_month_start
    df["is_month_end"] = df["obs_date"].dt.is_month_end
    df["is_quarter_end"] = df["obs_date"].dt.is_quarter_end
    df["is_year_start"] = df["obs_date"].dt.is_year_start
    df["is_year_end"] = df["obs_date"].dt.is_year_end
    
    return df


def add_target_variables(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add forward-looking targets for supervised learning.
    
    NOTE: These are what you're trying to PREDICT.
    In production, you'd exclude these from features to avoid leakage.
    """
    # Next day's return (1-day ahead prediction target)
    df["target_return_1d"] = df["log_return"].shift(-1)
    
    # Direction (classification target)
    df["target_direction_1d"] = (df["target_return_1d"] > 0).astype(int)
    
    # 5-day ahead return
    df["target_return_5d"] = (df["value"].shift(-5) - df["value"]) / df["value"]
    
    return df


def process_series(bucket: str, series_id: str, run_id: str) -> dict:
    """
    Transform Silver data into Gold with forecasting features.
    """
    # 1. Read Silver data
    df = read_silver_data(bucket, series_id)
    
    if df.empty:
        return {"series_id": series_id, "status": "no_data", "records": 0}
    
    # 2. Ensure sorted by date
    df = df.sort_values("obs_date").reset_index(drop=True)
    
    # 3. Add features
    df = add_return_features(df)
    df = add_rolling_features(df)
    df = add_calendar_features(df)
    df = add_target_variables(df)
    
    # 4. Add metadata
    df["run_id"] = run_id
    df["processed_at"] = datetime.utcnow().isoformat()
    
    # 5. Select and order columns for final output
    output_columns = [
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
        
        # Target variables (for training)
        "target_return_1d",
        "target_direction_1d",
        "target_return_5d",
        
        # Lineage
        "source",
        "run_id",
        "processed_at",
    ]
    
    # Keep only columns that exist (in case Silver schema varies)
    output_columns = [col for col in output_columns if col in df.columns]
    df = df[output_columns]
    
    # 6. Convert obs_date back to date type for partitioning
    df["obs_date"] = pd.to_datetime(df["obs_date"]).dt.date
    
    # 7. Drop rows with NaN in critical feature columns (first ~21 rows will have NaNs)
    #    Keep them but mark - let the ML pipeline decide how to handle
    rows_before = len(df)
    
    print(f"Generated {len(df)} Gold records for {series_id}")
    print(f"  Date range: {df['obs_date'].min()} to {df['obs_date'].max()}")
    print(f"  NaN rows in lag_21d: {df['lag_21d'].isna().sum()} (expected for first ~21 days)")
    
    # 8. Write to Gold (single file per series, like Silver)
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_buffer.seek(0)
    
    key = f"gold/source=BoC/series={series_id}/data.parquet"
    
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=parquet_buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    
    print(f"  ✓ Wrote to s3://{bucket}/{key}")
    
    return {
        "series_id": series_id,
        "status": "success",
        "records": len(df),
        "date_range": {
            "min": str(df["obs_date"].min()),
            "max": str(df["obs_date"].max()),
        },
        "output_key": key,
    }


def lambda_handler(event, context):
    """
    Gold transformation Lambda.
    
    Reads Silver data, adds forecasting features, writes to Gold layer.
    
    Event parameters:
        - run_id (optional): Identifier for this run. Defaults to timestamp.
    """
    bucket = os.environ["BUCKET"]
    series_ids = os.getenv("SERIES_IDS", "FXUSDCAD,FXEURCAD").split(",")
    
    # Generate run_id if not provided
    run_id = event.get("run_id", f"gold_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}")
    
    print(f"Starting Gold processing, run_id={run_id}")
    print(f"Series to process: {series_ids}")
    
    results = []
    
    for series_id in series_ids:
        series_id = series_id.strip()
        print(f"\nProcessing {series_id}...")
        
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
