from __future__ import annotations

import numpy as np
import pandas as pd

H = 7
TARGET_COL_H7 = "direction_7d"

NUMERIC_FEATURES_H7: list[str] = [
    "value",
    "ret_1d", "ret_3d", "ret_5d", "ret_10d", "ret_21d",
    "vol_5d", "vol_10d", "vol_21d", "vol_63d",
    "mom_5d", "mom_10d", "mom_21d",
    "zret_1d_21d", "zret_1d_63d",
    "vol_ratio_21_63",
    "vol_21_med_252",
    "is_high_vol",
    "day_of_week", "month", "is_month_end",
]


def _pct_return_from_values(value: pd.Series, prev_value: pd.Series) -> pd.Series:
    return (value / prev_value) - 1.0


def _pct_return(value: pd.Series, n: int) -> pd.Series:
    return (value / value.shift(n)) - 1.0


def _rolling_std(x: pd.Series, w: int) -> pd.Series:
    # Matches pandas default ddof=1 used in your reference parquet
    return x.rolling(w, min_periods=w).std()


def _rolling_mean(x: pd.Series, w: int) -> pd.Series:
    return x.rolling(w, min_periods=w).mean()


def build_features_h7_from_gold(gold_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build h=7 business-day direction features from Gold.

    Notes (must remain stable):
    - We assume Gold is already business-day aligned (BoC series, Mon–Fri).
    - ret_1d is computed from (value/prev_value - 1) unless a precomputed daily_return exists.
    - vol_* is rolling std of ret_1d with ddof=1 (pandas default).
    - mom_* is rolling mean of ret_1d.
    - zret_* is return scaled by vol (NOT z-score demeaned): ret_1d / vol_window.
    - Targets:
        fwd_return_7d = value.shift(-7)/value - 1
        direction_7d = (fwd_return_7d > 0).astype(int)
    """
    required_cols = {"obs_date", "value", "prev_value"}
    missing = required_cols - set(gold_df.columns)
    if missing:
        raise ValueError(f"Gold contract violated. Missing columns: {missing}")

    df = gold_df.copy()
    df["obs_date"] = pd.to_datetime(df["obs_date"])
    df = df.sort_values("obs_date").set_index("obs_date")

    # Series id (if present)
    series_id: str | None
    if "series_id" in df.columns:
        series_ids = df["series_id"].dropna().unique()
        if len(series_ids) != 1:
            raise ValueError(f"Expected exactly one series_id per file, found: {series_ids}")
        series_id = str(series_ids[0])
    else:
        series_id = None

    # Targets (7 business-day horizon)
    df[f"fwd_return_{H}d"] = (df["value"].shift(-H) / df["value"]) - 1.0
    df[f"direction_{H}d"] = (df[f"fwd_return_{H}d"] > 0).astype(int)

    # Drop last H rows where target is undefined
    df = df.iloc[:-H].copy()

    value = df["value"].astype(float)

    # 1) ret_1d
    if "daily_return" in df.columns:
        ret_1d = df["daily_return"].astype(float)
    else:
        ret_1d = _pct_return_from_values(value, df["prev_value"].astype(float))

    # 2) multi-day returns (pct change over n days)
    df["ret_1d"] = ret_1d
    df["ret_3d"] = _pct_return(value, 3)
    df["ret_5d"] = _pct_return(value, 5)
    df["ret_10d"] = _pct_return(value, 10)
    df["ret_21d"] = _pct_return(value, 21)

    # 3) vol windows (std of ret_1d)
    df["vol_5d"] = _rolling_std(ret_1d, 5)
    df["vol_10d"] = _rolling_std(ret_1d, 10)
    df["vol_21d"] = _rolling_std(ret_1d, 21)
    df["vol_63d"] = _rolling_std(ret_1d, 63)

    # 4) momentum windows (mean of ret_1d)
    df["mom_5d"] = _rolling_mean(ret_1d, 5)
    df["mom_10d"] = _rolling_mean(ret_1d, 10)
    df["mom_21d"] = _rolling_mean(ret_1d, 21)

    # 5) zret features (return / rolling vol) — this matches your reference parquet
    df["zret_1d_21d"] = df["ret_1d"] / df["vol_21d"]
    df["zret_1d_63d"] = df["ret_1d"] / df["vol_63d"]

    # 6) vol ratio + regime flag
    df["vol_ratio_21_63"] = df["vol_21d"] / df["vol_63d"]
    df["vol_21_med_252"] = df["vol_21d"].rolling(252, min_periods=252).median()
    df["is_high_vol"] = (df["vol_21d"] > df["vol_21_med_252"]).astype(int)

    # 7) calendar
    idx = df.index
    df["day_of_week"] = idx.dayofweek.astype(int)
    df["month"] = idx.month.astype(int)
    df["is_month_end"] = idx.is_month_end.astype(int)

    # Build final features dataframe
    feat_cols = [c for c in NUMERIC_FEATURES_H7 if c in df.columns]
    feat = df[feat_cols].copy()

    # Attach targets
    feat[f"direction_{H}d"] = df[f"direction_{H}d"].astype(int)
    feat[f"fwd_return_{H}d"] = df[f"fwd_return_{H}d"].astype(float)

    # Carry series_id if available (global trainer expects it)
    if series_id is not None:
        feat["series_id"] = series_id

    # Strict dropna to match notebook-style behavior
    target_cols = [f"direction_{H}d", f"fwd_return_{H}d"]
    feature_cols = [c for c in feat.columns if c not in target_cols]
    feat = feat.dropna(subset=feature_cols + target_cols).copy()

    return feat
