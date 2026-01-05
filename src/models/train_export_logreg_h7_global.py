"""
Train and export a global (multi-series) logistic regression model for horizon h7.

This script:
- Loads multiple Gold parquet files
- Builds h7 features per series
- Pools rows across series
- Trains a single model with series_id as categorical feature
- Exports model artifacts
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

try:
    import joblib
except ImportError:
    raise RuntimeError("joblib required. Install with: pip install joblib")

try:
    import pandas as pd
    import numpy as np
    from sklearn.compose import ColumnTransformer
    from sklearn.preprocessing import OneHotEncoder, StandardScaler
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
except ImportError:
    raise RuntimeError(
        "scikit-learn/pandas/numpy required. Install with: pip install scikit-learn pandas numpy"
    )

from src.features.h7 import build_features_h7_from_gold, TARGET_COL_H7, NUMERIC_FEATURES_H7


RANDOM_SEED = 7


def infer_series_id_from_path(parquet_path: Path) -> str:
    """
    Infer series_id from parquet file path.
    
    Looks for patterns like:
    - data/gold/FXUSDCAD/data.parquet -> FXUSDCAD
    - data/gold/series=FXUSDCAD/data.parquet -> FXUSDCAD
    
    Args:
        parquet_path: Path to parquet file
        
    Returns:
        Series ID string
    """
    # Try to find series_id in path segments
    parts = parquet_path.parts
    
    # Look for "series=XXX" pattern
    for part in parts:
        if part.startswith("series="):
            return part.split("=", 1)[1]
    
    # Look for directory name that looks like a series ID (e.g., FXUSDCAD)
    for part in parts:
        if part.startswith("FX") and len(part) >= 6:
            return part
    
    # Fallback: use parent directory name
    if parquet_path.parent.name:
        return parquet_path.parent.name
    
    raise ValueError(f"Could not infer series_id from path: {parquet_path}")


def load_gold_parquets(
    gold_root: Path,
    glob_pattern: str = "**/*.parquet",
    max_series: Optional[int] = None,
) -> list[tuple[str, pd.DataFrame]]:
    """
    Load Gold parquet files and infer series_id.
    
    Args:
        gold_root: Root directory containing Gold parquets
        glob_pattern: Glob pattern to find parquet files
        max_series: Maximum number of series to load (None = all)
        
    Returns:
        List of (series_id, dataframe) tuples
    """
    if not gold_root.exists():
        raise FileNotFoundError(f"Gold root directory not found: {gold_root}")
    
    parquet_files = list(gold_root.glob(glob_pattern))
    
    if not parquet_files:
        raise FileNotFoundError(
            f"No parquet files found in {gold_root} matching pattern {glob_pattern}"
        )
    
    print(f"Found {len(parquet_files)} parquet files")
    
    series_data = []
    for parquet_path in sorted(parquet_files)[:max_series] if max_series else sorted(parquet_files):
        try:
            df = pd.read_parquet(parquet_path)
            
            # Infer series_id if not present
            if "series_id" not in df.columns:
                series_id = infer_series_id_from_path(parquet_path)
                df["series_id"] = series_id
                print(f"  Inferred series_id={series_id} from {parquet_path.name}")
            else:
                # Validate single series
                unique_series = df["series_id"].unique()
                if len(unique_series) > 1:
                    print(f"  Warning: {parquet_path.name} contains multiple series: {unique_series}")
                    continue
                series_id = unique_series[0]
            
            series_data.append((series_id, df))
            print(f"  Loaded {series_id}: {len(df)} rows")
            
        except Exception as e:
            print(f"  Error loading {parquet_path}: {e}")
            continue
    
    if not series_data:
        raise ValueError("No series data loaded successfully")
    
    return series_data


def train_global_model(
    df_features: pd.DataFrame,
    target_col: str = TARGET_COL_H7,
    random_state: int = RANDOM_SEED,
) -> Pipeline:
    """
    Train a global logistic regression model with series_id as categorical.
    
    Args:
        df_features: Feature dataframe with obs_date, series_id, features, and target
        target_col: Name of target column
        random_state: Random seed
        
    Returns:
        Fitted sklearn Pipeline
    """
    if target_col not in df_features.columns:
        raise ValueError(f"Target column '{target_col}' not found in features dataframe")
    
    # Separate features and target
    X = df_features.drop(columns=[target_col])
    y = df_features[target_col].astype(int)
    
    # Identify categorical and numeric columns
    categorical_cols = ["series_id"] if "series_id" in X.columns else []
    
    # Numeric columns: all columns except obs_date, series_id, and target
    numeric_cols = [
        c for c in X.columns
        if c not in {"obs_date", "series_id"} and pd.api.types.is_numeric_dtype(X[c])
    ]
    
    if not numeric_cols:
        raise ValueError("No numeric feature columns found")
    
    print(f"\nModel configuration:")
    print(f"  Categorical features: {categorical_cols}")
    print(f"  Numeric features: {len(numeric_cols)} columns")
    print(f"  Total training rows: {len(X)}")
    
    # Build preprocessing pipeline
    transformers = []
    
    if categorical_cols:
        transformers.append(
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), categorical_cols)
        )
    
    if numeric_cols:
        transformers.append(
            ("num", StandardScaler(), numeric_cols)
        )
    
    if not transformers:
        raise ValueError("No features to transform")
    
    preprocessor = ColumnTransformer(transformers, remainder="drop")
    
    # Logistic regression
    clf = LogisticRegression(
        max_iter=2000,
        solver="lbfgs",
        random_state=random_state,
    )
    
    # Full pipeline
    pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("clf", clf),
    ])
    
    print("\nTraining model...")
    pipeline.fit(X, y)
    print("Model training complete")
    
    return pipeline


def export_global_model(
    gold_root: Path,
    out_dir: Path,
    version: str = "h7_global_v1",
    glob_pattern: str = "**/*.parquet",
    max_series: Optional[int] = None,
    dry_run: bool = False,
) -> None:
    """
    Export global logistic regression model.
    
    Args:
        gold_root: Root directory containing Gold parquets
        out_dir: Output directory for artifacts
        version: Model version string
        glob_pattern: Glob pattern to find parquet files
        max_series: Maximum number of series to load
        dry_run: If True, don't write files
    """
    print("=" * 60)
    print("Global Logistic Regression Model Export (h7)")
    print("=" * 60)
    print(f"Gold root: {gold_root}")
    print(f"Output dir: {out_dir}")
    print(f"Version: {version}")
    
    # Load Gold parquets
    print("\nLoading Gold parquets...")
    series_data = load_gold_parquets(gold_root, glob_pattern, max_series)
    
    print(f"\nLoaded {len(series_data)} series")
    
    # Build features for each series
    print("\nBuilding features...")
    feature_dfs = []
    
    for series_id, gold_df in series_data:
        try:
            features_df = build_features_h7_from_gold(gold_df)
            feature_dfs.append(features_df)
            print(f"  {series_id}: {len(features_df)} feature rows")
        except NotImplementedError as e:
            print(f"  {series_id}: Feature building not implemented - {e}")
            raise
        except Exception as e:
            print(f"  Error building features for {series_id}: {e}")
            continue
    
    if not feature_dfs:
        raise ValueError("No feature dataframes created")
    
    # Concatenate all series
    print("\nConcatenating features across series...")
    df_all = pd.concat(feature_dfs, ignore_index=True)
    print(f"Total pooled rows: {len(df_all)}")
    print(f"Series in pool: {df_all['series_id'].unique()}")
    
    # Train model
    model = train_global_model(df_all, target_col=TARGET_COL_H7)
    
    # Identify feature columns for export
    # Exclude obs_date, series_id, target
    feature_cols = [
        c for c in df_all.columns
        if c not in {"obs_date", "series_id", TARGET_COL_H7}
        and pd.api.types.is_numeric_dtype(df_all[c])
    ]
    
    # Add series_id as categorical
    feature_spec = {
        "categorical": ["series_id"],
        "numeric": sorted(feature_cols),
    }
    
    if dry_run:
        print("\n[DRY RUN] Would write files to:", out_dir)
        print(f"  Model: logreg_h7_global.joblib")
        print(f"  Features: features_h7.json")
        print(f"  Metadata: metadata_h7.json")
        return
    
    # Create output directory
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # Save model
    model_path = out_dir / "logreg_h7_global.joblib"
    joblib.dump(model, model_path)
    print(f"\nSaved model to: {model_path}")
    
    # Save feature spec
    features_json_path = out_dir / "features_h7.json"
    with open(features_json_path, "w") as f:
        json.dump(feature_spec, f, indent=2)
    print(f"Saved feature spec to: {features_json_path}")
    
    # Save metadata
    metadata = {
        "version": version,
        "horizon": 7,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "n_rows": len(df_all),
        "n_series": len(df_all["series_id"].unique()),
        "series_ids": sorted(df_all["series_id"].unique().tolist()),
        "n_numeric_features": len(feature_cols),
        "target_col": TARGET_COL_H7,
        "notes": "Global multi-series model with series_id as categorical feature",
    }
    
    metadata_path = out_dir / "metadata_h7.json"
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"Saved metadata to: {metadata_path}")
    
    print("\n" + "=" * 60)
    print("Export complete!")
    print("=" * 60)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train and export global logistic regression model for h7"
    )
    parser.add_argument(
        "--gold-root",
        type=Path,
        required=True,
        help="Root directory containing Gold parquet files (e.g., data/gold)",
    )
    parser.add_argument(
        "--glob-pattern",
        type=str,
        default="**/*.parquet",
        help="Glob pattern to find parquet files (default: **/*.parquet)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("models"),
        help="Output directory for artifacts (default: models)",
    )
    parser.add_argument(
        "--version",
        type=str,
        default="h7_global_v1",
        help="Model version string (default: h7_global_v1)",
    )
    parser.add_argument(
        "--max-series",
        type=int,
        default=None,
        help="Maximum number of series to load (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write files, just verify",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    
    export_global_model(
        gold_root=args.gold_root,
        out_dir=args.out_dir,
        version=args.version,
        glob_pattern=args.glob_pattern,
        max_series=args.max_series,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()

