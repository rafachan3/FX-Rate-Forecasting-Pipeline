"""
Deterministic inference runner for horizon h7.

Given Gold parquet(s) and exported model artifacts, produces decision_predictions_h7.parquet.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional

try:
    import joblib
except ImportError:
    raise RuntimeError("joblib required. Install with: pip install joblib")

try:
    import pandas as pd
    import numpy as np
except ImportError:
    raise RuntimeError("pandas/numpy required. Install with: pip install pandas numpy")

from src.features.h7 import build_features_h7_from_gold, TARGET_COL_H7
from src.models.train_export_logreg_h7_global import load_gold_parquets, infer_series_id_from_path
from src.signals.policy import apply_threshold_policy


def _find_file(model_dir: Path, candidates: list[str], file_type: str) -> Path:
    """
    Find a file by trying multiple candidate names.
    
    Args:
        model_dir: Directory to search
        candidates: List of candidate filenames to try in order
        file_type: Type of file (for error messages)
        
    Returns:
        Path to found file
        
    Raises:
        FileNotFoundError: If no candidate found or multiple ambiguous matches
    """
    found = None
    
    # Try candidates in order
    for candidate in candidates:
        path = model_dir / candidate
        if path.exists():
            found = path
            break
    
    # If no candidate found, try glob pattern as fallback
    if found is None:
        # For joblib files, try finding any *.joblib
        if candidates[-1].endswith(".joblib"):
            joblib_files = list(model_dir.glob("*.joblib"))
            if len(joblib_files) == 1:
                found = joblib_files[0]
            elif len(joblib_files) > 1:
                raise FileNotFoundError(
                    f"Multiple {file_type} files found in {model_dir}: {[f.name for f in joblib_files]}. "
                    f"Please specify explicitly or use --model-path."
                )
        # For JSON files, try finding files matching pattern (features_*.json or metadata_*.json)
        elif candidates[-1].endswith(".json"):
            # Extract base pattern (e.g., "features_" or "metadata_")
            base_pattern = candidates[-1].split("_")[0] + "_*.json"
            json_files = list(model_dir.glob(base_pattern))
            if len(json_files) == 1:
                found = json_files[0]
            elif len(json_files) > 1:
                raise FileNotFoundError(
                    f"Multiple {file_type} files found in {model_dir}: {[f.name for f in json_files]}. "
                    f"Please specify explicitly."
                )
    
    if found is None:
        raise FileNotFoundError(
            f"{file_type.capitalize()} file not found in {model_dir}. "
            f"Tried: {', '.join(candidates)}"
        )
    
    return found


def load_model_artifacts(model_dir: Path, model_path: Optional[Path] = None) -> tuple:
    """
    Load model artifacts from directory.
    
    Args:
        model_dir: Directory containing model artifacts
        model_path: Optional explicit path to model file (if provided, uses this directly)
        
    Returns:
        Tuple of (model, feature_spec, metadata)
    """
    # Find model file
    if model_path is not None:
        if not model_path.exists():
            raise FileNotFoundError(f"Model file not found: {model_path}")
        model_file = model_path
    else:
        model_candidates = [
            "logreg_h7_global.joblib",
            "logreg_h7.joblib",
            "model.joblib",
        ]
        model_file = _find_file(model_dir, model_candidates, "model")
    
    # Find features spec
    features_candidates = [
        "features_h7.json",
        "features_h7_global.json",
    ]
    features_file = _find_file(model_dir, features_candidates, "features")
    
    # Find metadata
    metadata_candidates = [
        "metadata_h7.json",
        "metadata_h7_global.json",
    ]
    metadata_file = _find_file(model_dir, metadata_candidates, "metadata")
    
    # Load model
    model = joblib.load(model_file)
    
    # Load feature spec
    with open(features_file) as f:
        feature_data = json.load(f)
    
    # Normalize feature spec format
    # Handle both formats: list (from export_logreg_h7) or dict (from train_export_logreg_h7_global)
    if isinstance(feature_data, list):
        # Old format: list of feature names (all numeric, no categorical)
        feature_spec = {
            "categorical": [],
            "numeric": feature_data,
        }
    elif isinstance(feature_data, dict):
        # New format: dict with 'categorical' and 'numeric' keys
        feature_spec = feature_data
    else:
        raise ValueError(f"Unexpected feature spec format: {type(feature_data)}")
    
    # Load metadata
    with open(metadata_file) as f:
        metadata = json.load(f)
    
    return model, feature_spec, metadata


def prepare_features_for_inference(
    df_features: pd.DataFrame,
    feature_spec: dict,
) -> pd.DataFrame:
    """
    Prepare feature dataframe for model inference.
    
    Ensures required columns are present and in correct order.
    
    Args:
        df_features: Feature dataframe from build_features_h7_from_gold
        feature_spec: Feature specification dict with 'categorical' and 'numeric' keys
        
    Returns:
        Prepared feature dataframe
    """
    # Required columns: series_id (categorical) + numeric features
    required_cat = feature_spec.get("categorical", [])
    required_num = feature_spec.get("numeric", [])
    
    missing_cat = [c for c in required_cat if c not in df_features.columns]
    missing_num = [c for c in required_num if c not in df_features.columns]
    
    if missing_cat:
        raise ValueError(
            f"Missing categorical features: {missing_cat}. "
            f"Available columns: {list(df_features.columns)}"
        )
    if missing_num:
        raise ValueError(
            f"Missing numeric features: {missing_num}. "
            f"Available columns: {list(df_features.columns)}"
        )
    
    # Select features in the order expected by the model
    feature_cols = required_cat + required_num
    df_prepared = df_features[feature_cols].copy()
    
    return df_prepared


def run_inference(
    gold_root: Path,
    model_dir: Path,
    out_path: Path,
    threshold: float = 0.6,
    glob_pattern: str = "**/*.parquet",
    dry_run: bool = False,
    model_path: Optional[Path] = None,
) -> None:
    """
    Run inference on Gold parquet files and produce decision predictions.
    
    Args:
        gold_root: Root directory containing Gold parquet files
        model_dir: Directory containing model artifacts
        out_path: Output path for decision_predictions_h7.parquet
        threshold: Threshold for policy (default: 0.6)
        glob_pattern: Glob pattern to find parquet files
        dry_run: If True, don't write output file
    """
    print("=" * 60)
    print("Inference Runner (h7)")
    print("=" * 60)
    print(f"Gold root: {gold_root}")
    print(f"Model dir: {model_dir}")
    print(f"Output: {out_path}")
    print(f"Threshold: {threshold}")
    
    # Load model artifacts
    print("\nLoading model artifacts...")
    if model_path:
        print(f"  Using explicit model path: {model_path}")
    model, feature_spec, metadata = load_model_artifacts(model_dir, model_path=model_path)
    print(f"  Model version: {metadata.get('version', 'unknown')}")
    print(f"  Horizon: {metadata.get('horizon', 'unknown')}")
    print(f"  Categorical features: {feature_spec.get('categorical', [])}")
    print(f"  Numeric features: {len(feature_spec.get('numeric', []))}")
    
    # Load Gold parquets
    print("\nLoading Gold parquets...")
    series_data = load_gold_parquets(gold_root, glob_pattern)
    print(f"Loaded {len(series_data)} series")
    
    # Build features and run inference for each series
    print("\nBuilding features and running inference...")
    all_predictions = []
    
    for series_id, gold_df in series_data:
        try:
            # Build features
            df_features = build_features_h7_from_gold(gold_df)
            
            # df_features has obs_date as DatetimeIndex from build_features_h7_from_gold
            # Extract obs_date from index for output
            if isinstance(df_features.index, pd.DatetimeIndex):
                obs_dates = df_features.index
            else:
                raise ValueError("Expected DatetimeIndex from build_features_h7_from_gold")
            
            # Ensure series_id is set
            if "series_id" not in df_features.columns:
                df_features["series_id"] = series_id
            
            # Prepare features for model (exclude target columns)
            # Remove target columns if present, but keep all other columns
            target_cols = [c for c in df_features.columns if c.startswith("direction_") or c.startswith("fwd_return_")]
            df_for_model = df_features.drop(columns=target_cols, errors="ignore")
            
            # Prepare features - this will validate and select only required features
            df_prepared = prepare_features_for_inference(df_for_model, feature_spec)
            
            # Ensure we have the same number of rows as features dataframe
            if len(df_prepared) != len(df_features):
                raise ValueError(
                    f"Feature preparation changed row count: {len(df_features)} -> {len(df_prepared)}"
                )
            
            # Run inference
            p_up = model.predict_proba(df_prepared)[:, 1]
            
            # Apply policy to get decisions
            decisions = apply_threshold_policy(pd.Series(p_up), t=threshold)
            
            # Ensure we have the same number of predictions as dates
            if len(p_up) != len(obs_dates):
                raise ValueError(
                    f"Length mismatch: {len(p_up)} predictions vs {len(obs_dates)} dates"
                )
            
            # Build predictions dataframe
            pred_df = pd.DataFrame({
                "obs_date": obs_dates.values,
                "series_id": df_features["series_id"].values,
                "p_up_logreg": p_up,
                "action_logreg": decisions.values,
            })
            
            all_predictions.append(pred_df)
            print(f"  {series_id}: {len(pred_df)} predictions")
            
        except Exception as e:
            print(f"  Error processing {series_id}: {e}")
            raise
    
    if not all_predictions:
        raise ValueError("No predictions generated")
    
    # Concatenate all predictions
    print("\nConcatenating predictions...")
    df_all = pd.concat(all_predictions, ignore_index=False)
    
    # Ensure obs_date is datetime and sort
    if "obs_date" in df_all.columns:
        df_all["obs_date"] = pd.to_datetime(df_all["obs_date"])
        df_all = df_all.sort_values("obs_date")
    elif isinstance(df_all.index, pd.DatetimeIndex):
        df_all = df_all.sort_index()
        # Add obs_date column from index
        df_all["obs_date"] = df_all.index
        df_all = df_all.reset_index(drop=True)
    
    # Enforce output contract: exact columns required
    REQUIRED_COLS = ["obs_date", "series_id", "p_up_logreg", "action_logreg"]
    
    # Check for missing required columns
    missing_cols = set(REQUIRED_COLS) - set(df_all.columns)
    if missing_cols:
        raise ValueError(
            f"Output contract violation: missing required columns: {sorted(missing_cols)}. "
            f"Available columns: {sorted(df_all.columns)}"
        )
    
    # Check for extra columns (fail-loud, do not silently drop)
    extra_cols = set(df_all.columns) - set(REQUIRED_COLS)
    if extra_cols:
        raise ValueError(
            f"Output contract violation: extra columns found: {sorted(extra_cols)}. "
            f"Required columns only: {REQUIRED_COLS}"
        )
    
    # Select columns in exact required order
    df_output = df_all[REQUIRED_COLS].copy()
    
    # Ensure series_id is present (should already be, but fail-loud if not)
    if df_output["series_id"].isna().any():
        raise ValueError("Output contract violation: series_id contains NaN values")
    
    # Final sort by obs_date, then series_id using stable sort for determinism
    df_output = df_output.sort_values(
        ["obs_date", "series_id"], kind="mergesort"
    ).reset_index(drop=True)
    
    print(f"\nTotal predictions: {len(df_output)}")
    print(f"Date range: {df_output['obs_date'].min()} to {df_output['obs_date'].max()}")
    print(f"Series: {sorted(df_output['series_id'].unique())}")
    
    if dry_run:
        print("\n[DRY RUN] Would write to:", out_path)
        print(f"  Shape: {df_output.shape}")
        print(f"  Columns: {list(df_output.columns)}")
        return
    
    # Write output
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_output.to_parquet(out_path, index=False)
    print(f"\nWrote predictions to: {out_path}")
    print("=" * 60)
    print("Inference complete!")
    print("=" * 60)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run inference for horizon h7"
    )
    parser.add_argument(
        "--gold-root",
        type=Path,
        required=True,
        help="Root directory containing Gold parquet files (e.g., data/gold)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("outputs/decision_predictions_h7.parquet"),
        help="Output path for decision_predictions_h7.parquet (default: outputs/decision_predictions_h7.parquet)",
    )
    parser.add_argument(
        "--model-dir",
        type=Path,
        default=Path("models"),
        help="Directory containing model artifacts (default: models)",
    )
    parser.add_argument(
        "--model-path",
        type=Path,
        default=None,
        help="Explicit path to model .joblib file (optional, will auto-detect if not provided)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.6,
        help="Threshold for policy (default: 0.6)",
    )
    parser.add_argument(
        "--glob-pattern",
        type=str,
        default="**/*.parquet",
        help="Glob pattern to find parquet files (default: **/*.parquet)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write output file, just verify",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    
    run_inference(
        gold_root=args.gold_root,
        model_dir=args.model_dir,
        out_path=args.out,
        threshold=args.threshold,
        glob_pattern=args.glob_pattern,
        dry_run=args.dry_run,
        model_path=args.model_path,
    )


if __name__ == "__main__":
    main()

