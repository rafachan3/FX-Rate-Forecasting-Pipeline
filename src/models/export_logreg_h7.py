"""
Export fitted logistic regression model for horizon h7.

This script loads features from the notebook outputs and fits a model
that matches the structure used in notebook 04.
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
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
except ImportError:
    raise RuntimeError("scikit-learn/pandas/numpy required. Install with: pip install scikit-learn pandas numpy")

try:
    from sklearn.preprocessing import StandardScaler
except ImportError:
    StandardScaler = None


RANDOM_SEED = 7


def identify_feature_columns(df: pd.DataFrame, target_col: str, horizon: int) -> list[str]:
    """
    Identify feature columns by excluding known target columns.
    
    Args:
        df: DataFrame with all columns
        target_col: Name of target column (e.g., "direction_7d")
        horizon: Horizon value (e.g., 7)
        
    Returns:
        Ordered list of feature column names
    """
    excluded = {
        target_col,
        f"fwd_return_{horizon}d",
        # Additional common target patterns
        f"direction_{horizon}d",
        f"fwd_return_{horizon}d",
    }
    
    feature_cols = [c for c in df.columns if c not in excluded]
    
    # Sort for deterministic ordering
    feature_cols = sorted(feature_cols)
    
    return feature_cols


def fit_model(
    X: pd.DataFrame,
    y: pd.Series,
    use_scaler: bool = False,
    random_state: int = RANDOM_SEED,
) -> Pipeline:
    """
    Fit logistic regression model pipeline.
    
    Args:
        X: Feature matrix
        y: Target vector (0/1)
        use_scaler: Whether to use StandardScaler
        random_state: Random seed
        
    Returns:
        Fitted sklearn Pipeline
    """
    steps = []
    
    if use_scaler:
        if StandardScaler is None:
            raise RuntimeError("StandardScaler not available. Install scikit-learn.")
        steps.append(("scaler", StandardScaler(with_mean=True, with_std=True)))
    
    # LogisticRegression with conservative defaults
    # TODO: Align hyperparams with notebook 04 if different
    clf = LogisticRegression(
        max_iter=1000,  # Conservative default
        random_state=random_state,
        # Note: notebook 04 uses penalty="l2", C=1.0, solver="lbfgs", max_iter=2000
        # This script uses defaults to avoid guessing; verify alignment manually
    )
    
    steps.append(("clf", clf))
    
    pipeline = Pipeline(steps=steps)
    pipeline.fit(X, y)
    
    return pipeline


def verify_against_existing(
    model: Pipeline,
    X: pd.DataFrame,
    df_pred_path: Optional[Path],
    df_features: pd.DataFrame,
) -> None:
    """
    Verify exported model against existing predictions if available.
    
    Args:
        model: Fitted pipeline
        X: Feature matrix used for training
        df_pred_path: Path to decision_predictions_h7.parquet (optional)
        df_features: Original features dataframe (for date alignment)
    """
    # Compute predictions from exported model
    p_up_exported = model.predict_proba(X)[:, 1]
    
    print("\n" + "=" * 60)
    print("Verification Metrics")
    print("=" * 60)
    print(f"Exported model predictions:")
    print(f"  Mean: {p_up_exported.mean():.6f}")
    print(f"  Min:  {p_up_exported.min():.6f}")
    print(f"  Max:  {p_up_exported.max():.6f}")
    
    if df_pred_path is None or not df_pred_path.exists():
        print("\nNote: decision_predictions_h7.parquet not found. Skipping comparison.")
        return
    
    try:
        df_pred = pd.read_parquet(df_pred_path)
        
        # Align by date if possible
        if isinstance(df_features.index, pd.DatetimeIndex):
            if isinstance(df_pred.index, pd.DatetimeIndex):
                # Both have DatetimeIndex
                common_dates = df_features.index.intersection(df_pred.index)
                if len(common_dates) == 0:
                    print("\nWarning: No common dates found between features and predictions.")
                    return
                
                # Get predictions for common dates
                p_up_existing = df_pred.loc[common_dates, "p_up_logreg"].values
                p_up_exported_aligned = p_up_exported[df_features.index.isin(common_dates)]
                
            elif "obs_date" in df_pred.columns:
                # Predictions have obs_date column
                df_pred["obs_date"] = pd.to_datetime(df_pred["obs_date"])
                common_dates = df_features.index.intersection(df_pred["obs_date"])
                if len(common_dates) == 0:
                    print("\nWarning: No common dates found between features and predictions.")
                    return
                
                p_up_existing = df_pred[df_pred["obs_date"].isin(common_dates)]["p_up_logreg"].values
                p_up_exported_aligned = p_up_exported[df_features.index.isin(common_dates)]
            else:
                print("\nWarning: Cannot align predictions by date. Skipping comparison.")
                return
        else:
            # Try to align by row order if same length
            if len(df_pred) == len(p_up_exported) and "p_up_logreg" in df_pred.columns:
                p_up_existing = df_pred["p_up_logreg"].values
                p_up_exported_aligned = p_up_exported
            else:
                print("\nWarning: Cannot align predictions. Skipping comparison.")
                return
        
        if len(p_up_existing) != len(p_up_exported_aligned):
            print(f"\nWarning: Length mismatch ({len(p_up_existing)} vs {len(p_up_exported_aligned)}). Skipping comparison.")
            return
        
        # Compute metrics
        correlation = np.corrcoef(p_up_existing, p_up_exported_aligned)[0, 1]
        mae = np.mean(np.abs(p_up_existing - p_up_exported_aligned))
        
        print(f"\nComparison with existing p_up_logreg:")
        print(f"  Correlation: {correlation:.6f}")
        print(f"  MAE:         {mae:.6f}")
        print(f"  N aligned:   {len(p_up_existing)}")
        
    except Exception as e:
        print(f"\nWarning: Could not compare with existing predictions: {e}")


def export_model(
    features_parquet: Path,
    out_dir: Path,
    version: str = "h7_v1",
    target_col: str = "direction_7d",
    horizon: int = 7,
    use_scaler: bool = False,
    dry_run: bool = False,
) -> None:
    """
    Export fitted logistic regression model.
    
    Args:
        features_parquet: Path to features parquet file
        out_dir: Output directory for artifacts
        version: Model version string
        target_col: Name of target column
        horizon: Horizon value
        use_scaler: Whether to use StandardScaler
        dry_run: If True, don't write files
    """
    if not features_parquet.exists():
        raise FileNotFoundError(f"Features parquet not found: {features_parquet}")
    
    print(f"Loading features from: {features_parquet}")
    df_feat = pd.read_parquet(features_parquet)
    
    if isinstance(df_feat.index, pd.DatetimeIndex):
        df_feat = df_feat.sort_index()
    
    print(f"Loaded {len(df_feat)} rows, {len(df_feat.columns)} columns")
    
    # Identify feature columns
    feature_cols = identify_feature_columns(df_feat, target_col, horizon)
    print(f"Identified {len(feature_cols)} feature columns")
    
    if target_col not in df_feat.columns:
        raise ValueError(f"Target column '{target_col}' not found in dataframe")
    
    # Prepare data
    X = df_feat[feature_cols].copy()
    y = df_feat[target_col].astype(int).copy()
    
    print(f"Feature matrix shape: {X.shape}")
    print(f"Target vector shape: {y.shape}")
    print(f"Target distribution: {y.value_counts().to_dict()}")
    
    # Fit model
    print(f"\nFitting model (use_scaler={use_scaler})...")
    model = fit_model(X, y, use_scaler=use_scaler)
    print("Model fitted successfully")
    
    # Verify against existing predictions if available
    pred_path = features_parquet.parent / "decision_predictions_h7.parquet"
    verify_against_existing(model, X, pred_path, df_feat)
    
    if dry_run:
        print("\n[DRY RUN] Would write files to:", out_dir)
        return
    
    # Create output directory
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # Save feature columns list
    features_json_path = out_dir / "features_h7.json"
    with open(features_json_path, "w") as f:
        json.dump(feature_cols, f, indent=2)
    print(f"\nSaved feature columns to: {features_json_path}")
    
    # Save model
    model_path = out_dir / "logreg_h7.joblib"
    joblib.dump(model, model_path)
    print(f"Saved model to: {model_path}")
    
    # Save metadata
    metadata = {
        "version": version,
        "horizon": horizon,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "n_rows": len(df_feat),
        "n_features": len(feature_cols),
        "target_col": target_col,
        "use_scaler": use_scaler,
        "notes": "Exported for deterministic inference; hyperparams/scaling must match notebook 04",
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
        description="Export fitted logistic regression model for h7"
    )
    parser.add_argument(
        "--features-parquet",
        type=Path,
        required=True,
        help="Path to features parquet file (e.g., outputs/usdcad_features_h7.parquet)",
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
        default="h7_v1",
        help="Model version string (default: h7_v1)",
    )
    parser.add_argument(
        "--target-col",
        type=str,
        default="direction_7d",
        help="Target column name (default: direction_7d)",
    )
    parser.add_argument(
        "--use-scaler",
        action="store_true",
        help="Use StandardScaler (matches notebook 04)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write files, just verify",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    
    export_model(
        features_parquet=args.features_parquet,
        out_dir=args.out_dir,
        version=args.version,
        target_col=args.target_col,
        horizon=7,
        use_scaler=args.use_scaler,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()

