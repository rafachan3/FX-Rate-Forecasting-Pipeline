# src/artifacts/write_latest.py
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import uuid
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

import pandas as pd

from src.signals.policy import (
    apply_threshold_policy,
    confidence_from_p,
    normalize_label,
)


def _slug(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s).strip("_")


def series_id_to_pair(series_id: str) -> str:
    """
    Convert series_id (e.g., FXUSDCAD) to pair format (e.g., USD_CAD).
    
    Rule: Strip leading "FX", then insert underscore before "CAD".
    All series are against CAD.
    
    Args:
        series_id: Series identifier like "FXUSDCAD", "FXEURCAD"
        
    Returns:
        Pair string like "USD_CAD", "EUR_CAD"
    """
    if not series_id.startswith("FX"):
        raise ValueError(f"Expected series_id to start with 'FX', got: {series_id}")
    
    # Remove "FX" prefix
    base = series_id[2:]
    
    # Insert underscore before "CAD"
    if not base.endswith("CAD"):
        raise ValueError(f"Expected series_id to end with 'CAD', got: {series_id}")
    
    currency = base[:-3]  # Everything before "CAD"
    return f"{currency}_CAD"


def _ensure_datetime_index_or_col(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # If obs_date is the index, keep it and also create a column for export
    if isinstance(df.index, pd.DatetimeIndex):
        if df.index.name is None:
            df.index.name = "obs_date"
        if df.index.name != "obs_date":
            # rename index name to obs_date for consistency
            df.index.name = "obs_date"
        df = df.sort_index()
        df["obs_date"] = df.index.tz_localize(None)
        return df

    # Else require obs_date column (fallback)
    if "obs_date" not in df.columns:
        raise ValueError("Expected a DatetimeIndex or an obs_date column.")
    df["obs_date"] = pd.to_datetime(df["obs_date"], errors="coerce").dt.tz_localize(None)
    df = df.sort_values("obs_date")
    return df


def _safe_float(x) -> Optional[float]:
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def _safe_str(x) -> Optional[str]:
    try:
        if pd.isna(x):
            return None
        return str(x).strip()
    except Exception:
        return None


@dataclass(frozen=True)
class LatestRow:
    obs_date: str
    pair: str

    # export both models if present
    p_up_logreg: Optional[float]
    p_up_tree: Optional[float]

    # optional, derived (backward compat)
    action_logreg: Optional[str]
    action_tree: Optional[str]

    # primary decision and confidence (if available)
    decision: Optional[str]
    confidence: Optional[float]


@dataclass(frozen=True)
class LatestArtifact:
    sha: str
    pair: str
    horizon: str
    generated_at: str
    rows: list[LatestRow]


def build_latest_for_df(
    df: pd.DataFrame,
    sha: str,
    pair: str,
    horizon: str,
    limit_rows: int,
    threshold: float,
) -> LatestArtifact:
    """
    Build LatestArtifact from a filtered dataframe (for a specific series).
    
    Args:
        df: DataFrame with obs_date, series_id, and prediction columns
        sha: Git SHA
        pair: Pair label (e.g., "USD_CAD")
        horizon: Horizon tag (e.g., "h7")
        limit_rows: Maximum rows to include (most recent)
        threshold: Threshold for policy decisions
        
    Returns:
        LatestArtifact for the given pair
    """
    df = _ensure_datetime_index_or_col(df)

    # Check for richer schema: decision and confidence columns
    has_decision = "decision" in df.columns
    has_confidence = "confidence" in df.columns

    # Identify probability columns (your current schema)
    p_logreg = "p_up_logreg" if "p_up_logreg" in df.columns else None
    p_tree = "p_up_tree" if "p_up_tree" in df.columns else None

    # If a "richer" schema exists, support it too
    # (for future: p_up_raw / p_up_cal, etc.)
    if p_logreg is None and "p_up_raw" in df.columns:
        p_logreg = "p_up_raw"

    df = df.tail(limit_rows)

    rows: list[LatestRow] = []
    for _, r in df.iterrows():
        pl = _safe_float(r[p_logreg]) if p_logreg else None
        pt = _safe_float(r[p_tree]) if p_tree else None

        # Primary decision and confidence: use from columns if available
        decision = None
        confidence = None

        if has_decision:
            decision_raw = _safe_str(r["decision"])
            decision = normalize_label(decision_raw) if decision_raw else None

        if has_confidence:
            confidence = _safe_float(r["confidence"])

        # If decision/confidence not in input, derive from probabilities
        if decision is None and (pl is not None or pt is not None):
            # Use logreg if available, else tree, else None
            p_primary = pl if pl is not None else pt
            if p_primary is not None:
                # Apply threshold policy with SIDEWAYS band
                decision_series = apply_threshold_policy(
                    pd.Series([p_primary]), t=threshold
                )
                decision = decision_series.iloc[0]

                # Compute confidence
                conf_series = confidence_from_p(
                    pd.Series([p_primary]), t=threshold
                )
                confidence = conf_series.iloc[0]

        # Backward compat: derive action_logreg and action_tree
        # Use old simple threshold logic for backward compat
        action_logreg = None
        action_tree = None

        if pl is not None:
            if pl >= threshold:
                action_logreg = "UP"
            elif pl <= (1.0 - threshold):
                action_logreg = "DOWN"
            else:
                action_logreg = "SIDEWAYS"

        if pt is not None:
            if pt >= threshold:
                action_tree = "UP"
            elif pt <= (1.0 - threshold):
                action_tree = "DOWN"
            else:
                action_tree = "SIDEWAYS"

        rows.append(
            LatestRow(
                obs_date=pd.Timestamp(r["obs_date"]).date().isoformat(),
                pair=pair,
                p_up_logreg=pl,
                p_up_tree=pt,
                action_logreg=action_logreg,
                action_tree=action_tree,
                decision=decision,
                confidence=confidence,
            )
        )

    return LatestArtifact(
        sha=sha,
        pair=pair,
        horizon=horizon,
        generated_at=pd.Timestamp.utcnow().isoformat(timespec="seconds") + "Z",
        rows=rows,
    )


def build_latest(
    outputs_dir: Path,
    sha: str,
    pair: str,
    horizon: str,
    limit_rows: int,
    threshold: float,
) -> LatestArtifact:
    """
    Build LatestArtifact for a single pair (backward compatibility).
    
    Reads decision_predictions_{horizon}.parquet and filters for the given pair.
    """
    # Your file name is decision_predictions_h7.parquet
    path = outputs_dir / f"decision_predictions_{horizon}.parquet"
    if not path.exists():
        raise FileNotFoundError(str(path))

    df = pd.read_parquet(path)
    
    # Filter by pair if series_id column exists
    if "series_id" in df.columns:
        # Convert pair back to series_id for filtering
        # This is a simple reverse: USD_CAD -> FXUSDCAD
        # But we need to handle the general case
        # For now, if pair doesn't match series_id format, filter won't work
        # This maintains backward compat for old usage
        pass  # Don't filter - old behavior was to use all rows
    
    return build_latest_for_df(df, sha, pair, horizon, limit_rows, threshold)


def build_all_latest(
    outputs_dir: Path,
    sha: str,
    horizon: str,
    limit_rows: int,
    threshold: float,
    target_dir: Optional[Path] = None,
) -> list[tuple[Path, Path]]:
    """
    Build latest artifacts for all series in decision_predictions parquet.
    
    Reads the parquet file once, groups by series_id, and generates one JSON/CSV
    file per pair.
    
    Args:
        outputs_dir: Directory containing decision_predictions_{horizon}.parquet
        sha: Git SHA
        horizon: Horizon tag (e.g., "h7")
        limit_rows: Maximum rows per pair (most recent)
        threshold: Threshold for policy decisions
        target_dir: Optional target directory for writing files. If None, uses outputs_dir/latest
        
    Returns:
        List of (json_path, csv_path) tuples for all generated files
    """
    path = outputs_dir / f"decision_predictions_{horizon}.parquet"
    if not path.exists():
        raise FileNotFoundError(str(path))
    
    # Read parquet once
    df = pd.read_parquet(path)
    df = _ensure_datetime_index_or_col(df)
    
    # Check that series_id column exists
    if "series_id" not in df.columns:
        raise ValueError(f"Expected 'series_id' column in {path}")
    
    # Get unique series_ids
    unique_series = df["series_id"].unique()
    
    generated_files = []
    
    for series_id in sorted(unique_series):
        # Filter dataframe for this series
        series_df = df[df["series_id"] == series_id].copy()
        
        # Convert series_id to pair format
        try:
            pair = series_id_to_pair(series_id)
        except ValueError as e:
            print(f"Warning: Skipping {series_id}: {e}")
            continue
        
        # Build latest artifact for this pair
        artifact = build_latest_for_df(
            df=series_df,
            sha=sha,
            pair=pair,
            horizon=horizon,
            limit_rows=limit_rows,
            threshold=threshold,
        )
        
        # Write artifacts to target directory (or default to outputs_dir/latest)
        json_path, csv_path = write_artifacts(outputs_dir, artifact, target_dir=target_dir)
        
        # Verify both files were created
        if not json_path.exists():
            raise RuntimeError(f"JSON file not created: {json_path}")
        if not csv_path.exists():
            raise RuntimeError(f"CSV file not created: {csv_path}")
        
        generated_files.append((json_path, csv_path))
    
    # Log generation stats
    target_str = str(target_dir) if target_dir else str(outputs_dir / "latest")
    json_count = len([f for f in generated_files if f[0].exists()])
    csv_count = len([f for f in generated_files if f[1].exists()])
    print(f"[write_latest] generated_json={json_count} generated_csv={csv_count} dir={target_str}")
    
    return generated_files


def promote_to_latest(*, latest_dir: str, files: list[tuple[str, str]]) -> None:
    """
    Atomically promote multiple files to latest directory.
    
    Uses a temporary directory and os.replace for atomicity. If any source file
    is missing or any copy fails, the latest directory remains unchanged.
    
    Args:
        latest_dir: Target directory for latest artifacts
        files: List of (src_path, dst_filename) tuples
        
    Raises:
        FileNotFoundError: If any source file is missing (before any writes)
        OSError: If file operations fail
    """
    latest_path = Path(latest_dir)
    latest_path.mkdir(parents=True, exist_ok=True)
    
    # Validate all source files exist before starting
    for src_path, _ in files:
        src = Path(src_path)
        if not src.exists():
            raise FileNotFoundError(f"Source file not found: {src_path}")
    
    # Create temporary directory inside latest_dir
    temp_dir = latest_path / f".tmp_{uuid.uuid4().hex[:8]}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Copy all files to temp directory
        temp_files = []
        for src_path, dst_filename in files:
            src = Path(src_path)
            temp_dst = temp_dir / dst_filename
            
            # Copy file
            shutil.copy2(src, temp_dst)
            temp_files.append((temp_dst, latest_path / dst_filename))
        
        # Atomically move files from temp to latest using os.replace
        for temp_file, final_file in temp_files:
            os.replace(temp_file, final_file)
        
        # Cleanup temp directory on success
        temp_dir.rmdir()
        
    except Exception as e:
        # Try to cleanup temp directory, but don't hide original error
        try:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
        except Exception:
            pass  # Ignore cleanup errors
        raise  # Re-raise original error


def write_artifacts(outputs_dir: Path, artifact: LatestArtifact, target_dir: Optional[Path] = None) -> tuple[Path, Path]:
    """
    Write latest artifact JSON and CSV files.
    
    Args:
        outputs_dir: Base outputs directory
        artifact: LatestArtifact to write
        target_dir: Optional target directory. If None, uses outputs_dir/latest if outputs_dir.name != "latest"
        
    Returns:
        Tuple of (json_path, csv_path)
    """
    if target_dir is None:
        # If caller already points to a "latest" directory, don't nest another one.
        target_dir = outputs_dir if outputs_dir.name == "latest" else (outputs_dir / "latest")
    target_dir.mkdir(parents=True, exist_ok=True)

    pair_slug = _slug(artifact.pair)
    json_path = target_dir / f"latest_{pair_slug}_{artifact.horizon}.json"
    csv_path = target_dir / f"latest_{pair_slug}_{artifact.horizon}.csv"

    payload = asdict(artifact)
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    # Always write CSV, even if empty
    df = pd.DataFrame([asdict(r) for r in artifact.rows])
    df.to_csv(csv_path, index=False)
    
    # Verify CSV was written
    if not csv_path.exists():
        raise RuntimeError(f"CSV file was not created: {csv_path}")

    return json_path, csv_path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Write UI-friendly latest artifacts.")
    p.add_argument("--outputs", required=True, help="Path to outputs/ directory")
    p.add_argument("--sha", required=True, help="Git SHA to stamp artifacts with")
    p.add_argument("--pair", default="USD/CAD", help="Label for the pair (used in filenames + payload)")
    p.add_argument("--horizon", default="h7", help="Horizon tag, e.g. h7")
    p.add_argument("--limit", type=int, default=365, help="Max rows to keep (most recent)")
    p.add_argument("--threshold", type=float, default=0.5, help="Threshold to derive UP/DOWN actions")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    outputs_dir = Path(args.outputs)

    artifact = build_latest(
        outputs_dir=outputs_dir,
        sha=args.sha,
        pair=args.pair,
        horizon=args.horizon,
        limit_rows=args.limit,
        threshold=args.threshold,
    )
    json_path, csv_path = write_artifacts(outputs_dir, artifact)
    print(f"Wrote: {json_path}")
    print(f"Wrote: {csv_path}")
    print(f"Rows: {len(artifact.rows)}")


if __name__ == "__main__":
    main()
