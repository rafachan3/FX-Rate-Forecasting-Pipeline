"""Deterministic run manifest builder for pipeline artifacts."""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

try:
    import pandas as pd
except ImportError:
    raise RuntimeError("pandas required. Install with: pip install pandas")


def sha256_file(path: str | Path) -> str:
    """
    Compute SHA256 hash of a file.
    
    Args:
        path: Path to file
        
    Returns:
        Hexadecimal SHA256 hash (64 characters)
        
    Raises:
        FileNotFoundError: If file does not exist
    """
    path_obj = Path(path)
    if not path_obj.exists():
        raise FileNotFoundError(f"File not found: {path_obj}")
    
    sha256_hash = hashlib.sha256()
    with open(path_obj, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    
    return sha256_hash.hexdigest()


def file_bytes(path: str | Path) -> int:
    """
    Get file size in bytes.
    
    Args:
        path: Path to file
        
    Returns:
        File size in bytes
        
    Raises:
        FileNotFoundError: If file does not exist
    """
    path_obj = Path(path)
    if not path_obj.exists():
        raise FileNotFoundError(f"File not found: {path_obj}")
    
    return path_obj.stat().st_size


def get_git_sha() -> str:
    """
    Get current git commit SHA.
    
    Returns:
        Git commit SHA (full 40-character hash)
        
    Raises:
        RuntimeError: If git command fails or returns empty
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        sha = result.stdout.strip()
        if not sha:
            raise RuntimeError("git rev-parse HEAD returned empty string")
        return sha
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"git rev-parse HEAD failed: {e.stderr}") from e
    except FileNotFoundError:
        raise RuntimeError("git command not found. Is git installed?")


def read_parquet_obs_date_range_and_rows(path: str | Path) -> dict[str, Any]:
    """
    Read parquet file and extract observation date range and row count.
    
    Args:
        path: Path to parquet file
        
    Returns:
        Dictionary with keys:
        - rows: int (number of rows)
        - min_obs_date: str (ISO date string) or None if empty
        - max_obs_date: str (ISO date string) or None if empty
        
    Raises:
        FileNotFoundError: If file does not exist
        ValueError: If obs_date column is missing
    """
    path_obj = Path(path)
    if not path_obj.exists():
        raise FileNotFoundError(f"File not found: {path_obj}")
    
    df = pd.read_parquet(path_obj)
    
    if "obs_date" not in df.columns:
        raise ValueError(f"Column 'obs_date' not found in {path_obj}")
    
    rows = len(df)
    
    if rows == 0:
        return {
            "rows": 0,
            "min_obs_date": None,
            "max_obs_date": None,
        }
    
    # Convert obs_date to datetime if it's not already
    obs_dates = pd.to_datetime(df["obs_date"])
    min_date = obs_dates.min()
    max_date = obs_dates.max()
    
    return {
        "rows": rows,
        "min_obs_date": min_date.date().isoformat() if pd.notna(min_date) else None,
        "max_obs_date": max_date.date().isoformat() if pd.notna(max_date) else None,
    }


def build_run_manifest(
    *,
    run_date: str,
    run_timestamp: str,
    gold_inputs: list[dict[str, Any]],
    model_artifacts: dict[str, Any],
    predictions_path: str,
) -> dict[str, Any]:
    """
    Build deterministic run manifest with metadata and file hashes.
    
    Args:
        run_date: Run date (ISO date string, e.g., "2024-01-15")
        run_timestamp: Run timestamp (ISO datetime string)
        gold_inputs: List of dicts, each with 'series_id' and 'path' keys
        model_artifacts: Dict with 'dir' and 'files' keys
            - 'dir': str (directory path)
            - 'files': dict mapping filename to path
        predictions_path: Path to predictions parquet file
        
    Returns:
        Dictionary containing complete run manifest (JSON-serializable)
        
    Raises:
        FileNotFoundError: If any required file is missing
        ValueError: If predictions parquet missing required columns
    """
    # Get git SHA
    git_sha = get_git_sha()
    
    # Process gold inputs (sorted by series_id for determinism)
    gold_inputs_sorted = sorted(gold_inputs, key=lambda x: x["series_id"])
    gold_manifest = []
    
    for gold_input in gold_inputs_sorted:
        series_id = gold_input["series_id"]
        gold_path = gold_input["path"]
        
        # Read parquet metadata
        parquet_info = read_parquet_obs_date_range_and_rows(gold_path)
        
        gold_manifest.append({
            "series_id": series_id,
            "path": str(gold_path),
            "sha256": sha256_file(gold_path),
            "bytes": file_bytes(gold_path),
            "rows": parquet_info["rows"],
            "min_obs_date": parquet_info["min_obs_date"],
            "max_obs_date": parquet_info["max_obs_date"],
        })
    
    # Process model artifacts
    artifacts_dir = model_artifacts["dir"]
    artifacts_files = model_artifacts["files"]
    
    model_files_manifest = {}
    for filename, filepath in artifacts_files.items():
        model_files_manifest[filename] = {
            "path": str(filepath),
            "sha256": sha256_file(filepath),
            "bytes": file_bytes(filepath),
        }
    
    # Process predictions
    predictions_info = read_parquet_obs_date_range_and_rows(predictions_path)
    
    # Read predictions to get by_series_rows
    df_pred = pd.read_parquet(predictions_path)
    
    # Validate required columns
    required_cols = {"obs_date", "series_id", "p_up_logreg", "action_logreg"}
    missing_cols = required_cols - set(df_pred.columns)
    if missing_cols:
        raise ValueError(
            f"Predictions parquet missing required columns: {sorted(missing_cols)}"
        )
    
    # Count rows by series_id (sorted for determinism)
    by_series_rows = (
        df_pred.groupby("series_id")
        .size()
        .sort_index()
        .to_dict()
    )
    
    manifest = {
        "run_date": run_date,
        "run_timestamp": run_timestamp,
        "timezone": "America/Toronto",
        "git_sha": git_sha,
        "python_version": sys.version.split()[0],  # e.g., "3.12.5"
        "gold_inputs": gold_manifest,
        "model_artifacts": {
            "dir": str(artifacts_dir),
            "files": model_files_manifest,
        },
        "predictions": {
            "path": str(predictions_path),
            "sha256": sha256_file(predictions_path),
            "bytes": file_bytes(predictions_path),
            "rows": predictions_info["rows"],
            "min_obs_date": predictions_info["min_obs_date"],
            "max_obs_date": predictions_info["max_obs_date"],
            "by_series_rows": by_series_rows,
        },
    }
    
    return manifest

