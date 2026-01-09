"""Deterministic path helpers for pipeline runs."""
from __future__ import annotations

from pathlib import Path


def get_run_dir(*, runs_dir: str, run_date: str) -> str:
    """
    Get run directory path for a given run date.
    
    Args:
        runs_dir: Base runs directory (e.g., "outputs/runs")
        run_date: Run date in YYYY-MM-DD format
        
    Returns:
        Absolute path to run directory (e.g., "outputs/runs/2024-01-15")
    """
    runs_path = Path(runs_dir)
    return str(runs_path / run_date)


def get_run_predictions_path(*, runs_dir: str, run_date: str) -> str:
    """
    Get predictions parquet path for a given run date.
    
    Args:
        runs_dir: Base runs directory (e.g., "outputs/runs")
        run_date: Run date in YYYY-MM-DD format
        
    Returns:
        Absolute path to predictions file (e.g., "outputs/runs/2024-01-15/decision_predictions_h7.parquet")
    """
    run_dir = get_run_dir(runs_dir=runs_dir, run_date=run_date)
    return str(Path(run_dir) / "decision_predictions_h7.parquet")


def get_run_manifest_path(*, runs_dir: str, run_date: str) -> str:
    """
    Get manifest JSON path for a given run date.
    
    Args:
        runs_dir: Base runs directory (e.g., "outputs/runs")
        run_date: Run date in YYYY-MM-DD format
        
    Returns:
        Absolute path to manifest file (e.g., "outputs/runs/2024-01-15/manifest.json")
    """
    run_dir = get_run_dir(runs_dir=runs_dir, run_date=run_date)
    return str(Path(run_dir) / "manifest.json")

