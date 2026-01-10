"""Tests for pipeline path helpers."""
from __future__ import annotations

from pathlib import Path

import pytest

from src.pipeline.paths import (
    get_run_dir,
    get_run_manifest_path,
    get_run_predictions_path,
)


def test_get_run_dir():
    """Test run directory path generation."""
    result = get_run_dir(runs_dir="outputs/runs", run_date="2024-01-15")
    assert result == str(Path("outputs/runs") / "2024-01-15")
    
    # Test with absolute path
    result_abs = get_run_dir(runs_dir="/absolute/path/runs", run_date="2024-01-15")
    assert result_abs == str(Path("/absolute/path/runs") / "2024-01-15")


def test_get_run_predictions_path():
    """Test predictions path generation."""
    result = get_run_predictions_path(runs_dir="outputs/runs", run_date="2024-01-15")
    expected = Path("outputs/runs") / "2024-01-15" / "decision_predictions_h7.parquet"
    assert result == str(expected)


def test_get_run_manifest_path():
    """Test manifest path generation."""
    result = get_run_manifest_path(runs_dir="outputs/runs", run_date="2024-01-15")
    expected = Path("outputs/runs") / "2024-01-15" / "manifest.json"
    assert result == str(expected)


def test_paths_deterministic():
    """Test that paths are deterministic (same inputs -> same outputs)."""
    runs_dir = "outputs/runs"
    run_date = "2024-01-15"
    
    result1 = get_run_dir(runs_dir=runs_dir, run_date=run_date)
    result2 = get_run_dir(runs_dir=runs_dir, run_date=run_date)
    assert result1 == result2
    
    pred1 = get_run_predictions_path(runs_dir=runs_dir, run_date=run_date)
    pred2 = get_run_predictions_path(runs_dir=runs_dir, run_date=run_date)
    assert pred1 == pred2
    
    manifest1 = get_run_manifest_path(runs_dir=runs_dir, run_date=run_date)
    manifest2 = get_run_manifest_path(runs_dir=runs_dir, run_date=run_date)
    assert manifest1 == manifest2

