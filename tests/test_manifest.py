"""Tests for run manifest builder."""
from __future__ import annotations

import tempfile
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytest

from src.artifacts.manifest import (
    build_run_manifest,
    file_bytes,
    get_git_sha,
    read_parquet_obs_date_range_and_rows,
    sha256_file,
)


def test_sha256_file(tmp_path: Path):
    """Test SHA256 file hashing."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("hello world")
    
    hash1 = sha256_file(test_file)
    assert len(hash1) == 64
    assert all(c in "0123456789abcdef" for c in hash1)
    
    # Same content should produce same hash
    hash2 = sha256_file(test_file)
    assert hash1 == hash2
    
    # Different content should produce different hash
    test_file.write_text("different content")
    hash3 = sha256_file(test_file)
    assert hash3 != hash1


def test_sha256_file_not_found(tmp_path: Path):
    """Test that sha256_file raises FileNotFoundError for missing file."""
    missing_file = tmp_path / "missing.txt"
    with pytest.raises(FileNotFoundError):
        sha256_file(missing_file)


def test_file_bytes(tmp_path: Path):
    """Test file size calculation."""
    test_file = tmp_path / "test.txt"
    content = "hello world"
    test_file.write_text(content)
    
    size = file_bytes(test_file)
    assert size == len(content.encode("utf-8"))


def test_file_bytes_not_found(tmp_path: Path):
    """Test that file_bytes raises FileNotFoundError for missing file."""
    missing_file = tmp_path / "missing.txt"
    with pytest.raises(FileNotFoundError):
        file_bytes(missing_file)


def test_get_git_sha():
    """Test git SHA retrieval."""
    sha = get_git_sha()
    assert len(sha) == 40
    assert all(c in "0123456789abcdef" for c in sha)


def test_read_parquet_obs_date_range_and_rows(tmp_path: Path):
    """Test reading parquet obs_date range and rows."""
    # Create test parquet
    df = pd.DataFrame({
        "obs_date": pd.date_range("2024-01-01", periods=5, freq="D"),
        "value": [1.0, 1.1, 1.2, 1.3, 1.4],
    })
    parquet_path = tmp_path / "test.parquet"
    df.to_parquet(parquet_path, index=False)
    
    result = read_parquet_obs_date_range_and_rows(parquet_path)
    
    assert result["rows"] == 5
    assert result["min_obs_date"] == "2024-01-01"
    assert result["max_obs_date"] == "2024-01-05"


def test_read_parquet_obs_date_range_and_rows_empty(tmp_path: Path):
    """Test reading empty parquet."""
    df = pd.DataFrame({"obs_date": [], "value": []})
    parquet_path = tmp_path / "empty.parquet"
    df.to_parquet(parquet_path, index=False)
    
    result = read_parquet_obs_date_range_and_rows(parquet_path)
    
    assert result["rows"] == 0
    assert result["min_obs_date"] is None
    assert result["max_obs_date"] is None


def test_read_parquet_obs_date_range_missing_column(tmp_path: Path):
    """Test that missing obs_date column raises ValueError."""
    df = pd.DataFrame({"value": [1.0, 1.1]})
    parquet_path = tmp_path / "no_obs_date.parquet"
    df.to_parquet(parquet_path, index=False)
    
    with pytest.raises(ValueError, match="Column 'obs_date' not found"):
        read_parquet_obs_date_range_and_rows(parquet_path)


def test_build_run_manifest(tmp_path: Path):
    """Test building complete run manifest."""
    # Create gold input parquet
    gold_dir = tmp_path / "gold"
    gold_dir.mkdir()
    gold_path1 = gold_dir / "FXUSDCAD.parquet"
    gold_path2 = gold_dir / "FXEURCAD.parquet"
    
    df_gold1 = pd.DataFrame({
        "obs_date": pd.date_range("2024-01-01", periods=3, freq="D"),
        "value": [1.0, 1.1, 1.2],
    })
    df_gold1.to_parquet(gold_path1, index=False)
    
    df_gold2 = pd.DataFrame({
        "obs_date": pd.date_range("2024-01-02", periods=2, freq="D"),
        "value": [1.5, 1.6],
    })
    df_gold2.to_parquet(gold_path2, index=False)
    
    # Create model artifacts
    artifacts_dir = tmp_path / "models"
    artifacts_dir.mkdir()
    model_file = artifacts_dir / "model.joblib"
    features_file = artifacts_dir / "features.json"
    model_file.write_bytes(b"fake model data")
    features_file.write_bytes(b'{"features": ["f1", "f2"]}')
    
    # Create predictions parquet
    predictions_path = tmp_path / "predictions.parquet"
    df_pred = pd.DataFrame({
        "obs_date": pd.date_range("2024-01-01", periods=4, freq="D"),
        "series_id": ["FXUSDCAD", "FXUSDCAD", "FXEURCAD", "FXEURCAD"],
        "p_up_logreg": [0.6, 0.7, 0.4, 0.5],
        "action_logreg": ["UP", "UP", "DOWN", "SIDEWAYS"],
    })
    df_pred.to_parquet(predictions_path, index=False)
    
    # Build manifest
    manifest = build_run_manifest(
        run_date="2024-01-15",
        run_timestamp="2024-01-15T14:30:00-05:00",
        gold_inputs=[
            {"series_id": "FXEURCAD", "path": gold_path2},
            {"series_id": "FXUSDCAD", "path": gold_path1},  # Out of order to test sorting
        ],
        model_artifacts={
            "dir": artifacts_dir,
            "files": {
                "model.joblib": model_file,
                "features.json": features_file,
            },
        },
        predictions_path=predictions_path,
    )
    
    # Verify structure
    assert manifest["run_date"] == "2024-01-15"
    assert manifest["run_timestamp"] == "2024-01-15T14:30:00-05:00"
    assert manifest["timezone"] == "America/Toronto"
    assert manifest["git_sha"] is not None
    assert len(manifest["git_sha"]) == 40
    assert manifest["python_version"] is not None
    
    # Verify gold_inputs sorted by series_id
    gold_inputs = manifest["gold_inputs"]
    assert len(gold_inputs) == 2
    assert gold_inputs[0]["series_id"] == "FXEURCAD"  # Sorted
    assert gold_inputs[1]["series_id"] == "FXUSDCAD"
    
    # Verify gold input structure
    for gold_input in gold_inputs:
        assert "series_id" in gold_input
        assert "path" in gold_input
        assert "sha256" in gold_input
        assert len(gold_input["sha256"]) == 64
        assert "bytes" in gold_input
        assert "rows" in gold_input
        assert "min_obs_date" in gold_input
        assert "max_obs_date" in gold_input
    
    # Verify model artifacts structure
    assert "model_artifacts" in manifest
    assert manifest["model_artifacts"]["dir"] == str(artifacts_dir)
    assert "files" in manifest["model_artifacts"]
    assert "model.joblib" in manifest["model_artifacts"]["files"]
    assert "features.json" in manifest["model_artifacts"]["files"]
    
    for file_info in manifest["model_artifacts"]["files"].values():
        assert "path" in file_info
        assert "sha256" in file_info
        assert len(file_info["sha256"]) == 64
        assert "bytes" in file_info
    
    # Verify predictions structure
    assert "predictions" in manifest
    assert manifest["predictions"]["path"] == str(predictions_path)
    assert manifest["predictions"]["rows"] == 4
    assert manifest["predictions"]["min_obs_date"] == "2024-01-01"
    assert manifest["predictions"]["max_obs_date"] == "2024-01-04"
    assert "by_series_rows" in manifest["predictions"]
    assert manifest["predictions"]["by_series_rows"]["FXEURCAD"] == 2
    assert manifest["predictions"]["by_series_rows"]["FXUSDCAD"] == 2


def test_build_run_manifest_missing_predictions_columns(tmp_path: Path):
    """Test that missing required columns in predictions raises ValueError."""
    predictions_path = tmp_path / "predictions.parquet"
    df_pred = pd.DataFrame({
        "obs_date": pd.date_range("2024-01-01", periods=2, freq="D"),
        "series_id": ["FXUSDCAD", "FXUSDCAD"],
        # Missing p_up_logreg and action_logreg
    })
    df_pred.to_parquet(predictions_path, index=False)
    
    gold_path = tmp_path / "gold.parquet"
    df_gold = pd.DataFrame({
        "obs_date": pd.date_range("2024-01-01", periods=2, freq="D"),
        "value": [1.0, 1.1],
    })
    df_gold.to_parquet(gold_path, index=False)
    
    artifacts_dir = tmp_path / "models"
    artifacts_dir.mkdir()
    model_file = artifacts_dir / "model.joblib"
    model_file.write_bytes(b"fake")
    
    with pytest.raises(ValueError, match="missing required columns"):
        build_run_manifest(
            run_date="2024-01-15",
            run_timestamp="2024-01-15T14:30:00-05:00",
            gold_inputs=[{"series_id": "FXUSDCAD", "path": gold_path}],
            model_artifacts={"dir": artifacts_dir, "files": {"model.joblib": model_file}},
            predictions_path=predictions_path,
        )


def test_build_run_manifest_deterministic(tmp_path: Path):
    """Test that manifest is deterministic (same inputs -> same output)."""
    # Create test files
    gold_path = tmp_path / "gold.parquet"
    df_gold = pd.DataFrame({
        "obs_date": pd.date_range("2024-01-01", periods=2, freq="D"),
        "value": [1.0, 1.1],
    })
    df_gold.to_parquet(gold_path, index=False)
    
    predictions_path = tmp_path / "predictions.parquet"
    df_pred = pd.DataFrame({
        "obs_date": pd.date_range("2024-01-01", periods=2, freq="D"),
        "series_id": ["FXUSDCAD", "FXUSDCAD"],
        "p_up_logreg": [0.6, 0.7],
        "action_logreg": ["UP", "DOWN"],
    })
    df_pred.to_parquet(predictions_path, index=False)
    
    artifacts_dir = tmp_path / "models"
    artifacts_dir.mkdir()
    model_file = artifacts_dir / "model.joblib"
    model_file.write_bytes(b"fake")
    
    # Build manifest twice
    manifest1 = build_run_manifest(
        run_date="2024-01-15",
        run_timestamp="2024-01-15T14:30:00-05:00",
        gold_inputs=[{"series_id": "FXUSDCAD", "path": gold_path}],
        model_artifacts={"dir": artifacts_dir, "files": {"model.joblib": model_file}},
        predictions_path=predictions_path,
    )
    
    manifest2 = build_run_manifest(
        run_date="2024-01-15",
        run_timestamp="2024-01-15T14:30:00-05:00",
        gold_inputs=[{"series_id": "FXUSDCAD", "path": gold_path}],
        model_artifacts={"dir": artifacts_dir, "files": {"model.joblib": model_file}},
        predictions_path=predictions_path,
    )
    
    # Manifests should be identical (except git_sha might change if commits happen)
    assert manifest1["run_date"] == manifest2["run_date"]
    assert manifest1["gold_inputs"][0]["sha256"] == manifest2["gold_inputs"][0]["sha256"]
    assert manifest1["predictions"]["sha256"] == manifest2["predictions"]["sha256"]

