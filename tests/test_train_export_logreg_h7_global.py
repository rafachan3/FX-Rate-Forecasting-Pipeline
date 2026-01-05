"""
Tests for src.models.train_export_logreg_h7_global module.
"""
import tempfile
import json
from pathlib import Path

import pytest
import pandas as pd
import numpy as np

try:
    import joblib
    from sklearn.pipeline import Pipeline
except ImportError:
    pytest.skip("sklearn/joblib required", allow_module_level=True)

from src.models.train_export_logreg_h7_global import (
    infer_series_id_from_path,
    load_gold_parquets,
    train_global_model,
    export_global_model,
)
from src.features.h7 import build_features_h7_from_gold


def test_infer_series_id_from_path():
    """Test series_id inference from path."""
    # Test pattern: data/gold/FXUSDCAD/data.parquet
    path1 = Path("data/gold/FXUSDCAD/data.parquet")
    assert infer_series_id_from_path(path1) == "FXUSDCAD"
    
    # Test pattern: data/gold/series=FXEURCAD/data.parquet
    path2 = Path("data/gold/series=FXEURCAD/data.parquet")
    assert infer_series_id_from_path(path2) == "FXEURCAD"
    
    # Test parent directory fallback
    path3 = Path("some/path/FXGBPCAD/file.parquet")
    assert infer_series_id_from_path(path3) == "FXGBPCAD"


def test_load_gold_parquets():
    """Test loading Gold parquet files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        # Create two series directories
        series1_dir = tmp_path / "FXUSDCAD"
        series1_dir.mkdir()
        series2_dir = tmp_path / "FXEURCAD"
        series2_dir.mkdir()
        
        # Create parquet files
        df1 = pd.DataFrame({
            "obs_date": pd.date_range("2024-01-01", periods=5, freq="D"),
            "value": np.random.randn(5),
            "prev_value": np.random.randn(5),
        })
        df1.to_parquet(series1_dir / "data.parquet")
        
        df2 = pd.DataFrame({
            "obs_date": pd.date_range("2024-01-01", periods=5, freq="D"),
            "value": np.random.randn(5),
            "prev_value": np.random.randn(5),
        })
        df2.to_parquet(series2_dir / "data.parquet")
        
        # Load parquets
        series_data = load_gold_parquets(tmp_path, glob_pattern="**/*.parquet")
        
        assert len(series_data) == 2
        series_ids = [sid for sid, _ in series_data]
        assert "FXUSDCAD" in series_ids
        assert "FXEURCAD" in series_ids


def test_build_features_h7_requires_prev_value():
    """Test that feature builder requires prev_value column (Gold contract)."""
    df = pd.DataFrame({
        "obs_date": pd.date_range("2024-01-01", periods=5, freq="D"),
        "series_id": ["FXUSDCAD"] * 5,
        "value": [1.0, 1.1, 1.2, 1.3, 1.4],
    })
    
    # Should raise ValueError for missing prev_value
    with pytest.raises(ValueError, match="Missing columns.*prev_value"):
        build_features_h7_from_gold(df)


def test_export_global_model_requires_prev_value():
    """Test that export fails gracefully when Gold contract violated (missing prev_value)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        # Create a series directory
        series_dir = tmp_path / "FXUSDCAD"
        series_dir.mkdir()
        
        # Create minimal parquet without prev_value (violates Gold contract)
        df = pd.DataFrame({
            "obs_date": pd.date_range("2024-01-01", periods=5, freq="D"),
            "value": np.random.randn(5),
        })
        df.to_parquet(series_dir / "data.parquet")
        
        # Export should raise ValueError because prev_value is missing
        with pytest.raises(ValueError, match="No feature dataframes created"):
            export_global_model(
                gold_root=tmp_path,
                out_dir=tmp_path / "models",
                dry_run=True,
            )


def test_train_global_model():
    """Test global model training with synthetic data."""
    # Create synthetic feature dataframe
    df = pd.DataFrame({
        "obs_date": pd.date_range("2024-01-01", periods=20, freq="D"),
        "series_id": ["FXUSDCAD"] * 10 + ["FXEURCAD"] * 10,
        "feature1": np.random.randn(20),
        "feature2": np.random.randn(20),
        "direction_7d": np.random.randint(0, 2, 20),
    })
    
    model = train_global_model(df, target_col="direction_7d")
    
    assert isinstance(model, Pipeline)
    assert hasattr(model, "predict_proba")
    
    # Test prediction
    X = df.drop(columns=["direction_7d"])
    proba = model.predict_proba(X)
    assert proba.shape == (20, 2)


def test_train_global_model_with_multiple_series():
    """Test model training with multiple series (categorical feature)."""
    df = pd.DataFrame({
        "obs_date": pd.date_range("2024-01-01", periods=30, freq="D"),
        "series_id": ["FXUSDCAD"] * 10 + ["FXEURCAD"] * 10 + ["FXGBPCAD"] * 10,
        "feature1": np.random.randn(30),
        "feature2": np.random.randn(30),
        "direction_7d": np.random.randint(0, 2, 30),
    })
    
    model = train_global_model(df, target_col="direction_7d")
    
    # Verify model can predict
    X = df.drop(columns=["direction_7d"])
    predictions = model.predict(X)
    assert len(predictions) == 30

