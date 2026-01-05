"""
Tests for src.models.export_logreg_h7 module.
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

from src.models.export_logreg_h7 import (
    identify_feature_columns,
    fit_model,
    export_model,
)


def test_identify_feature_columns():
    """Test feature column identification."""
    df = pd.DataFrame({
        "feature1": [1, 2, 3],
        "feature2": [4, 5, 6],
        "direction_7d": [0, 1, 0],
        "fwd_return_7d": [0.1, -0.1, 0.2],
    })
    
    feature_cols = identify_feature_columns(df, "direction_7d", 7)
    
    assert "feature1" in feature_cols
    assert "feature2" in feature_cols
    assert "direction_7d" not in feature_cols
    assert "fwd_return_7d" not in feature_cols
    assert len(feature_cols) == 2


def test_fit_model():
    """Test model fitting."""
    X = pd.DataFrame({
        "feature1": [1, 2, 3, 4, 5],
        "feature2": [0.1, 0.2, 0.3, 0.4, 0.5],
    })
    y = pd.Series([0, 1, 0, 1, 1])
    
    model = fit_model(X, y, use_scaler=False)
    
    assert isinstance(model, Pipeline)
    assert hasattr(model, "predict_proba")
    
    # Test prediction
    proba = model.predict_proba(X)
    assert proba.shape == (5, 2)
    assert np.allclose(proba.sum(axis=1), 1.0)


def test_fit_model_with_scaler():
    """Test model fitting with scaler."""
    X = pd.DataFrame({
        "feature1": [1, 2, 3, 4, 5],
        "feature2": [0.1, 0.2, 0.3, 0.4, 0.5],
    })
    y = pd.Series([0, 1, 0, 1, 1])
    
    model = fit_model(X, y, use_scaler=True)
    
    assert isinstance(model, Pipeline)
    assert len(model.steps) == 2  # scaler + clf
    assert model.steps[0][0] == "scaler"


def test_export_model():
    """Test full export workflow."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        # Create synthetic features parquet
        df_feat = pd.DataFrame({
            "feature1": np.random.randn(10),
            "feature2": np.random.randn(10),
            "direction_7d": np.random.randint(0, 2, 10),
            "fwd_return_7d": np.random.randn(10),
        })
        df_feat.index = pd.date_range("2024-01-01", periods=10, freq="D")
        
        features_path = tmp_path / "features.parquet"
        df_feat.to_parquet(features_path)
        
        out_dir = tmp_path / "models"
        
        # Export model
        export_model(
            features_parquet=features_path,
            out_dir=out_dir,
            version="test_v1",
            target_col="direction_7d",
            horizon=7,
            use_scaler=False,
            dry_run=False,
        )
        
        # Verify outputs
        features_json = out_dir / "features_h7.json"
        assert features_json.exists(), "features_h7.json should be created"
        
        with open(features_json) as f:
            feature_list = json.load(f)
        
        assert isinstance(feature_list, list)
        assert "feature1" in feature_list
        assert "feature2" in feature_list
        assert "direction_7d" not in feature_list
        assert "fwd_return_7d" not in feature_list
        
        # Verify model file
        model_path = out_dir / "logreg_h7.joblib"
        assert model_path.exists(), "logreg_h7.joblib should be created"
        
        # Verify model is loadable
        loaded_model = joblib.load(model_path)
        assert isinstance(loaded_model, Pipeline)
        
        # Verify metadata
        metadata_path = out_dir / "metadata_h7.json"
        assert metadata_path.exists(), "metadata_h7.json should be created"
        
        with open(metadata_path) as f:
            metadata = json.load(f)
        
        assert metadata["version"] == "test_v1"
        assert metadata["horizon"] == 7
        assert metadata["n_features"] == 2
        assert metadata["target_col"] == "direction_7d"
        assert "created_at_utc" in metadata


def test_export_model_dry_run():
    """Test dry run mode."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        df_feat = pd.DataFrame({
            "feature1": np.random.randn(5),
            "direction_7d": np.random.randint(0, 2, 5),
        })
        df_feat.index = pd.date_range("2024-01-01", periods=5, freq="D")
        
        features_path = tmp_path / "features.parquet"
        df_feat.to_parquet(features_path)
        
        out_dir = tmp_path / "models"
        
        # Dry run should not create files
        export_model(
            features_parquet=features_path,
            out_dir=out_dir,
            dry_run=True,
        )
        
        # Verify no files created
        assert not (out_dir / "features_h7.json").exists()
        assert not (out_dir / "logreg_h7.joblib").exists()


def test_export_model_missing_target():
    """Test error handling for missing target column."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        df_feat = pd.DataFrame({
            "feature1": [1, 2, 3],
        })
        
        features_path = tmp_path / "features.parquet"
        df_feat.to_parquet(features_path)
        
        with pytest.raises(ValueError, match="Target column"):
            export_model(
                features_parquet=features_path,
                out_dir=tmp_path / "models",
                target_col="direction_7d",
            )

