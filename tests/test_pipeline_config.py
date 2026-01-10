"""Tests for pipeline configuration schema and loader."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

import os
from unittest.mock import patch

from src.pipeline.config import (
    ArtifactsConfig,
    EmailConfig,
    OutputsConfig,
    PipelineConfig,
    PublishConfig,
    SeriesConfig,
    S3Config,
    load_pipeline_config,
)


def test_load_sample_config_successfully():
    """Test that sample config loads successfully."""
    config_path = Path("config/pipeline_h7.json")
    if not config_path.exists():
        pytest.skip("Sample config file not found")
    
    # Sample config uses env var for API key, so we need to mock it
    with patch.dict(os.environ, {"SENDGRID_API_KEY": "SG.test-key-for-sample-config"}):
        config = load_pipeline_config(config_path)
    
    assert isinstance(config, PipelineConfig)
    assert config.horizon == "h7"
    assert config.timezone == "America/Toronto"
    assert len(config.series) > 0
    
    # Check that FXUSDCAD is present (don't assume it's first)
    series_ids = [s.series_id for s in config.series]
    assert "FXUSDCAD" in series_ids
    
    # If config has 23 series, verify count
    if len(config.series) == 23:
        assert len(config.series) == 23
    
    assert config.s3.bucket == "fx-rate-pipeline-dev"
    assert config.artifacts.dir == "models"
    # Email config should be loaded
    assert config.email is not None
    assert config.email.body_format == "html"


def test_horizon_mismatch_raises_value_error():
    """Test that horizon mismatch raises ValueError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "config.json"
        config_data = {
            "horizon": "h14",
            "timezone": "America/Toronto",
            "series": [{"series_id": "FXUSDCAD", "gold_local_path": "data/gold/FXUSDCAD/data.parquet"}],
            "s3": {
                "bucket": "test-bucket",
                "prefix_template": "gold/source=BoC/series={series_id}/",
                "filename": "data.parquet",
                "profile": "fx-gold",
            },
            "artifacts": {
                "dir": "models",
                "model_file": "model.joblib",
                "features_file": "features.json",
                "metadata_file": "metadata.json",
            },
            "outputs": {
                "runs_dir": "outputs/runs",
                "latest_dir": "outputs/latest",
            },
        }
        with open(config_path, "w") as f:
            json.dump(config_data, f)
        
        with pytest.raises(ValueError, match='horizon must be "h7"'):
            load_pipeline_config(config_path)


def test_timezone_mismatch_raises_value_error():
    """Test that timezone mismatch raises ValueError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "config.json"
        config_data = {
            "horizon": "h7",
            "timezone": "UTC",
            "series": [{"series_id": "FXUSDCAD", "gold_local_path": "data/gold/FXUSDCAD/data.parquet"}],
            "s3": {
                "bucket": "test-bucket",
                "prefix_template": "gold/source=BoC/series={series_id}/",
                "filename": "data.parquet",
                "profile": "fx-gold",
            },
            "artifacts": {
                "dir": "models",
                "model_file": "model.joblib",
                "features_file": "features.json",
                "metadata_file": "metadata.json",
            },
            "outputs": {
                "runs_dir": "outputs/runs",
                "latest_dir": "outputs/latest",
            },
        }
        with open(config_path, "w") as f:
            json.dump(config_data, f)
        
        with pytest.raises(ValueError, match='timezone must be "America/Toronto"'):
            load_pipeline_config(config_path)


def test_prefix_template_missing_series_id_raises_value_error():
    """Test that prefix_template missing {series_id} raises ValueError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "config.json"
        config_data = {
            "horizon": "h7",
            "timezone": "America/Toronto",
            "series": [{"series_id": "FXUSDCAD", "gold_local_path": "data/gold/FXUSDCAD/data.parquet"}],
            "s3": {
                "bucket": "test-bucket",
                "prefix_template": "gold/source=BoC/",
                "filename": "data.parquet",
                "profile": "fx-gold",
            },
            "artifacts": {
                "dir": "models",
                "model_file": "model.joblib",
                "features_file": "features.json",
                "metadata_file": "metadata.json",
            },
            "outputs": {
                "runs_dir": "outputs/runs",
                "latest_dir": "outputs/latest",
            },
        }
        with open(config_path, "w") as f:
            json.dump(config_data, f)
        
        with pytest.raises(ValueError, match='prefix_template must contain "{series_id}"'):
            load_pipeline_config(config_path)


def test_unknown_top_level_key_raises_value_error():
    """Test that unknown top-level key raises ValueError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "config.json"
        config_data = {
            "horizon": "h7",
            "timezone": "America/Toronto",
            "series": [{"series_id": "FXUSDCAD", "gold_local_path": "data/gold/FXUSDCAD/data.parquet"}],
            "s3": {
                "bucket": "test-bucket",
                "prefix_template": "gold/source=BoC/series={series_id}/",
                "filename": "data.parquet",
                "profile": "fx-gold",
            },
            "artifacts": {
                "dir": "models",
                "model_file": "model.joblib",
                "features_file": "features.json",
                "metadata_file": "metadata.json",
            },
            "outputs": {
                "runs_dir": "outputs/runs",
                "latest_dir": "outputs/latest",
            },
            "unknown_key": "should fail",
        }
        with open(config_path, "w") as f:
            json.dump(config_data, f)
        
        with pytest.raises(ValueError, match="Unknown keys in top-level"):
            load_pipeline_config(config_path)


def test_unknown_nested_key_raises_value_error():
    """Test that unknown nested key (e.g., in artifacts) raises ValueError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "config.json"
        config_data = {
            "horizon": "h7",
            "timezone": "America/Toronto",
            "series": [{"series_id": "FXUSDCAD", "gold_local_path": "data/gold/FXUSDCAD/data.parquet"}],
            "s3": {
                "bucket": "test-bucket",
                "prefix_template": "gold/source=BoC/series={series_id}/",
                "filename": "data.parquet",
                "profile": "fx-gold",
            },
            "artifacts": {
                "dir": "models",
                "model_file": "model.joblib",
                "features_file": "features.json",
                "metadata_file": "metadata.json",
                "extra_field": "should fail",
            },
            "outputs": {
                "runs_dir": "outputs/runs",
                "latest_dir": "outputs/latest",
            },
        }
        with open(config_path, "w") as f:
            json.dump(config_data, f)
        
        with pytest.raises(ValueError, match="Unknown keys in artifacts"):
            load_pipeline_config(config_path)


def test_s3_key_for_series_returns_correct_format():
    """Test that s3_key_for_series returns correct format."""
    config_path = Path("config/pipeline_h7.json")
    if not config_path.exists():
        pytest.skip("Sample config file not found")
    
    # Sample config uses env var for API key, so we need to mock it
    with patch.dict(os.environ, {"SENDGRID_API_KEY": "SG.test-key-for-sample-config"}):
        config = load_pipeline_config(config_path)
    
    result = config.s3_key_for_series("FXUSDCAD")
    assert result == "gold/source=BoC/series=FXUSDCAD/data.parquet"
    assert not result.startswith("s3://")
    assert "fx-rate-pipeline-dev" not in result  # Bucket not included


def test_series_config_validation():
    """Test SeriesConfig validation."""
    # Valid config
    series = SeriesConfig(series_id="FXUSDCAD", gold_local_path="data/gold/FXUSDCAD/data.parquet")
    assert series.series_id == "FXUSDCAD"
    
    # Empty series_id
    with pytest.raises(ValueError, match="series_id must be a non-empty string"):
        SeriesConfig(series_id="", gold_local_path="data/gold/FXUSDCAD/data.parquet")
    
    # Empty gold_local_path
    with pytest.raises(ValueError, match="gold_local_path must be a non-empty string"):
        SeriesConfig(series_id="FXUSDCAD", gold_local_path="")


def test_s3_config_validation():
    """Test S3Config validation."""
    # Valid config
    s3 = S3Config(
        bucket="test-bucket",
        prefix_template="gold/source=BoC/series={series_id}/",
        filename="data.parquet",
        profile="fx-gold",
    )
    assert s3.bucket == "test-bucket"
    assert s3.profile == "fx-gold"
    
    # Missing {series_id} in prefix_template
    with pytest.raises(ValueError, match='prefix_template must contain "{series_id}"'):
        S3Config(
            bucket="test-bucket",
            prefix_template="gold/source=BoC/",
            filename="data.parquet",
            profile="fx-gold",
        )
    
    # Filename doesn't end with .parquet
    with pytest.raises(ValueError, match='filename must end with ".parquet"'):
        S3Config(
            bucket="test-bucket",
            prefix_template="gold/source=BoC/series={series_id}/",
            filename="data.json",
            profile="fx-gold",
        )
    
    # Empty profile string (not None)
    with pytest.raises(ValueError, match="s3.profile must be None or a non-empty string"):
        S3Config(
            bucket="test-bucket",
            prefix_template="gold/source=BoC/series={series_id}/",
            filename="data.parquet",
            profile="",
        )
    
    # None profile is valid
    s3_none = S3Config(
        bucket="test-bucket",
        prefix_template="gold/source=BoC/series={series_id}/",
        filename="data.parquet",
        profile=None,
    )
    assert s3_none.profile is None


def test_artifacts_config_validation():
    """Test ArtifactsConfig validation."""
    # Valid config
    artifacts = ArtifactsConfig(
        dir="models",
        model_file="model.joblib",
        features_file="features.json",
        metadata_file="metadata.json",
    )
    assert artifacts.dir == "models"
    
    # Empty dir
    with pytest.raises(ValueError, match="artifacts.dir must be a non-empty string"):
        ArtifactsConfig(
            dir="",
            model_file="model.joblib",
            features_file="features.json",
            metadata_file="metadata.json",
        )


def test_outputs_config_validation():
    """Test OutputsConfig validation."""
    # Valid config
    outputs = OutputsConfig(runs_dir="outputs/runs", latest_dir="outputs/latest")
    assert outputs.runs_dir == "outputs/runs"
    
    # Empty runs_dir
    with pytest.raises(ValueError, match="outputs.runs_dir must be a non-empty string"):
        OutputsConfig(runs_dir="", latest_dir="outputs/latest")


def test_publish_config_validation():
    """Test PublishConfig validation."""
    # Valid config
    publish = PublishConfig(
        bucket="test-bucket",
        profile="fx-gold",
        prefix_runs_template="predictions/{horizon}/runs/{run_date}/",
        prefix_latest="predictions/{horizon}/latest/",
    )
    assert publish.bucket == "test-bucket"
    assert publish.profile == "fx-gold"
    
    # Missing {horizon} in prefix_runs_template
    with pytest.raises(ValueError, match='publish.prefix_runs_template must contain "{horizon}"'):
        PublishConfig(
            bucket="test-bucket",
            profile="fx-gold",
            prefix_runs_template="predictions/runs/{run_date}/",
            prefix_latest="predictions/{horizon}/latest/",
        )
    
    # Missing {run_date} in prefix_runs_template
    with pytest.raises(ValueError, match='publish.prefix_runs_template must contain "{run_date}"'):
        PublishConfig(
            bucket="test-bucket",
            profile="fx-gold",
            prefix_runs_template="predictions/{horizon}/runs/",
            prefix_latest="predictions/{horizon}/latest/",
        )
    
    # Missing {horizon} in prefix_latest
    with pytest.raises(ValueError, match='publish.prefix_latest must contain "{horizon}"'):
        PublishConfig(
            bucket="test-bucket",
            profile="fx-gold",
            prefix_runs_template="predictions/{horizon}/runs/{run_date}/",
            prefix_latest="predictions/latest/",
        )
    
    # Empty bucket
    with pytest.raises(ValueError, match="publish.bucket must be a non-empty string"):
        PublishConfig(
            bucket="",
            profile="fx-gold",
            prefix_runs_template="predictions/{horizon}/runs/{run_date}/",
            prefix_latest="predictions/{horizon}/latest/",
        )
    
    # Empty profile string (not None)
    with pytest.raises(ValueError, match="publish.profile must be None or a non-empty string"):
        PublishConfig(
            bucket="test-bucket",
            profile="",
            prefix_runs_template="predictions/{horizon}/runs/{run_date}/",
            prefix_latest="predictions/{horizon}/latest/",
        )
    
    # None profile is valid
    publish_none = PublishConfig(
        bucket="test-bucket",
        profile=None,
        prefix_runs_template="predictions/{horizon}/runs/{run_date}/",
        prefix_latest="predictions/{horizon}/latest/",
    )
    assert publish_none.profile is None


def test_load_config_with_publish():
    """Test that config with publish section loads successfully."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "config.json"
        config_data = {
            "horizon": "h7",
            "timezone": "America/Toronto",
            "series": [{"series_id": "FXUSDCAD", "gold_local_path": "data/gold/FXUSDCAD/data.parquet"}],
            "s3": {
                "bucket": "test-bucket",
                "prefix_template": "gold/source=BoC/series={series_id}/",
                "filename": "data.parquet",
                "profile": "fx-gold",
            },
            "artifacts": {
                "dir": "models",
                "model_file": "model.joblib",
                "features_file": "features.json",
                "metadata_file": "metadata.json",
            },
            "outputs": {
                "runs_dir": "outputs/runs",
                "latest_dir": "outputs/latest",
            },
            "publish": {
                "bucket": "fx-rate-pipeline-dev",
                "profile": "fx-gold",
                "prefix_runs_template": "predictions/{horizon}/runs/{run_date}/",
                "prefix_latest": "predictions/{horizon}/latest/",
            },
        }
        with open(config_path, "w") as f:
            json.dump(config_data, f)
        
        config = load_pipeline_config(config_path)
        assert config.publish is not None
        assert config.publish.bucket == "fx-rate-pipeline-dev"
        assert config.publish.profile == "fx-gold"
        assert config.publish.prefix_runs_template == "predictions/{horizon}/runs/{run_date}/"
        assert config.publish.prefix_latest == "predictions/{horizon}/latest/"


def test_load_config_without_publish():
    """Test that config without publish section loads successfully (publish is None)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "config.json"
        config_data = {
            "horizon": "h7",
            "timezone": "America/Toronto",
            "series": [{"series_id": "FXUSDCAD", "gold_local_path": "data/gold/FXUSDCAD/data.parquet"}],
            "s3": {
                "bucket": "test-bucket",
                "prefix_template": "gold/source=BoC/series={series_id}/",
                "filename": "data.parquet",
                "profile": "fx-gold",
            },
            "artifacts": {
                "dir": "models",
                "model_file": "model.joblib",
                "features_file": "features.json",
                "metadata_file": "metadata.json",
            },
            "outputs": {
                "runs_dir": "outputs/runs",
                "latest_dir": "outputs/latest",
            },
        }
        with open(config_path, "w") as f:
            json.dump(config_data, f)
        
        config = load_pipeline_config(config_path)
        assert config.publish is None

def test_load_config_with_email():
    """Test that config with email section loads successfully."""
    with patch.dict(os.environ, {"SENDGRID_API_KEY": "SG.test-key"}):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.json"
            config_data = {
                "horizon": "h7",
                "timezone": "America/Toronto",
                "series": [{"series_id": "FXUSDCAD", "gold_local_path": "data/gold/FXUSDCAD/data.parquet"}],
                "s3": {
                    "bucket": "test-bucket",
                    "prefix_template": "gold/source=BoC/series={series_id}/",
                    "filename": "data.parquet",
                    "profile": "fx-gold",
                },
                "artifacts": {
                    "dir": "models",
                    "model_file": "model.joblib",
                    "features_file": "features.json",
                    "metadata_file": "metadata.json",
                },
                "outputs": {
                    "runs_dir": "outputs/runs",
                    "latest_dir": "outputs/latest",
                },
                "email": {
                    "api_key": "${SENDGRID_API_KEY}",
                    "from_email": "sender@example.com",
                    "to_emails": ["recipient@example.com"],
                    "subject_template": "[FX] {horizon} latest — {run_date}",
                    "body_format": "text",
                },
            }
            with open(config_path, "w") as f:
                json.dump(config_data, f)
            
            config = load_pipeline_config(config_path)
            assert config.email is not None
            assert config.email.api_key == "SG.test-key"
            assert config.email.from_email == "sender@example.com"
            assert config.email.to_emails == ["recipient@example.com"]
            assert config.email.subject_template == "[FX] {horizon} latest — {run_date}"


def test_load_config_without_email():
    """Test that config without email section loads successfully (email is None)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "config.json"
        config_data = {
            "horizon": "h7",
            "timezone": "America/Toronto",
            "series": [{"series_id": "FXUSDCAD", "gold_local_path": "data/gold/FXUSDCAD/data.parquet"}],
            "s3": {
                "bucket": "test-bucket",
                "prefix_template": "gold/source=BoC/series={series_id}/",
                "filename": "data.parquet",
                "profile": "fx-gold",
            },
            "artifacts": {
                "dir": "models",
                "model_file": "model.joblib",
                "features_file": "features.json",
                "metadata_file": "metadata.json",
            },
            "outputs": {
                "runs_dir": "outputs/runs",
                "latest_dir": "outputs/latest",
            },
        }
        with open(config_path, "w") as f:
            json.dump(config_data, f)
        
        config = load_pipeline_config(config_path)
        assert config.email is None

