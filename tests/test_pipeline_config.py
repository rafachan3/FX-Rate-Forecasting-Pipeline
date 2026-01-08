"""Tests for pipeline configuration schema and loader."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

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
    
    config = load_pipeline_config(config_path)
    
    assert isinstance(config, PipelineConfig)
    assert config.horizon == "h7"
    assert config.timezone == "America/Toronto"
    assert len(config.series) > 0
    assert config.series[0].series_id == "FXUSDCAD"
    assert config.s3.bucket == "fx-rate-pipeline-dev"
    assert config.artifacts.dir == "models"


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


def test_email_config_validation():
    """Test EmailConfig validation."""
    # Valid config
    email = EmailConfig(
        provider="ses",
        region="us-east-2",
        from_email="sender@example.com",
        to_emails=["recipient@example.com"],
        subject_template="[FX] {horizon} latest — {run_date}",
        body_format="text",
        aws_profile="fx-gold",
    )
    assert email.provider == "ses"
    assert email.region == "us-east-2"
    
    # Invalid provider
    with pytest.raises(ValueError, match='email.provider must be "ses"'):
        EmailConfig(
            provider="smtp",
            region="us-east-2",
            from_email="sender@example.com",
            to_emails=["recipient@example.com"],
            subject_template="[FX] {horizon} latest — {run_date}",
        )
    
    # Missing {horizon} in subject_template
    with pytest.raises(ValueError, match='email.subject_template must contain "{horizon}"'):
        EmailConfig(
            provider="ses",
            region="us-east-2",
            from_email="sender@example.com",
            to_emails=["recipient@example.com"],
            subject_template="[FX] latest — {run_date}",
        )
    
    # Missing {run_date} in subject_template
    with pytest.raises(ValueError, match='email.subject_template must contain "{run_date}"'):
        EmailConfig(
            provider="ses",
            region="us-east-2",
            from_email="sender@example.com",
            to_emails=["recipient@example.com"],
            subject_template="[FX] {horizon} latest",
        )
    
    # Empty to_emails
    with pytest.raises(ValueError, match="email.to_emails must be a non-empty list"):
        EmailConfig(
            provider="ses",
            region="us-east-2",
            from_email="sender@example.com",
            to_emails=[],
            subject_template="[FX] {horizon} latest — {run_date}",
        )
    
    # Empty from_email
    with pytest.raises(ValueError, match="email.from_email must be a non-empty string"):
        EmailConfig(
            provider="ses",
            region="us-east-2",
            from_email="",
            to_emails=["recipient@example.com"],
            subject_template="[FX] {horizon} latest — {run_date}",
        )
    
    # Invalid body_format
    with pytest.raises(ValueError, match='email.body_format must be "text"'):
        EmailConfig(
            provider="ses",
            region="us-east-2",
            from_email="sender@example.com",
            to_emails=["recipient@example.com"],
            subject_template="[FX] {horizon} latest — {run_date}",
            body_format="html",
        )
    
    # Empty aws_profile string
    with pytest.raises(ValueError, match="email.aws_profile must be None or a non-empty string"):
        EmailConfig(
            provider="ses",
            region="us-east-2",
            from_email="sender@example.com",
            to_emails=["recipient@example.com"],
            subject_template="[FX] {horizon} latest — {run_date}",
            aws_profile="",
        )


def test_load_config_with_email():
    """Test that config with email section loads successfully."""
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
                "provider": "ses",
                "region": "us-east-2",
                "from_email": "sender@example.com",
                "to_emails": ["recipient@example.com"],
                "subject_template": "[FX] {horizon} latest — {run_date}",
                "body_format": "text",
                "aws_profile": "fx-gold",
            },
        }
        with open(config_path, "w") as f:
            json.dump(config_data, f)
        
        config = load_pipeline_config(config_path)
        assert config.email is not None
        assert config.email.provider == "ses"
        assert config.email.region == "us-east-2"
        assert config.email.from_email == "sender@example.com"
        assert config.email.to_emails == ["recipient@example.com"]
        assert config.email.subject_template == "[FX] {horizon} latest — {run_date}"
        assert config.email.aws_profile == "fx-gold"


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

