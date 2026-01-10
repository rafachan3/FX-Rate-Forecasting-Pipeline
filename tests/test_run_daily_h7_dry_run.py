"""Tests for --dry-run flag in daily runner."""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.pipeline.run_daily_h7 import main


def test_dry_run_validates_config_and_prints_plan(tmp_path: Path, capsys):
    """Test that --dry-run validates config and prints planned actions without executing."""
    config_path = tmp_path / "config.json"
    runs_dir = tmp_path / "outputs" / "runs"
    latest_dir = tmp_path / "outputs" / "latest"
    models_dir = tmp_path / "models"
    
    config_data = {
        "horizon": "h7",
        "timezone": "America/Toronto",
        "series": [
            {"series_id": "FXUSDCAD", "gold_local_path": "data/gold/FXUSDCAD/data.parquet"},
        ],
        "s3": {
            "bucket": "test-bucket",
            "prefix_template": "gold/source=BoC/series={series_id}/",
            "filename": "data.parquet",
            "profile": "fx-gold",
        },
        "artifacts": {
            "dir": str(models_dir),
            "model_file": "model.joblib",
            "features_file": "features.json",
            "metadata_file": "metadata.json",
        },
        "outputs": {
            "runs_dir": str(runs_dir),
            "latest_dir": str(latest_dir),
        },
        "publish": {
            "bucket": "test-bucket",
            "profile": None,
            "prefix_runs_template": "predictions/{horizon}/runs/{run_date}/",
            "prefix_latest": "predictions/{horizon}/latest/",
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
    
    run_date = "2024-01-15"
    
    # Set SENDGRID_API_KEY for email config resolution
    with patch.dict(os.environ, {"SENDGRID_API_KEY": "SG.test-key"}):
        with patch("src.pipeline.run_daily_h7.toronto_today") as mock_today:
            mock_today.return_value.isoformat.return_value = run_date
            
            with patch("src.pipeline.run_daily_h7.parse_args") as mock_args:
                mock_args.return_value = MagicMock(
                    config=str(config_path),
                    sync=True,
                    run_date=None,
                    models_dir=None,
                    publish=True,
                    email=True,
                    dry_run=True,  # Enable dry-run
                )
                
                main()
    
    # Verify no outputs were created
    assert not runs_dir.exists()
    assert not latest_dir.exists()
    
    # Verify output contains expected information
    captured = capsys.readouterr()
    output = captured.out
    
    assert "[DRY RUN]" in output
    assert f"Run date: {run_date}" in output
    assert "Horizon: h7" in output
    assert "FXUSDCAD" in output
    assert "Sync: Enabled" in output
    assert "Publish: Enabled" in output
    assert "Email: Enabled" in output
    assert "sender@example.com" in output
    assert "recipient@example.com" in output
    assert "No files written" in output


def test_dry_run_without_publish_or_email(tmp_path: Path, capsys):
    """Test that --dry-run works without publish/email config."""
    config_path = tmp_path / "config.json"
    
    config_data = {
        "horizon": "h7",
        "timezone": "America/Toronto",
        "series": [
            {"series_id": "FXUSDCAD", "gold_local_path": "data/gold/FXUSDCAD/data.parquet"},
        ],
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
        # No publish or email sections
    }
    
    with open(config_path, "w") as f:
        json.dump(config_data, f)
    
    run_date = "2024-01-15"
    
    with patch("src.pipeline.run_daily_h7.toronto_today") as mock_today:
        mock_today.return_value.isoformat.return_value = run_date
        
        with patch("src.pipeline.run_daily_h7.parse_args") as mock_args:
            mock_args.return_value = MagicMock(
                config=str(config_path),
                sync=False,
                run_date=None,
                models_dir=None,
                publish=False,
                email=False,
                dry_run=True,
            )
            
            main()
    
    # Verify output
    captured = capsys.readouterr()
    output = captured.out
    
    assert "[DRY RUN]" in output
    assert "Publish: Disabled" in output
    assert "Email: Disabled" in output

