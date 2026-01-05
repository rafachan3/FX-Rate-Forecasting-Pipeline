"""Smoke tests for daily runner CLI."""
from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.pipeline.run_daily_h7 import main


def create_minimal_gold_parquet(path: Path, series_id: str, n_rows: int = 10) -> None:
    """Create a minimal gold parquet file with required columns."""
    dates = pd.date_range("2024-01-01", periods=n_rows + 7, freq="B")
    df = pd.DataFrame(
        {
            "obs_date": dates,
            "series_id": series_id,
            "value": [1.0 + i * 0.01 for i in range(len(dates))],
            "prev_value": [0.99 + i * 0.01 for i in range(len(dates))],
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def create_minimal_predictions_parquet(path: Path, n_rows: int = 5) -> None:
    """Create a minimal predictions parquet file."""
    df = pd.DataFrame(
        {
            "obs_date": pd.date_range("2024-01-01", periods=n_rows, freq="D"),
            "series_id": ["FXUSDCAD"] * n_rows,
            "p_up_logreg": [0.6] * n_rows,
            "action_logreg": ["UP"] * n_rows,
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def test_run_daily_h7_smoke(tmp_path: Path):
    """Smoke test for daily runner: creates run dir, predictions, manifest, promotes to latest."""
    # Create config
    config_path = tmp_path / "config.json"
    runs_dir = tmp_path / "outputs" / "runs"
    latest_dir = tmp_path / "outputs" / "latest"
    gold_dir = tmp_path / "data" / "gold"
    models_dir = tmp_path / "models"
    
    gold_path = gold_dir / "FXUSDCAD" / "data.parquet"
    create_minimal_gold_parquet(gold_path, "FXUSDCAD", n_rows=100)
    
    # Create minimal model artifacts
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "logreg_h7.joblib").write_bytes(b"fake model")
    (models_dir / "features_h7.json").write_text(json.dumps(["feature1", "feature2"]))
    (models_dir / "metadata_h7.json").write_text(
        json.dumps({"version": "test", "horizon": 7})
    )
    
    config_data = {
        "horizon": "h7",
        "timezone": "America/Toronto",
        "series": [
            {"series_id": "FXUSDCAD", "gold_local_path": str(gold_path)},
        ],
        "s3": {
            "bucket": "test-bucket",
            "prefix_template": "gold/source=BoC/series={series_id}/",
            "filename": "data.parquet",
            "profile": "fx-gold",
        },
        "artifacts": {
            "dir": str(models_dir),
            "model_file": "logreg_h7.joblib",
            "features_file": "features_h7.json",
            "metadata_file": "metadata_h7.json",
        },
        "outputs": {
            "runs_dir": str(runs_dir),
            "latest_dir": str(latest_dir),
        },
    }
    
    with open(config_path, "w") as f:
        json.dump(config_data, f)
    
    # Mock inference subprocess to create predictions directly
    run_date = "2024-01-15"
    run_predictions_path = runs_dir / run_date / "decision_predictions_h7.parquet"
    
    def mock_inference_subprocess(cmd, **kwargs):
        # Create predictions file directly
        create_minimal_predictions_parquet(run_predictions_path, n_rows=5)
        return MagicMock(returncode=0, stderr="")
    
    # Mock toronto_today to return fixed date
    with patch("src.pipeline.run_daily_h7.toronto_today") as mock_today:
        mock_today.return_value.isoformat.return_value = run_date
        
        with patch("src.pipeline.run_daily_h7.toronto_now_iso") as mock_now:
            mock_now.return_value = "2024-01-15T14:30:00-05:00"
            
            with patch("subprocess.run", side_effect=mock_inference_subprocess):
                # Mock get_git_sha
                with patch("src.artifacts.manifest.get_git_sha") as mock_git:
                    mock_git.return_value = "a" * 40
                    
                    # Run main with mocked args
                    with patch(
                        "src.pipeline.run_daily_h7.parse_args"
                    ) as mock_args:
                        mock_args.return_value = MagicMock(
                            config=str(config_path),
                            sync=False,
                            run_date=None,
                            models_dir=None,
                            publish=False,
                        )
                        
                        main()
    
    # Verify run directory was created
    run_dir = runs_dir / run_date
    assert run_dir.exists()
    
    # Verify predictions exist in run directory (not fixed outputs/ path)
    assert run_predictions_path.exists()
    assert run_predictions_path.parent == run_dir
    
    # Verify manifest exists in run directory
    manifest_path = run_dir / "manifest.json"
    assert manifest_path.exists()
    
    # Verify manifest content references run directory path
    with open(manifest_path) as f:
        manifest = json.load(f)
    
    assert manifest["run_date"] == run_date
    assert "gold_inputs" in manifest
    assert "model_artifacts" in manifest
    assert "predictions" in manifest
    # Verify manifest references run directory path, not fixed outputs/ path
    assert str(run_predictions_path) in manifest["predictions"]["path"]
    
    # Verify latest contains both files (promoted from run directory)
    assert (latest_dir / "decision_predictions_h7.parquet").exists()
    assert (latest_dir / "manifest.json").exists()
    
    # Verify fixed outputs/decision_predictions_h7.parquet is NOT required
    # (daily runner writes directly to run_dir, not to fixed path)
    fixed_output_path = tmp_path / "outputs" / "decision_predictions_h7.parquet"
    # This file should NOT exist because we're using --out with run_dir path
    assert not fixed_output_path.exists()


def test_run_daily_h7_publish_called_after_outputs_exist(tmp_path: Path):
    """Test that publish functions are called only after outputs exist."""
    # Create config with publish section
    config_path = tmp_path / "config.json"
    runs_dir = tmp_path / "outputs" / "runs"
    latest_dir = tmp_path / "outputs" / "latest"
    gold_dir = tmp_path / "data" / "gold"
    models_dir = tmp_path / "models"
    
    gold_path = gold_dir / "FXUSDCAD" / "data.parquet"
    create_minimal_gold_parquet(gold_path, "FXUSDCAD", n_rows=100)
    
    # Create minimal model artifacts
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "logreg_h7.joblib").write_bytes(b"fake model")
    (models_dir / "features_h7.json").write_text(json.dumps(["feature1", "feature2"]))
    (models_dir / "metadata_h7.json").write_text(
        json.dumps({"version": "test", "horizon": 7})
    )
    
    config_data = {
        "horizon": "h7",
        "timezone": "America/Toronto",
        "series": [
            {"series_id": "FXUSDCAD", "gold_local_path": str(gold_path)},
        ],
        "s3": {
            "bucket": "test-bucket",
            "prefix_template": "gold/source=BoC/series={series_id}/",
            "filename": "data.parquet",
            "profile": "fx-gold",
        },
        "artifacts": {
            "dir": str(models_dir),
            "model_file": "logreg_h7.joblib",
            "features_file": "features_h7.json",
            "metadata_file": "metadata_h7.json",
        },
        "outputs": {
            "runs_dir": str(runs_dir),
            "latest_dir": str(latest_dir),
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
    
    # Mock inference subprocess to create predictions directly
    run_date = "2024-01-15"
    run_predictions_path = runs_dir / run_date / "decision_predictions_h7.parquet"
    
    def mock_inference_subprocess(cmd, **kwargs):
        # Create predictions file directly
        create_minimal_predictions_parquet(run_predictions_path, n_rows=5)
        return MagicMock(returncode=0, stderr="")
    
    # Mock publish functions
    with patch("src.pipeline.run_daily_h7.publish_run_outputs") as mock_publish_run:
        with patch("src.pipeline.run_daily_h7.publish_latest_outputs") as mock_publish_latest:
            # Mock toronto_today to return fixed date
            with patch("src.pipeline.run_daily_h7.toronto_today") as mock_today:
                mock_today.return_value.isoformat.return_value = run_date
                
                with patch("src.pipeline.run_daily_h7.toronto_now_iso") as mock_now:
                    mock_now.return_value = "2024-01-15T14:30:00-05:00"
                    
                    with patch("subprocess.run", side_effect=mock_inference_subprocess):
                        # Mock get_git_sha
                        with patch("src.artifacts.manifest.get_git_sha") as mock_git:
                            mock_git.return_value = "a" * 40
                            
                            # Run main with mocked args and --publish flag
                            with patch(
                                "src.pipeline.run_daily_h7.parse_args"
                            ) as mock_args:
                                mock_args.return_value = MagicMock(
                                    config=str(config_path),
                                    sync=False,
                                    run_date=None,
                                    models_dir=None,
                                    publish=True,  # Enable publish
                                )
                                
                                main()
            
            # Verify publish_run_outputs was called after outputs exist
            assert mock_publish_run.called
            call_args = mock_publish_run.call_args
            assert call_args.kwargs["run_dir"] == str(runs_dir / run_date)
            assert call_args.kwargs["horizon"] == "h7"
            assert call_args.kwargs["run_date"] == run_date
            assert call_args.kwargs["bucket"] == "fx-rate-pipeline-dev"
            assert call_args.kwargs["profile"] == "fx-gold"
            
            # Verify publish_latest_outputs was called after publish_run_outputs
            assert mock_publish_latest.called
            call_args = mock_publish_latest.call_args
            assert call_args.kwargs["latest_dir"] == str(latest_dir)
            assert call_args.kwargs["horizon"] == "h7"
            assert call_args.kwargs["bucket"] == "fx-rate-pipeline-dev"
            assert call_args.kwargs["profile"] == "fx-gold"
            
            # Verify publish_run was called before publish_latest
            assert mock_publish_run.call_count == 1
            assert mock_publish_latest.call_count == 1


def test_run_daily_h7_publish_run_failure_prevents_latest_publish(tmp_path: Path):
    """Test that if publish_run fails, publish_latest is not called."""
    # Create config with publish section
    config_path = tmp_path / "config.json"
    runs_dir = tmp_path / "outputs" / "runs"
    latest_dir = tmp_path / "outputs" / "latest"
    gold_dir = tmp_path / "data" / "gold"
    models_dir = tmp_path / "models"
    
    gold_path = gold_dir / "FXUSDCAD" / "data.parquet"
    create_minimal_gold_parquet(gold_path, "FXUSDCAD", n_rows=100)
    
    # Create minimal model artifacts
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "logreg_h7.joblib").write_bytes(b"fake model")
    (models_dir / "features_h7.json").write_text(json.dumps(["feature1", "feature2"]))
    (models_dir / "metadata_h7.json").write_text(
        json.dumps({"version": "test", "horizon": 7})
    )
    
    config_data = {
        "horizon": "h7",
        "timezone": "America/Toronto",
        "series": [
            {"series_id": "FXUSDCAD", "gold_local_path": str(gold_path)},
        ],
        "s3": {
            "bucket": "test-bucket",
            "prefix_template": "gold/source=BoC/series={series_id}/",
            "filename": "data.parquet",
            "profile": "fx-gold",
        },
        "artifacts": {
            "dir": str(models_dir),
            "model_file": "logreg_h7.joblib",
            "features_file": "features_h7.json",
            "metadata_file": "metadata_h7.json",
        },
        "outputs": {
            "runs_dir": str(runs_dir),
            "latest_dir": str(latest_dir),
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
    
    # Mock inference subprocess to create predictions directly
    run_date = "2024-01-15"
    run_predictions_path = runs_dir / run_date / "decision_predictions_h7.parquet"
    
    def mock_inference_subprocess(cmd, **kwargs):
        # Create predictions file directly
        create_minimal_predictions_parquet(run_predictions_path, n_rows=5)
        return MagicMock(returncode=0, stderr="")
    
    # Mock publish_run_outputs to fail
    with patch("src.pipeline.run_daily_h7.publish_run_outputs") as mock_publish_run:
        mock_publish_run.side_effect = RuntimeError("S3 upload failed")
        
        with patch("src.pipeline.run_daily_h7.publish_latest_outputs") as mock_publish_latest:
            # Mock toronto_today to return fixed date
            with patch("src.pipeline.run_daily_h7.toronto_today") as mock_today:
                mock_today.return_value.isoformat.return_value = run_date
                
                with patch("src.pipeline.run_daily_h7.toronto_now_iso") as mock_now:
                    mock_now.return_value = "2024-01-15T14:30:00-05:00"
                    
                    with patch("subprocess.run", side_effect=mock_inference_subprocess):
                        # Mock get_git_sha
                        with patch("src.artifacts.manifest.get_git_sha") as mock_git:
                            mock_git.return_value = "a" * 40
                            
                            # Run main with mocked args and --publish flag
                            with patch(
                                "src.pipeline.run_daily_h7.parse_args"
                            ) as mock_args:
                                mock_args.return_value = MagicMock(
                                    config=str(config_path),
                                    sync=False,
                                    run_date=None,
                                    models_dir=None,
                                    publish=True,  # Enable publish
                                )
                                
                                with pytest.raises(RuntimeError, match="S3 upload failed"):
                                    main()
            
            # Verify publish_run_outputs was called
            assert mock_publish_run.called
            
            # Verify publish_latest_outputs was NOT called (because publish_run failed)
            assert not mock_publish_latest.called


def test_run_daily_h7_publish_without_config_raises_error(tmp_path: Path):
    """Test that --publish flag without publish config raises ValueError."""
    # Create config WITHOUT publish section
    config_path = tmp_path / "config.json"
    runs_dir = tmp_path / "outputs" / "runs"
    latest_dir = tmp_path / "outputs" / "latest"
    gold_dir = tmp_path / "data" / "gold"
    models_dir = tmp_path / "models"
    
    gold_path = gold_dir / "FXUSDCAD" / "data.parquet"
    create_minimal_gold_parquet(gold_path, "FXUSDCAD", n_rows=100)
    
    # Create minimal model artifacts
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "logreg_h7.joblib").write_bytes(b"fake model")
    (models_dir / "features_h7.json").write_text(json.dumps(["feature1", "feature2"]))
    (models_dir / "metadata_h7.json").write_text(
        json.dumps({"version": "test", "horizon": 7})
    )
    
    config_data = {
        "horizon": "h7",
        "timezone": "America/Toronto",
        "series": [
            {"series_id": "FXUSDCAD", "gold_local_path": str(gold_path)},
        ],
        "s3": {
            "bucket": "test-bucket",
            "prefix_template": "gold/source=BoC/series={series_id}/",
            "filename": "data.parquet",
            "profile": "fx-gold",
        },
        "artifacts": {
            "dir": str(models_dir),
            "model_file": "logreg_h7.joblib",
            "features_file": "features_h7.json",
            "metadata_file": "metadata_h7.json",
        },
        "outputs": {
            "runs_dir": str(runs_dir),
            "latest_dir": str(latest_dir),
        },
        # No publish section
    }
    
    with open(config_path, "w") as f:
        json.dump(config_data, f)
    
    # Mock inference subprocess to create predictions directly
    run_date = "2024-01-15"
    run_predictions_path = runs_dir / run_date / "decision_predictions_h7.parquet"
    
    def mock_inference_subprocess(cmd, **kwargs):
        # Create predictions file directly
        create_minimal_predictions_parquet(run_predictions_path, n_rows=5)
        return MagicMock(returncode=0, stderr="")
    
    # Mock toronto_today to return fixed date
    with patch("src.pipeline.run_daily_h7.toronto_today") as mock_today:
        mock_today.return_value.isoformat.return_value = run_date
        
        with patch("src.pipeline.run_daily_h7.toronto_now_iso") as mock_now:
            mock_now.return_value = "2024-01-15T14:30:00-05:00"
            
            with patch("subprocess.run", side_effect=mock_inference_subprocess):
                # Mock get_git_sha
                with patch("src.artifacts.manifest.get_git_sha") as mock_git:
                    mock_git.return_value = "a" * 40
                    
                    # Run main with mocked args and --publish flag
                    with patch("src.pipeline.run_daily_h7.parse_args") as mock_args:
                        mock_args.return_value = MagicMock(
                            config=str(config_path),
                            sync=False,
                            run_date=None,
                            models_dir=None,
                            publish=True,  # Enable publish but no config
                        )
                        
                        with pytest.raises(
                            ValueError, match="publish configuration is missing"
                        ):
                            main()


def test_run_daily_h7_sync_failure_includes_aws_command(tmp_path: Path):
    """Test that sync failure error message includes AWS command with profile."""
    config_path = tmp_path / "config.json"
    runs_dir = tmp_path / "outputs" / "runs"
    latest_dir = tmp_path / "outputs" / "latest"
    models_dir = tmp_path / "models"
    
    config_data = {
        "horizon": "h7",
        "timezone": "America/Toronto",
        "series": [
            {"series_id": "FXUSDCAD", "gold_local_path": str(tmp_path / "gold.parquet")},
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
    }
    
    with open(config_path, "w") as f:
        json.dump(config_data, f)
    
    # Mock sync to fail
    def mock_sync_failure(cmd, **kwargs):
        return MagicMock(returncode=1, stderr="Access Denied")
    
    run_date = "2024-01-15"
    
    with patch("src.pipeline.run_daily_h7.toronto_today") as mock_today:
        mock_today.return_value.isoformat.return_value = run_date
        
        with patch("subprocess.run", side_effect=mock_sync_failure):
            with patch("src.pipeline.run_daily_h7.parse_args") as mock_args:
                mock_args.return_value = MagicMock(
                    config=str(config_path),
                    sync=True,  # Enable sync
                    run_date=None,
                    models_dir=None,
                    publish=False,
                )
                
                with pytest.raises(RuntimeError) as exc_info:
                    main()
                
                # Verify error message includes AWS command with profile
                error_msg = str(exc_info.value)
                assert "--profile fx-gold" in error_msg or "cmd=" in error_msg


def test_run_daily_h7_inference_failure_leaves_latest_unchanged(tmp_path: Path):
    """Test that if inference fails, latest directory remains unchanged."""
    # Create config
    config_path = tmp_path / "config.json"
    runs_dir = tmp_path / "outputs" / "runs"
    latest_dir = tmp_path / "outputs" / "latest"
    models_dir = tmp_path / "models"
    
    config_data = {
        "horizon": "h7",
        "timezone": "America/Toronto",
        "series": [
            {"series_id": "FXUSDCAD", "gold_local_path": str(tmp_path / "gold.parquet")},
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
    }
    
    with open(config_path, "w") as f:
        json.dump(config_data, f)
    
    # Create existing file in latest (should remain unchanged)
    latest_dir.mkdir(parents=True, exist_ok=True)
    existing_file = latest_dir / "decision_predictions_h7.parquet"
    existing_file.write_bytes(b"old content")
    
    # Mock inference to fail
    def mock_inference_failure(cmd, **kwargs):
        return MagicMock(returncode=1, stderr="Inference failed")
    
    run_date = "2024-01-15"
    
    with patch("src.pipeline.run_daily_h7.toronto_today") as mock_today:
        mock_today.return_value.isoformat.return_value = run_date
        
        with patch("subprocess.run", side_effect=mock_inference_failure):
            with patch("src.pipeline.run_daily_h7.parse_args") as mock_args:
                mock_args.return_value = MagicMock(
                    config=str(config_path),
                    sync=False,
                    run_date=None,
                    models_dir=None,
                    publish=False,
                )
                
                with pytest.raises(RuntimeError, match="Inference failed"):
                    main()
    
    # Verify latest file was not modified
    assert existing_file.exists()
    assert existing_file.read_bytes() == b"old content"
    
    # Verify no new manifest in latest
    assert not (latest_dir / "manifest.json").exists()

