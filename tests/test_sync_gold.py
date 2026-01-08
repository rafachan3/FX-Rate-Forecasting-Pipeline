"""Tests for gold data synchronization."""
from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.data_access.sync_gold import sync_gold_from_config, sync_gold_series
from src.pipeline.config import PipelineConfig, load_pipeline_config


def test_sync_gold_series_calls_aws_cli_correctly(tmp_path: Path):
    """Test that sync_gold_series calls AWS CLI with correct arguments including profile."""
    bucket = "test-bucket"
    key = "gold/source=BoC/series=FXUSDCAD/data.parquet"
    dst_path = tmp_path / "gold" / "FXUSDCAD" / "data.parquet"
    profile = "fx-gold"
    
    # Create a dummy file to simulate downloaded content
    dummy_content = b"fake parquet data"
    
    with patch("subprocess.run") as mock_run:
        # Mock successful AWS CLI execution
        mock_run.return_value = MagicMock(returncode=0, stderr="")
        
        # Mock file creation
        with patch("tempfile.NamedTemporaryFile") as mock_temp:
            mock_file = MagicMock()
            mock_file.name = str(tmp_path / "temp.tmp")
            mock_file.__enter__ = MagicMock(return_value=mock_file)
            mock_file.__exit__ = MagicMock(return_value=None)
            mock_temp.return_value = mock_file
            
            # Create temp file with content
            temp_file = tmp_path / "temp.tmp"
            temp_file.write_bytes(dummy_content)
            
            sync_gold_series(bucket=bucket, key=key, dst_path=str(dst_path), profile=profile)
        
        # Verify AWS CLI was called correctly
        mock_run.assert_called_once()
        call_args = mock_run.call_args
        cmd = call_args[0][0]
        assert cmd[0] == "aws"
        assert cmd[1] == "s3"
        assert cmd[2] == "cp"
        assert cmd[3] == f"s3://{bucket}/{key}"
        assert str(cmd[4]) == str(temp_file)  # Temp file path
        assert "--profile" in cmd
        assert profile in cmd
        assert "--only-show-errors" in cmd
        # Verify profile flag and value appear after --only-show-errors (order: base cmd, --only-show-errors, --profile, profile)
        profile_idx = cmd.index("--profile")
        profile_value_idx = cmd.index(profile)
        only_show_errors_idx = cmd.index("--only-show-errors")
        assert profile_value_idx == profile_idx + 1
        assert only_show_errors_idx < profile_idx  # --only-show-errors comes before --profile
        assert call_args[1]["capture_output"] is True
        assert call_args[1]["text"] is True
        assert call_args[1]["check"] is False
        
        # Verify destination file exists and was atomically replaced
        assert dst_path.exists()
        assert dst_path.read_bytes() == dummy_content


def test_sync_gold_series_without_profile_omits_profile_flag(tmp_path: Path):
    """Test that sync_gold_series omits --profile flag when profile is None."""
    bucket = "test-bucket"
    key = "gold/source=BoC/series=FXUSDCAD/data.parquet"
    dst_path = tmp_path / "gold" / "FXUSDCAD" / "data.parquet"
    
    # Create a dummy file to simulate downloaded content
    dummy_content = b"fake parquet data"
    
    with patch("subprocess.run") as mock_run:
        # Mock successful AWS CLI execution
        mock_run.return_value = MagicMock(returncode=0, stderr="")
        
        # Mock file creation
        with patch("tempfile.NamedTemporaryFile") as mock_temp:
            mock_file = MagicMock()
            mock_file.name = str(tmp_path / "temp.tmp")
            mock_file.__enter__ = MagicMock(return_value=mock_file)
            mock_file.__exit__ = MagicMock(return_value=None)
            mock_temp.return_value = mock_file
            
            # Create temp file with content
            temp_file = tmp_path / "temp.tmp"
            temp_file.write_bytes(dummy_content)
            
            # Call with profile=None (or omit profile parameter)
            sync_gold_series(bucket=bucket, key=key, dst_path=str(dst_path), profile=None)
        
        # Verify AWS CLI was called correctly WITHOUT --profile flag
        mock_run.assert_called_once()
        call_args = mock_run.call_args
        cmd = call_args[0][0]
        assert cmd[:4] == ["aws", "s3", "cp", f"s3://{bucket}/{key}"]
        assert str(cmd[4]) == str(temp_file)  # Temp file path
        assert "--profile" not in cmd  # Profile flag should not be present
        assert "--only-show-errors" in cmd
        assert call_args[1]["capture_output"] is True
        assert call_args[1]["text"] is True
        assert call_args[1]["check"] is False
        
        # Verify destination file exists and was atomically replaced
        assert dst_path.exists()
        assert dst_path.read_bytes() == dummy_content


def test_sync_gold_series_raises_on_aws_cli_not_found(tmp_path: Path):
    """Test that sync_gold_series raises FileNotFoundError when AWS CLI is not found."""
    bucket = "test-bucket"
    key = "gold/source=BoC/series=FXUSDCAD/data.parquet"
    dst_path = tmp_path / "gold" / "FXUSDCAD" / "data.parquet"
    profile = "fx-gold"
    
    with patch("subprocess.run") as mock_run:
        # Mock AWS CLI not found (exit code 127)
        mock_run.return_value = MagicMock(returncode=127, stderr="")
        
        with pytest.raises(FileNotFoundError, match="AWS CLI not found"):
            sync_gold_series(bucket=bucket, key=key, dst_path=str(dst_path), profile=profile)


def test_sync_gold_series_raises_on_aws_cli_failure(tmp_path: Path):
    """Test that sync_gold_series raises RuntimeError with command string when AWS CLI fails."""
    bucket = "test-bucket"
    key = "gold/source=BoC/series=FXUSDCAD/data.parquet"
    dst_path = tmp_path / "gold" / "FXUSDCAD" / "data.parquet"
    profile = "fx-gold"
    
    with patch("subprocess.run") as mock_run:
        # Mock AWS CLI failure
        mock_run.return_value = MagicMock(returncode=1, stderr="Access Denied")
        
        with patch("tempfile.NamedTemporaryFile") as mock_temp:
            mock_file = MagicMock()
            mock_file.name = str(tmp_path / "temp.tmp")
            mock_file.__enter__ = MagicMock(return_value=mock_file)
            mock_file.__exit__ = MagicMock(return_value=None)
            mock_temp.return_value = mock_file
            
            with pytest.raises(RuntimeError) as exc_info:
                sync_gold_series(bucket=bucket, key=key, dst_path=str(dst_path), profile=profile)
            
            # Verify error message includes command string and profile
            error_msg = str(exc_info.value)
            assert "exit=1" in error_msg
            assert "cmd=" in error_msg
            assert f"--profile {profile}" in error_msg
            assert "Access Denied" in error_msg


def test_sync_gold_series_without_profile_error_message(tmp_path: Path):
    """Test that error message includes command string even when profile is None."""
    bucket = "test-bucket"
    key = "gold/source=BoC/series=FXUSDCAD/data.parquet"
    dst_path = tmp_path / "gold" / "FXUSDCAD" / "data.parquet"
    
    with patch("subprocess.run") as mock_run:
        # Mock AWS CLI failure
        mock_run.return_value = MagicMock(returncode=1, stderr="Access Denied")
        
        with patch("tempfile.NamedTemporaryFile") as mock_temp:
            mock_file = MagicMock()
            mock_file.name = str(tmp_path / "temp.tmp")
            mock_file.__enter__ = MagicMock(return_value=mock_file)
            mock_file.__exit__ = MagicMock(return_value=None)
            mock_temp.return_value = mock_file
            
            with pytest.raises(RuntimeError) as exc_info:
                sync_gold_series(bucket=bucket, key=key, dst_path=str(dst_path), profile=None)
            
            # Verify error message includes command string but NOT --profile
            error_msg = str(exc_info.value)
            assert "exit=1" in error_msg
            assert "cmd=" in error_msg
            assert "--profile" not in error_msg  # Should not include profile when None
            assert "Access Denied" in error_msg


def test_sync_gold_series_raises_on_empty_file(tmp_path: Path):
    """Test that sync_gold_series raises RuntimeError when downloaded file is empty."""
    bucket = "test-bucket"
    key = "gold/source=BoC/series=FXUSDCAD/data.parquet"
    dst_path = tmp_path / "gold" / "FXUSDCAD" / "data.parquet"
    profile = "fx-gold"
    
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stderr="")
        
        with patch("tempfile.NamedTemporaryFile") as mock_temp:
            mock_file = MagicMock()
            temp_file = tmp_path / "temp.tmp"
            temp_file.write_bytes(b"")  # Empty file
            mock_file.name = str(temp_file)
            mock_file.__enter__ = MagicMock(return_value=mock_file)
            mock_file.__exit__ = MagicMock(return_value=None)
            mock_temp.return_value = mock_file
            
            with pytest.raises(RuntimeError, match="Downloaded file is empty"):
                sync_gold_series(bucket=bucket, key=key, dst_path=str(dst_path), profile=profile)


def test_sync_gold_series_atomic_replace(tmp_path: Path):
    """Test that sync_gold_series uses atomic file replacement."""
    bucket = "test-bucket"
    key = "gold/source=BoC/series=FXUSDCAD/data.parquet"
    dst_path = tmp_path / "gold" / "FXUSDCAD" / "data.parquet"
    profile = "fx-gold"
    
    # Create existing file
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    dst_path.write_bytes(b"old content")
    
    new_content = b"new content"
    
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stderr="")
        
        with patch("tempfile.NamedTemporaryFile") as mock_temp:
            mock_file = MagicMock()
            temp_file = tmp_path / "temp.tmp"
            temp_file.write_bytes(new_content)
            mock_file.name = str(temp_file)
            mock_file.__enter__ = MagicMock(return_value=mock_file)
            mock_file.__exit__ = MagicMock(return_value=None)
            mock_temp.return_value = mock_file
            
            sync_gold_series(bucket=bucket, key=key, dst_path=str(dst_path), profile=profile)
        
        # Verify file was replaced atomically
        assert dst_path.read_bytes() == new_content
        # Verify temp file was cleaned up
        assert not temp_file.exists()


def test_sync_gold_from_config(tmp_path: Path):
    """Test that sync_gold_from_config syncs all series in sorted order."""
    # Create minimal config
    config_data = {
        "horizon": "h7",
        "timezone": "America/Toronto",
        "series": [
            {"series_id": "FXEURCAD", "gold_local_path": str(tmp_path / "gold" / "FXEURCAD" / "data.parquet")},
            {"series_id": "FXUSDCAD", "gold_local_path": str(tmp_path / "gold" / "FXUSDCAD" / "data.parquet")},
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
    }
    
    import json
    config_path = tmp_path / "config.json"
    with open(config_path, "w") as f:
        json.dump(config_data, f)
    
    config = load_pipeline_config(config_path)
    
    call_order = []
    
    def mock_sync(bucket, key, dst_path, profile):
        call_order.append((bucket, key, dst_path, profile))
    
    with patch("src.data_access.sync_gold.sync_gold_series", side_effect=mock_sync):
        sync_gold_from_config(cfg=config)
    
    # Verify profile was passed
    assert call_order[0][3] == "fx-gold"
    assert call_order[1][3] == "fx-gold"
    
    # Verify series were synced in sorted order (FXEURCAD before FXUSDCAD)
    assert len(call_order) == 2
    assert "FXEURCAD" in call_order[0][1]
    assert "FXUSDCAD" in call_order[1][1]


def test_sync_gold_from_config_with_null_profile(tmp_path: Path):
    """Test that sync_gold_from_config works with null profile (uses ambient credentials)."""
    # Create minimal config with null profile
    config_data = {
        "horizon": "h7",
        "timezone": "America/Toronto",
        "series": [
            {"series_id": "FXUSDCAD", "gold_local_path": str(tmp_path / "gold" / "FXUSDCAD" / "data.parquet")},
        ],
        "s3": {
            "bucket": "test-bucket",
            "prefix_template": "gold/source=BoC/series={series_id}/",
            "filename": "data.parquet",
            "profile": None,
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
    
    import json
    config_path = tmp_path / "config.json"
    with open(config_path, "w") as f:
        json.dump(config_data, f)
    
    config = load_pipeline_config(config_path)
    assert config.s3.profile is None
    
    call_order = []
    
    def mock_sync(bucket, key, dst_path, profile):
        call_order.append((bucket, key, dst_path, profile))
    
    with patch("src.data_access.sync_gold.sync_gold_series", side_effect=mock_sync):
        sync_gold_from_config(cfg=config)
    
    # Verify None profile was passed
    assert call_order[0][3] is None

