"""Tests for S3 publishing module."""
from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.pipeline.publish_s3 import (
    aws_s3_cp,
    publish_latest_outputs,
    publish_run_outputs,
    s3_uri,
)


def test_s3_uri():
    """Test S3 URI construction."""
    assert s3_uri("test-bucket", "path/to/file") == "s3://test-bucket/path/to/file"
    assert s3_uri("my-bucket", "key") == "s3://my-bucket/key"


def test_aws_s3_cp_success():
    """Test successful AWS S3 copy."""
    with tempfile.TemporaryDirectory() as tmpdir:
        local_file = Path(tmpdir) / "test.txt"
        local_file.write_text("test content")
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stderr="")
            
            aws_s3_cp(local_file, "test-bucket", "path/to/file.txt", "test-profile")
            
            # Verify subprocess.run was called
            assert mock_run.called
            
            # Verify command includes --profile
            call_args = mock_run.call_args
            cmd = call_args[0][0]
            assert "aws" in cmd
            assert "s3" in cmd
            assert "cp" in cmd
            assert str(local_file) in cmd
            assert "s3://test-bucket/path/to/file.txt" in cmd
            assert "--profile" in cmd
            assert "test-profile" in cmd
            assert "--only-show-errors" in cmd


def test_aws_s3_cp_failure_includes_command():
    """Test that AWS S3 copy failure includes command in error message."""
    with tempfile.TemporaryDirectory() as tmpdir:
        local_file = Path(tmpdir) / "test.txt"
        local_file.write_text("test content")
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1, stderr="AccessDenied: Access Denied"
            )
            
            with pytest.raises(RuntimeError) as exc_info:
                aws_s3_cp(local_file, "test-bucket", "path/to/file.txt", "test-profile")
            
            error_msg = str(exc_info.value)
            assert "AWS S3 copy failed" in error_msg
            assert "exit code 1" in error_msg
            assert "Command:" in error_msg
            assert "aws s3 cp" in error_msg
            assert "--profile" in error_msg
            assert "test-profile" in error_msg
            assert "AccessDenied" in error_msg


def test_aws_s3_cp_file_not_found():
    """Test that AWS S3 copy raises FileNotFoundError for missing local file."""
    with pytest.raises(FileNotFoundError):
        aws_s3_cp("nonexistent.txt", "test-bucket", "path/to/file.txt", "test-profile")


def test_publish_run_outputs():
    """Test publishing run outputs to S3."""
    with tempfile.TemporaryDirectory() as tmpdir:
        run_dir = Path(tmpdir) / "runs" / "2024-01-15"
        run_dir.mkdir(parents=True)
        
        # Create required files
        predictions_file = run_dir / "decision_predictions_h7.parquet"
        manifest_file = run_dir / "manifest.json"
        predictions_file.write_bytes(b"fake parquet")
        manifest_file.write_bytes(b'{"test": "data"}')
        
        with patch("src.pipeline.publish_s3.aws_s3_cp") as mock_cp:
            publish_run_outputs(
                run_dir=run_dir,
                horizon="h7",
                run_date="2024-01-15",
                bucket="test-bucket",
                profile="test-profile",
                prefix_runs_template="predictions/{horizon}/runs/{run_date}/",
            )
            
            # Verify aws_s3_cp called twice (predictions and manifest)
            assert mock_cp.call_count == 2
            
            # Verify first call (predictions)
            first_call = mock_cp.call_args_list[0]
            assert str(predictions_file) in str(first_call)
            assert "test-bucket" in str(first_call)
            assert "predictions/h7/runs/2024-01-15/decision_predictions_h7.parquet" in str(
                first_call
            )
            assert "test-profile" in str(first_call)
            
            # Verify second call (manifest)
            second_call = mock_cp.call_args_list[1]
            assert str(manifest_file) in str(second_call)
            assert "predictions/h7/runs/2024-01-15/manifest.json" in str(second_call)


def test_publish_run_outputs_missing_file():
    """Test that publish_run_outputs raises FileNotFoundError for missing files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        run_dir = Path(tmpdir) / "runs" / "2024-01-15"
        run_dir.mkdir(parents=True)
        
        # Missing predictions file
        manifest_file = run_dir / "manifest.json"
        manifest_file.write_bytes(b'{"test": "data"}')
        
        with pytest.raises(FileNotFoundError, match="Predictions file not found"):
            publish_run_outputs(
                run_dir=run_dir,
                horizon="h7",
                run_date="2024-01-15",
                bucket="test-bucket",
                profile="test-profile",
                prefix_runs_template="predictions/{horizon}/runs/{run_date}/",
            )


def test_publish_latest_outputs():
    """Test publishing latest outputs to S3."""
    with tempfile.TemporaryDirectory() as tmpdir:
        latest_dir = Path(tmpdir) / "latest"
        latest_dir.mkdir()
        
        # Create required files
        predictions_file = latest_dir / "decision_predictions_h7.parquet"
        manifest_file = latest_dir / "manifest.json"
        predictions_file.write_bytes(b"fake parquet")
        manifest_file.write_bytes(b'{"test": "data"}')
        
        with patch("src.pipeline.publish_s3.aws_s3_cp") as mock_cp:
            publish_latest_outputs(
                latest_dir=latest_dir,
                horizon="h7",
                bucket="test-bucket",
                profile="test-profile",
                prefix_latest="predictions/{horizon}/latest/",
            )
            
            # Verify aws_s3_cp called twice (predictions and manifest)
            assert mock_cp.call_count == 2
            
            # Verify first call (predictions)
            first_call = mock_cp.call_args_list[0]
            assert str(predictions_file) in str(first_call)
            assert "test-bucket" in str(first_call)
            assert "predictions/h7/latest/decision_predictions_h7.parquet" in str(first_call)
            assert "test-profile" in str(first_call)
            
            # Verify second call (manifest)
            second_call = mock_cp.call_args_list[1]
            assert "predictions/h7/latest/manifest.json" in str(second_call)


def test_publish_latest_outputs_missing_file():
    """Test that publish_latest_outputs raises FileNotFoundError for missing files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        latest_dir = Path(tmpdir) / "latest"
        latest_dir.mkdir()
        
        # Missing predictions file
        manifest_file = latest_dir / "manifest.json"
        manifest_file.write_bytes(b'{"test": "data"}')
        
        with pytest.raises(FileNotFoundError, match="Predictions file not found"):
            publish_latest_outputs(
                latest_dir=latest_dir,
                horizon="h7",
                bucket="test-bucket",
                profile="test-profile",
                prefix_latest="predictions/{horizon}/latest/",
            )

