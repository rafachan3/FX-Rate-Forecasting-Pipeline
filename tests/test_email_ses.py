"""Tests for SES email module."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.pipeline.config import EmailConfig
from src.pipeline.email_ses import (
    build_email_body_text,
    build_email_subject,
    send_email_ses,
)


def test_build_email_subject():
    """Test email subject template rendering."""
    cfg = EmailConfig(
        provider="ses",
        region="us-east-2",
        from_email="sender@example.com",
        to_emails=["recipient@example.com"],
        subject_template="[FX] {horizon} latest — {run_date}",
    )
    
    subject = build_email_subject(cfg, "h7", "2024-01-15")
    assert subject == "[FX] h7 latest — 2024-01-15"


def test_build_email_body_text_deterministic_ordering():
    """Test that email body uses deterministic ordering by series_id."""
    with tempfile.TemporaryDirectory() as tmpdir:
        latest_dir = Path(tmpdir) / "latest"
        latest_dir.mkdir()
        
        # Create predictions parquet with multiple series (out of order)
        df_pred = pd.DataFrame({
            "obs_date": pd.date_range("2024-01-01", periods=5, freq="D"),
            "series_id": ["FXEURCAD", "FXUSDCAD", "FXEURCAD", "FXUSDCAD", "FXGBPCAD"],
            "p_up_logreg": [0.6, 0.7, 0.65, 0.75, 0.5],
            "action_logreg": ["UP", "UP", "UP", "UP", "DOWN"],
        })
        predictions_file = latest_dir / "decision_predictions_h7.parquet"
        df_pred.to_parquet(predictions_file, index=False)
        
        # Build email body
        body = build_email_body_text(
            horizon="h7",
            run_date="2024-01-15",
            latest_dir=latest_dir,
        )
        
        # Verify series appear in sorted order (FXEURCAD, FXGBPCAD, FXUSDCAD)
        lines = body.split("\n")
        # Filter for lines that contain series_id pattern (FX followed by 6+ chars, colon)
        signal_lines = [l for l in lines if "  FX" in l and ":" in l and "p=" in l]
        
        # Check ordering: FXEURCAD should come before FXGBPCAD, which comes before FXUSDCAD
        assert len(signal_lines) == 3
        assert "FXEURCAD" in signal_lines[0]
        assert "FXGBPCAD" in signal_lines[1]
        assert "FXUSDCAD" in signal_lines[2]
        
        # Verify body contains expected content
        assert "h7" in body
        assert "2024-01-15" in body
        assert "Latest signals" in body


def test_build_email_body_text_includes_manifest_info():
    """Test that email body includes manifest information if available."""
    with tempfile.TemporaryDirectory() as tmpdir:
        latest_dir = Path(tmpdir) / "latest"
        latest_dir.mkdir()
        
        # Create predictions parquet
        df_pred = pd.DataFrame({
            "obs_date": pd.date_range("2024-01-01", periods=3, freq="D"),
            "series_id": ["FXUSDCAD", "FXUSDCAD", "FXUSDCAD"],
            "p_up_logreg": [0.6, 0.7, 0.65],
            "action_logreg": ["UP", "UP", "UP"],
        })
        predictions_file = latest_dir / "decision_predictions_h7.parquet"
        df_pred.to_parquet(predictions_file, index=False)
        
        # Create manifest
        manifest = {
            "run_date": "2024-01-15",
            "predictions": {
                "by_series_rows": {
                    "FXUSDCAD": 3,
                    "FXEURCAD": 2,
                }
            },
        }
        manifest_file = latest_dir / "manifest.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f)
        
        # Build email body
        body = build_email_body_text(
            horizon="h7",
            run_date="2024-01-15",
            latest_dir=latest_dir,
            manifest_path=manifest_file,
        )
        
        # Verify manifest info included
        assert "Row counts" in body
        assert "FXEURCAD: 2 rows" in body  # Sorted order
        assert "FXUSDCAD: 3 rows" in body


def test_build_email_body_text_includes_s3_locations():
    """Test that email body includes S3 locations if publish config provided."""
    with tempfile.TemporaryDirectory() as tmpdir:
        latest_dir = Path(tmpdir) / "latest"
        latest_dir.mkdir()
        
        # Create predictions parquet
        df_pred = pd.DataFrame({
            "obs_date": pd.date_range("2024-01-01", periods=2, freq="D"),
            "series_id": ["FXUSDCAD", "FXUSDCAD"],
            "p_up_logreg": [0.6, 0.7],
            "action_logreg": ["UP", "UP"],
        })
        predictions_file = latest_dir / "decision_predictions_h7.parquet"
        df_pred.to_parquet(predictions_file, index=False)
        
        # Build email body with publish config
        publish_config = {
            "bucket": "test-bucket",
            "runs_prefix": "predictions/h7/runs/2024-01-15/",
            "latest_prefix": "predictions/h7/latest/",
        }
        
        body = build_email_body_text(
            horizon="h7",
            run_date="2024-01-15",
            latest_dir=latest_dir,
            publish_config=publish_config,
        )
        
        # Verify S3 locations included
        assert "S3 locations" in body
        assert "s3://test-bucket/predictions/h7/runs/2024-01-15/" in body
        assert "s3://test-bucket/predictions/h7/latest/" in body


def test_build_email_body_text_missing_file_raises():
    """Test that missing predictions file raises FileNotFoundError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        latest_dir = Path(tmpdir) / "latest"
        latest_dir.mkdir()
        
        with pytest.raises(FileNotFoundError, match="Predictions file not found"):
            build_email_body_text(
                horizon="h7",
                run_date="2024-01-15",
                latest_dir=latest_dir,
            )


def test_build_email_body_text_missing_columns_raises():
    """Test that missing required columns raises ValueError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        latest_dir = Path(tmpdir) / "latest"
        latest_dir.mkdir()
        
        # Create parquet with missing columns
        df_pred = pd.DataFrame({
            "obs_date": pd.date_range("2024-01-01", periods=2, freq="D"),
            "series_id": ["FXUSDCAD", "FXUSDCAD"],
            # Missing p_up_logreg and action_logreg
        })
        predictions_file = latest_dir / "decision_predictions_h7.parquet"
        df_pred.to_parquet(predictions_file, index=False)
        
        with pytest.raises(ValueError, match="Predictions parquet missing required columns"):
            build_email_body_text(
                horizon="h7",
                run_date="2024-01-15",
                latest_dir=latest_dir,
            )


def test_send_email_ses_success():
    """Test successful email send via SES."""
    cfg = EmailConfig(
        provider="ses",
        region="us-east-2",
        from_email="sender@example.com",
        to_emails=["recipient@example.com"],
        subject_template="[FX] {horizon} latest — {run_date}",
        aws_profile="test-profile",
    )
    
    with patch("boto3.Session") as mock_session:
        mock_client = MagicMock()
        mock_session.return_value.client.return_value = mock_client
        mock_client.send_email.return_value = {"MessageId": "test-message-id"}
        
        send_email_ses(cfg, "Test Subject", "Test body")
        
        # Verify boto3 session created with profile
        mock_session.assert_called_once_with(
            profile_name="test-profile", region_name="us-east-2"
        )
        
        # Verify send_email called
        assert mock_client.send_email.called


def test_send_email_ses_without_profile():
    """Test email send without AWS profile (uses environment/instance profile)."""
    cfg = EmailConfig(
        provider="ses",
        region="us-east-2",
        from_email="sender@example.com",
        to_emails=["recipient@example.com"],
        subject_template="[FX] {horizon} latest — {run_date}",
        aws_profile=None,
    )
    
    with patch("boto3.Session") as mock_session:
        mock_client = MagicMock()
        mock_session.return_value.client.return_value = mock_client
        mock_client.send_email.return_value = {"MessageId": "test-message-id"}
        
        send_email_ses(cfg, "Test Subject", "Test body")
        
        # Verify boto3 session created without profile
        mock_session.assert_called_once_with(region_name="us-east-2")


def test_send_email_ses_failure_includes_error_info():
    """Test that SES failure includes error code/message and from/to addresses."""
    cfg = EmailConfig(
        provider="ses",
        region="us-east-2",
        from_email="sender@example.com",
        to_emails=["recipient@example.com"],
        subject_template="[FX] {horizon} latest — {run_date}",
    )
    
    with patch("boto3.Session") as mock_session:
        from botocore.exceptions import ClientError
        
        mock_client = MagicMock()
        mock_session.return_value.client.return_value = mock_client
        
        # Mock ClientError
        error_response = {
            "Error": {
                "Code": "MessageRejected",
                "Message": "Email address not verified",
            }
        }
        mock_client.send_email.side_effect = ClientError(
            error_response, "SendEmail"
        )
        
        with pytest.raises(RuntimeError) as exc_info:
            send_email_ses(cfg, "Test Subject", "Test body")
        
        error_msg = str(exc_info.value)
        assert "MessageRejected" in error_msg
        assert "Email address not verified" in error_msg
        assert "sender@example.com" in error_msg
        assert "recipient@example.com" in error_msg


def test_send_email_ses_fallback_to_v1():
    """Test that SES falls back to v1 if v2 not available."""
    cfg = EmailConfig(
        provider="ses",
        region="us-east-2",
        from_email="sender@example.com",
        to_emails=["recipient@example.com"],
        subject_template="[FX] {horizon} latest — {run_date}",
    )
    
    with patch("boto3.Session") as mock_session:
        from botocore.exceptions import ClientError
        
        # Mock SESv2 client to fail with InvalidAction
        mock_client_v2 = MagicMock()
        error_response = {"Error": {"Code": "InvalidAction", "Message": "Invalid action"}}
        mock_client_v2.send_email.side_effect = ClientError(error_response, "SendEmail")
        
        # Mock SES v1 client to succeed
        mock_client_v1 = MagicMock()
        mock_client_v1.send_email.return_value = {"MessageId": "test-message-id"}
        
        mock_session_instance = MagicMock()
        mock_session_instance.client.side_effect = [mock_client_v2, mock_client_v1]
        mock_session.return_value = mock_session_instance
        
        send_email_ses(cfg, "Test Subject", "Test body")
        
        # Verify both clients were tried
        assert mock_client_v2.send_email.called
        assert mock_client_v1.send_email.called

