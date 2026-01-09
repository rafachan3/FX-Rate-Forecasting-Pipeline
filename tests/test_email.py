"""Tests for SendGrid email module."""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.pipeline.config import EmailConfig
from src.pipeline.email import (
    build_email_body_html,
    build_email_body_text,
    build_email_subject,
    send_email,
)


# =============================================================================
# EmailConfig Tests
# =============================================================================

def test_email_config_valid():
    """Test valid SendGrid email configuration."""
    with patch.dict(os.environ, {"SENDGRID_API_KEY": "SG.test-key-12345"}):
        email = EmailConfig(
            api_key="${SENDGRID_API_KEY}",
            from_email="sender@example.com",
            to_emails=["recipient@example.com"],
            subject_template="[FX] {horizon} latest — {run_date}",
            body_format="text",
        )
        assert email.api_key == "SG.test-key-12345"  # Resolved from env
        assert email.from_email == "sender@example.com"


def test_email_config_direct_api_key():
    """Test config with direct API key (not from env var)."""
    email = EmailConfig(
        api_key="SG.direct-api-key",
        from_email="sender@example.com",
        to_emails=["recipient@example.com"],
        subject_template="[FX] {horizon} latest — {run_date}",
    )
    assert email.api_key == "SG.direct-api-key"


def test_email_config_missing_env_var():
    """Test that missing environment variable raises ValueError."""
    with patch.dict(os.environ, {}, clear=True):
        os.environ.pop("SENDGRID_API_KEY", None)
        
        with pytest.raises(ValueError, match="Environment variable SENDGRID_API_KEY not set"):
            EmailConfig(
                api_key="${SENDGRID_API_KEY}",
                from_email="sender@example.com",
                to_emails=["recipient@example.com"],
                subject_template="[FX] {horizon} latest — {run_date}",
            )


def test_email_config_missing_api_key():
    """Test that missing api_key raises ValueError."""
    with pytest.raises(ValueError, match="email.api_key must be a non-empty string"):
        EmailConfig(
            api_key="",
            from_email="sender@example.com",
            to_emails=["recipient@example.com"],
            subject_template="[FX] {horizon} latest — {run_date}",
        )


def test_email_config_empty_to_emails():
    """Test that empty to_emails raises ValueError."""
    with pytest.raises(ValueError, match="email.to_emails must be a non-empty list"):
        EmailConfig(
            api_key="SG.test-key",
            from_email="sender@example.com",
            to_emails=[],
            subject_template="[FX] {horizon} latest — {run_date}",
        )


def test_email_config_missing_horizon_placeholder():
    """Test that missing {horizon} placeholder raises ValueError."""
    with pytest.raises(ValueError, match='email.subject_template must contain "{horizon}"'):
        EmailConfig(
            api_key="SG.test-key",
            from_email="sender@example.com",
            to_emails=["recipient@example.com"],
            subject_template="[FX] latest — {run_date}",
        )


def test_email_config_missing_run_date_placeholder():
    """Test that missing {run_date} placeholder raises ValueError."""
    with pytest.raises(ValueError, match='email.subject_template must contain "{run_date}"'):
        EmailConfig(
            api_key="SG.test-key",
            from_email="sender@example.com",
            to_emails=["recipient@example.com"],
            subject_template="[FX] {horizon} latest",
        )


# =============================================================================
# build_email_subject Tests
# =============================================================================

def test_build_email_subject():
    """Test email subject template rendering."""
    cfg = EmailConfig(
        api_key="SG.test-key",
        from_email="sender@example.com",
        to_emails=["recipient@example.com"],
        subject_template="[FX] {horizon} latest — {run_date}",
    )
    
    subject = build_email_subject(cfg, "h7", "2024-01-15")
    assert subject == "[FX] h7 latest — 2024-01-15"


# =============================================================================
# build_email_body_text Tests
# =============================================================================

def test_build_email_body_text_deterministic_ordering():
    """Test that email body uses deterministic ordering by series_id."""
    with tempfile.TemporaryDirectory() as tmpdir:
        latest_dir = Path(tmpdir) / "latest"
        latest_dir.mkdir()
        
        df_pred = pd.DataFrame({
            "obs_date": pd.date_range("2024-01-01", periods=5, freq="D"),
            "series_id": ["FXEURCAD", "FXUSDCAD", "FXEURCAD", "FXUSDCAD", "FXGBPCAD"],
            "p_up_logreg": [0.6, 0.7, 0.65, 0.75, 0.5],
            "action_logreg": ["UP", "UP", "UP", "UP", "DOWN"],
        })
        predictions_file = latest_dir / "decision_predictions_h7.parquet"
        df_pred.to_parquet(predictions_file, index=False)
        
        body = build_email_body_text(
            horizon="h7",
            run_date="2024-01-15",
            latest_dir=latest_dir,
        )
        
        # Check that all currency pairs are present
        assert "EUR/CAD" in body
        assert "GBP/CAD" in body
        assert "USD/CAD" in body
        
        # Check that the order is deterministic (alphabetical by series_id)
        # EUR/CAD should appear before GBP/CAD, and GBP/CAD before USD/CAD
        eur_pos = body.find("EUR/CAD")
        gbp_pos = body.find("GBP/CAD")
        usd_pos = body.find("USD/CAD")
        
        assert eur_pos < gbp_pos < usd_pos, "Currency pairs should be in alphabetical order"
        
        # Check header and date info
        assert "7-Day" in body  # Horizon display name
        assert "2024-01-15" in body or "January 15, 2024" in body
        assert "TODAY'S SIGNALS" in body or "SIGNALS" in body


def test_build_email_body_text_missing_predictions():
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


def test_build_email_body_html():
    """Test that HTML email body is generated correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        latest_dir = Path(tmpdir) / "latest"
        latest_dir.mkdir()
        
        df_pred = pd.DataFrame({
            "obs_date": pd.date_range("2024-01-01", periods=3, freq="D"),
            "series_id": ["FXUSDCAD", "FXEURCAD", "FXUSDCAD"],
            "p_up_logreg": [0.75, 0.35, 0.72],
            "action_logreg": ["UP", "DOWN", "UP"],
        })
        predictions_file = latest_dir / "decision_predictions_h7.parquet"
        df_pred.to_parquet(predictions_file, index=False)
        
        html = build_email_body_html(
            horizon="h7",
            run_date="2024-01-15",
            latest_dir=latest_dir,
        )
        
        # Check HTML structure
        assert "<!DOCTYPE html>" in html
        assert "<html>" in html
        assert "</html>" in html
        
        # Check currency pairs are present
        assert "USD/CAD" in html
        assert "EUR/CAD" in html
        
        # Check signals info
        assert "7-Day" in html  # Horizon display name
        assert "January 15, 2024" in html  # Readable date
        
        # Check signal indicators (UP should have Bullish, DOWN should have Bearish)
        assert "Bullish" in html
        assert "Bearish" in html
        
        # Check probability display
        assert "75.0%" in html or "72.0%" in html  # USD/CAD probability
        assert "35.0%" in html  # EUR/CAD probability


# =============================================================================
# send_email Tests
# =============================================================================

def test_send_email_success():
    """Test successful email send via SendGrid."""
    cfg = EmailConfig(
        api_key="SG.test-key-12345",
        from_email="sender@example.com",
        to_emails=["recipient@example.com"],
        subject_template="[FX] {horizon} latest — {run_date}",
    )
    
    with patch("src.pipeline.email.SendGridAPIClient") as mock_sg_class:
        mock_client = MagicMock()
        mock_sg_class.return_value = mock_client
        mock_response = MagicMock()
        mock_response.status_code = 202
        mock_response.body = b""
        mock_client.send.return_value = mock_response
        
        send_email(cfg, "Test Subject", "Test body")
        
        mock_sg_class.assert_called_once_with("SG.test-key-12345")
        assert mock_client.send.called


def test_send_email_failure_non_2xx():
    """Test that non-2xx response raises RuntimeError."""
    cfg = EmailConfig(
        api_key="SG.test-key",
        from_email="sender@example.com",
        to_emails=["recipient@example.com"],
        subject_template="[FX] {horizon} latest — {run_date}",
    )
    
    with patch("src.pipeline.email.SendGridAPIClient") as mock_sg_class:
        mock_client = MagicMock()
        mock_sg_class.return_value = mock_client
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.body = b"Bad Request"
        mock_client.send.return_value = mock_response
        
        with pytest.raises(RuntimeError) as exc_info:
            send_email(cfg, "Test Subject", "Test body")
        
        assert "400" in str(exc_info.value)


def test_send_email_exception():
    """Test that SendGrid exceptions are wrapped in RuntimeError."""
    cfg = EmailConfig(
        api_key="SG.test-key",
        from_email="sender@example.com",
        to_emails=["recipient@example.com"],
        subject_template="[FX] {horizon} latest — {run_date}",
    )
    
    with patch("src.pipeline.email.SendGridAPIClient") as mock_sg_class:
        mock_sg_class.side_effect = Exception("Network error")
        
        with pytest.raises(RuntimeError) as exc_info:
            send_email(cfg, "Test Subject", "Test body")
        
        assert "Network error" in str(exc_info.value)


def test_send_email_missing_api_key():
    """Test that missing API key raises ValueError."""
    cfg = MagicMock()
    cfg.api_key = None
    cfg.from_email = "sender@example.com"
    cfg.to_emails = ["recipient@example.com"]
    
    with pytest.raises(ValueError, match="API key not configured"):
        send_email(cfg, "Test Subject", "Test body")
