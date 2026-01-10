"""Tests for SendGrid email module."""
from __future__ import annotations

import json
import os
import sys
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
# Fixtures for mocking sendgrid module
# =============================================================================

@pytest.fixture
def mock_sendgrid_module():
    """Create a fake sendgrid module in sys.modules for testing."""
    # Create mock sendgrid module structure
    mock_sendgrid = MagicMock()
    mock_helpers = MagicMock()
    
    # Create a mock Mail class that can be instantiated with any kwargs
    class MockMail:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            # Set attributes for convenience
            for key, value in kwargs.items():
                setattr(self, key, value)
    
    mock_helpers.mail.Mail = MockMail
    
    # Make SendGridAPIClient return a mock client with .send() method
    # Store clients by API key so tests can configure them
    clients_by_key = {}
    
    def mock_sg_client_init(api_key):
        if api_key not in clients_by_key:
            client = MagicMock()
            client.api_key = api_key
            # Default response (success)
            response = MagicMock()
            response.status_code = 202
            response.body = b""
            client.send = MagicMock(return_value=response)
            clients_by_key[api_key] = client
        return clients_by_key[api_key]
    
    mock_sendgrid.SendGridAPIClient = MagicMock(side_effect=mock_sg_client_init)
    # Expose clients dict so tests can access and configure clients
    mock_sendgrid._clients_by_key = clients_by_key
    mock_sendgrid.helpers = mock_helpers
    
    # Inject into sys.modules
    sys.modules['sendgrid'] = mock_sendgrid
    sys.modules['sendgrid.helpers'] = mock_helpers
    sys.modules['sendgrid.helpers.mail'] = mock_helpers.mail
    
    yield mock_sendgrid
    
    # Cleanup: remove from sys.modules
    for key in ['sendgrid', 'sendgrid.helpers', 'sendgrid.helpers.mail']:
        sys.modules.pop(key, None)


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

def test_send_email_success(mock_sendgrid_module):
    """Test successful email send via SendGrid."""
    cfg = EmailConfig(
        api_key="SG.test-key-12345",
        from_email="sender@example.com",
        to_emails=["recipient@example.com"],
        subject_template="[FX] {horizon} latest — {run_date}",
    )
    
    send_email(cfg, "Test Subject", "Test body")
    
    # Verify SendGridAPIClient was called with correct API key
    mock_sendgrid_module.SendGridAPIClient.assert_called_once_with("SG.test-key-12345")
    # Get the client that was created
    mock_client = mock_sendgrid_module._clients_by_key["SG.test-key-12345"]
    assert mock_client.send.called


def test_send_email_failure_non_2xx(mock_sendgrid_module):
    """Test that non-2xx response raises RuntimeError."""
    cfg = EmailConfig(
        api_key="SG.test-key",
        from_email="sender@example.com",
        to_emails=["recipient@example.com"],
        subject_template="[FX] {horizon} latest — {run_date}",
    )
    
    # Configure the client to return non-2xx status before calling send_email
    # The client will be created when SendGridAPIClient is called
    # Pre-create it with error response
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.body = b"Bad Request"
    mock_client.send.return_value = mock_response
    mock_sendgrid_module._clients_by_key["SG.test-key"] = mock_client
    
    with pytest.raises(RuntimeError) as exc_info:
        send_email(cfg, "Test Subject", "Test body")
    
    assert "400" in str(exc_info.value)


def test_send_email_exception(mock_sendgrid_module):
    """Test that SendGrid exceptions are wrapped in RuntimeError."""
    cfg = EmailConfig(
        api_key="SG.test-key",
        from_email="sender@example.com",
        to_emails=["recipient@example.com"],
        subject_template="[FX] {horizon} latest — {run_date}",
    )
    
    # Configure mock to raise exception
    mock_sendgrid_module.SendGridAPIClient.side_effect = Exception("Network error")
    
    with pytest.raises(RuntimeError) as exc_info:
        send_email(cfg, "Test Subject", "Test body")
    
    assert "Network error" in str(exc_info.value)


def test_send_email_missing_api_key():
    """Test that missing API key raises ValueError before importing sendgrid."""
    cfg = MagicMock()
    cfg.api_key = None
    cfg.from_email = "sender@example.com"
    cfg.to_emails = ["recipient@example.com"]
    
    # Should raise ValueError without attempting sendgrid import
    with pytest.raises(ValueError, match="API key not configured"):
        send_email(cfg, "Test Subject", "Test body")
