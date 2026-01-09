"""Tests for subscription request validation."""

import pytest

from src.api.models import (
    Frequency,
    MonthlyTiming,
    SubscriptionRequest,
    WeeklyDay,
)


def test_subscription_request_defaults():
    """Test subscription request with defaults."""
    req = SubscriptionRequest(email="test@example.com")
    
    assert req.email == "test@example.com"
    assert req.pairs == ["USD_CAD", "EUR_CAD"]
    assert req.frequency == Frequency.WEEKLY
    assert req.weekly_day == WeeklyDay.WED  # Auto-set for WEEKLY
    assert req.monthly_timing is None


def test_subscription_request_weekly_day_auto_set():
    """Test that weekly_day is auto-set for WEEKLY frequency."""
    req = SubscriptionRequest(
        email="test@example.com",
        frequency=Frequency.WEEKLY,
    )
    assert req.weekly_day == WeeklyDay.WED


def test_subscription_request_daily_no_weekly_day():
    """Test that daily frequency doesn't require weekly_day."""
    req = SubscriptionRequest(
        email="test@example.com",
        frequency=Frequency.DAILY,
    )
    assert req.weekly_day is None


def test_subscription_request_pair_normalization():
    """Test pair name normalization."""
    req = SubscriptionRequest(
        email="test@example.com",
        pairs=["usd/cad", "eur_cad", "GBP_CAD"],
    )
    
    assert req.pairs == ["USD_CAD", "EUR_CAD", "GBP_CAD"]


def test_subscription_request_monthly():
    """Test monthly subscription request."""
    req = SubscriptionRequest(
        email="test@example.com",
        frequency=Frequency.MONTHLY,
        monthly_timing=MonthlyTiming.FIRST_BUSINESS_DAY,
    )
    
    assert req.frequency == Frequency.MONTHLY
    assert req.monthly_timing == MonthlyTiming.FIRST_BUSINESS_DAY
    assert req.weekly_day is None


def test_subscription_request_invalid_email():
    """Test that invalid email is rejected."""
    with pytest.raises(ValueError):
        SubscriptionRequest(email="not-an-email")


def test_subscription_request_empty_pairs():
    """Test that empty pairs list is rejected."""
    with pytest.raises(ValueError):
        SubscriptionRequest(email="test@example.com", pairs=[])

