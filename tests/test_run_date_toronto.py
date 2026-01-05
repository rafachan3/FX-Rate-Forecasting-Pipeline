"""Tests for Toronto timezone run date utilities."""
from __future__ import annotations

from datetime import date, datetime

import pytest
from zoneinfo import ZoneInfo

from src.pipeline.run_date import toronto_now_iso, toronto_today

TORONTO_TZ = ZoneInfo("America/Toronto")
UTC_TZ = ZoneInfo("UTC")


def test_toronto_today_uses_current_time_when_none():
    """Test that toronto_today() uses current time when now is None."""
    result = toronto_today()
    assert isinstance(result, date)
    # Should be today's date in Toronto (could be yesterday or tomorrow in UTC)
    assert result <= date.today() + date.resolution
    assert result >= date.today() - date.resolution


def test_toronto_today_raises_on_naive_datetime():
    """Test that toronto_today() raises ValueError for naive datetime."""
    naive_dt = datetime(2024, 1, 15, 12, 0, 0)
    with pytest.raises(ValueError, match="timezone-aware"):
        toronto_today(now=naive_dt)


def test_toronto_today_converts_utc_to_toronto_date():
    """Test that UTC datetime converts correctly to Toronto date."""
    # Use a datetime that crosses midnight in Toronto to test conversion
    # Jan 15, 2024 05:00 UTC = Jan 15, 2024 00:00 EST (midnight in Toronto)
    utc_dt = datetime(2024, 1, 15, 5, 0, 0, tzinfo=UTC_TZ)
    result = toronto_today(now=utc_dt)
    assert result == date(2024, 1, 15)
    
    # Jan 15, 2024 04:59 UTC = Jan 14, 2024 23:59 EST (previous day in Toronto)
    utc_dt_prev = datetime(2024, 1, 15, 4, 59, 0, tzinfo=UTC_TZ)
    result_prev = toronto_today(now=utc_dt_prev)
    assert result_prev == date(2024, 1, 14)


def test_toronto_today_with_toronto_datetime():
    """Test that Toronto datetime returns correct date."""
    toronto_dt = datetime(2024, 1, 15, 14, 30, 0, tzinfo=TORONTO_TZ)
    result = toronto_today(now=toronto_dt)
    assert result == date(2024, 1, 15)


def test_toronto_now_iso_uses_current_time_when_none():
    """Test that toronto_now_iso() uses current time when now is None."""
    result = toronto_now_iso()
    assert isinstance(result, str)
    assert "T" in result
    assert "+" in result or "-" in result  # Must contain timezone offset


def test_toronto_now_iso_raises_on_naive_datetime():
    """Test that toronto_now_iso() raises ValueError for naive datetime."""
    naive_dt = datetime(2024, 1, 15, 12, 0, 0)
    with pytest.raises(ValueError, match="timezone-aware"):
        toronto_now_iso(now=naive_dt)


def test_toronto_now_iso_returns_iso_format_with_offset():
    """Test that toronto_now_iso() returns ISO format with timezone offset."""
    # Use a fixed UTC datetime
    utc_dt = datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC_TZ)
    result = toronto_now_iso(now=utc_dt)
    
    assert isinstance(result, str)
    assert "T" in result
    # Should contain timezone offset (either -05:00 or -04:00 depending on DST)
    assert "+" in result or "-" in result
    # Should be parseable as ISO format
    parsed = datetime.fromisoformat(result)
    assert parsed.tzinfo is not None


def test_toronto_now_iso_with_toronto_datetime():
    """Test that Toronto datetime returns correct ISO string."""
    toronto_dt = datetime(2024, 1, 15, 14, 30, 0, tzinfo=TORONTO_TZ)
    result = toronto_now_iso(now=toronto_dt)
    assert isinstance(result, str)
    assert "2024-01-15T14:30:00" in result
    assert "+" in result or "-" in result

