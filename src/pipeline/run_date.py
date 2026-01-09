"""Deterministic run date utilities for Toronto timezone."""
from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

TORONTO_TZ = ZoneInfo("America/Toronto")


def toronto_today(now: datetime | None = None) -> date:
    """
    Get today's date in America/Toronto timezone.
    
    Args:
        now: Optional datetime to use as reference. If None, uses current time.
            Must be timezone-aware if provided. Raises ValueError if naive.
    
    Returns:
        Current date in Toronto timezone.
    
    Raises:
        ValueError: If now is provided but is naive (not timezone-aware).
    """
    if now is None:
        now = datetime.now(tz=TORONTO_TZ)
    elif now.tzinfo is None:
        raise ValueError("now must be timezone-aware, got naive datetime")
    
    return now.astimezone(TORONTO_TZ).date()


def toronto_now_iso(now: datetime | None = None) -> str:
    """
    Get current datetime as ISO string in America/Toronto timezone.
    
    Args:
        now: Optional datetime to use as reference. If None, uses current time.
            Must be timezone-aware if provided. Raises ValueError if naive.
    
    Returns:
        ISO format string with timezone offset (e.g., "2024-01-15T14:30:00-05:00").
    
    Raises:
        ValueError: If now is provided but is naive (not timezone-aware).
    """
    if now is None:
        now = datetime.now(tz=TORONTO_TZ)
    elif now.tzinfo is None:
        raise ValueError("now must be timezone-aware, got naive datetime")
    
    toronto_dt = now.astimezone(TORONTO_TZ)
    return toronto_dt.isoformat()

