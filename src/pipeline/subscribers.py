"""Subscriber management for email delivery from Neon PostgreSQL database."""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from typing import List, Optional

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    raise RuntimeError("psycopg2 required. Install with: pip install psycopg2-binary")


@dataclass
class Subscriber:
    """Active subscriber with their preferences."""
    
    id: int
    email: str
    unsubscribe_token: str
    frequency: str  # DAILY, WEEKLY, MONTHLY
    weekly_day: Optional[str]  # MON, TUE, WED, THU, FRI (for WEEKLY)
    monthly_timing: Optional[str]  # FIRST_BUSINESS_DAY, LAST_BUSINESS_DAY (for MONTHLY)
    pairs: List[str]  # List of FX pairs like ['FXUSDCAD', 'FXEURCAD']
    timezone: str


def get_database_url() -> str:
    """
    Get database URL from environment variable.
    
    Returns:
        Database connection URL
        
    Raises:
        ValueError: If POSTGRES_URL is not set
    """
    # Use POSTGRES_URL (Vercel/Neon standard) with DATABASE_URL as fallback
    db_url = os.environ.get("POSTGRES_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise ValueError(
            "POSTGRES_URL environment variable not set. "
            "Set it to your Neon PostgreSQL connection string."
        )
    return db_url


def _get_connection():
    """Get PostgreSQL database connection."""
    db_url = get_database_url()
    return psycopg2.connect(db_url, cursor_factory=RealDictCursor)


def _day_of_week_matches(target_day: str, check_date: date) -> bool:
    """
    Check if the check_date matches the target day of week.
    
    Args:
        target_day: Day abbreviation (MON, TUE, WED, THU, FRI)
        check_date: Date to check
        
    Returns:
        True if the day matches
    """
    day_map = {
        "MON": 0,
        "TUE": 1,
        "WED": 2,
        "THU": 3,
        "FRI": 4,
    }
    return check_date.weekday() == day_map.get(target_day, -1)


def _is_first_business_day(check_date: date) -> bool:
    """
    Check if the date is the first business day of the month.
    
    Args:
        check_date: Date to check
        
    Returns:
        True if it's the first business day (Mon-Fri) of the month
    """
    # First, check if it's a weekday
    if check_date.weekday() >= 5:  # Saturday or Sunday
        return False
    
    # Check if it's the first weekday of the month
    first_of_month = check_date.replace(day=1)
    
    # Find the first business day
    first_business_day = first_of_month
    while first_business_day.weekday() >= 5:  # Skip weekend
        first_business_day = first_business_day.replace(day=first_business_day.day + 1)
    
    return check_date == first_business_day


def _is_last_business_day(check_date: date) -> bool:
    """
    Check if the date is the last business day of the month.
    
    Args:
        check_date: Date to check
        
    Returns:
        True if it's the last business day (Mon-Fri) of the month
    """
    # First, check if it's a weekday
    if check_date.weekday() >= 5:  # Saturday or Sunday
        return False
    
    # Find the last day of the month
    if check_date.month == 12:
        next_month_first = check_date.replace(year=check_date.year + 1, month=1, day=1)
    else:
        next_month_first = check_date.replace(month=check_date.month + 1, day=1)
    
    from datetime import timedelta
    last_of_month = next_month_first - timedelta(days=1)
    
    # Find the last business day
    last_business_day = last_of_month
    while last_business_day.weekday() >= 5:  # Skip weekend
        last_business_day = last_business_day - timedelta(days=1)
    
    return check_date == last_business_day


def should_send_today(subscriber: Subscriber, run_date: date) -> bool:
    """
    Determine if an email should be sent to a subscriber today based on their frequency.
    
    Args:
        subscriber: Subscriber with frequency preferences
        run_date: The date of the pipeline run
        
    Returns:
        True if the subscriber should receive an email today
    """
    # Skip weekends for all frequencies (no signals on weekends)
    if run_date.weekday() >= 5:
        return False
    
    if subscriber.frequency == "DAILY":
        return True
    
    elif subscriber.frequency == "WEEKLY":
        if not subscriber.weekly_day:
            # Default to Friday if not specified
            return run_date.weekday() == 4
        return _day_of_week_matches(subscriber.weekly_day, run_date)
    
    elif subscriber.frequency == "MONTHLY":
        if subscriber.monthly_timing == "FIRST_BUSINESS_DAY":
            return _is_first_business_day(run_date)
        elif subscriber.monthly_timing == "LAST_BUSINESS_DAY":
            return _is_last_business_day(run_date)
        else:
            # Default to first business day if not specified
            return _is_first_business_day(run_date)
    
    return False


def fetch_active_subscribers(run_date: Optional[date] = None) -> List[Subscriber]:
    """
    Fetch all active subscribers from the database.
    
    Args:
        run_date: Optional date to filter by frequency (if None, returns all active)
        
    Returns:
        List of active Subscriber objects
    """
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    s.id,
                    s.email,
                    s.unsubscribe_token,
                    sp.frequency,
                    sp.weekly_day,
                    sp.monthly_timing,
                    sp.pairs,
                    sp.timezone
                FROM subscriptions s
                JOIN subscription_preferences sp ON s.id = sp.subscription_id
                WHERE s.is_active = true
                ORDER BY s.email
            """)
            
            rows = cur.fetchall()
            
            subscribers = []
            for row in rows:
                subscriber = Subscriber(
                    id=row["id"],
                    email=row["email"],
                    unsubscribe_token=row["unsubscribe_token"],
                    frequency=row["frequency"],
                    weekly_day=row["weekly_day"],
                    monthly_timing=row["monthly_timing"],
                    pairs=row["pairs"] if row["pairs"] else [],
                    timezone=row["timezone"] or "America/Toronto",
                )
                subscribers.append(subscriber)
            
            return subscribers
            
    finally:
        conn.close()


def fetch_subscribers_for_today(run_date: date) -> List[Subscriber]:
    """
    Fetch active subscribers who should receive an email today based on their frequency.
    
    Args:
        run_date: The date of the pipeline run
        
    Returns:
        List of Subscriber objects who should receive an email today
    """
    all_subscribers = fetch_active_subscribers()
    
    return [
        sub for sub in all_subscribers
        if should_send_today(sub, run_date)
    ]


def generate_unsubscribe_url(base_url: str, token: str) -> str:
    """
    Generate the unsubscribe URL for a subscriber.
    
    Args:
        base_url: Base URL of the website (e.g., "https://northbound-fx.com")
        token: Subscriber's unsubscribe token
        
    Returns:
        Full unsubscribe URL
    """
    return f"{base_url}/unsubscribe?token={token}"
