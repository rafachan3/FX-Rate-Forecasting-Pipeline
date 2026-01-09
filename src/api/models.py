"""Pydantic models for API request/response validation."""

from datetime import datetime
from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, EmailStr, Field, field_validator, model_validator


class Frequency(str, Enum):
    """Delivery frequency options."""
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"


class WeeklyDay(str, Enum):
    """Weekday options."""
    MON = "MON"
    TUE = "TUE"
    WED = "WED"
    THU = "THU"
    FRI = "FRI"


class MonthlyTiming(str, Enum):
    """Monthly delivery timing options."""
    FIRST_BUSINESS_DAY = "FIRST_BUSINESS_DAY"
    LAST_BUSINESS_DAY = "LAST_BUSINESS_DAY"


class Direction(str, Enum):
    """Signal direction."""
    UP = "UP"
    DOWN = "DOWN"
    ABSTAIN = "ABSTAIN"


# Request Models
class SubscriptionRequest(BaseModel):
    """Subscription creation/update request."""
    email: EmailStr
    pairs: list[str] = Field(default=["USD_CAD", "EUR_CAD"], min_length=1)
    frequency: Frequency = Frequency.WEEKLY
    weekly_day: Optional[WeeklyDay] = Field(default=None)
    monthly_timing: Optional[MonthlyTiming] = Field(default=None)
    
    @field_validator("pairs")
    @classmethod
    def validate_pairs(cls, v: list[str]) -> list[str]:
        """Normalize pair names to uppercase."""
        return [p.upper().replace("/", "_") for p in v]
    
    @model_validator(mode="after")
    def set_default_weekly_day(self) -> "SubscriptionRequest":
        """Set default weekly_day if frequency is WEEKLY and value is None."""
        if self.frequency == Frequency.WEEKLY and self.weekly_day is None:
            self.weekly_day = WeeklyDay.WED
        return self


class UnsubscribeRequest(BaseModel):
    """Unsubscribe request."""
    email: EmailStr


# Response Models
class HealthResponse(BaseModel):
    """Health check response."""
    ok: bool
    service: str
    env: str
    time_utc: str
    email_enabled: bool


class PredictionItem(BaseModel):
    """Single prediction item."""
    pair: str
    pair_label: str
    generated_at: str
    obs_date: str
    direction: Direction
    confidence: float = Field(ge=0.0, le=1.0)
    model: Literal["logreg"] = "logreg"
    raw: dict[str, float]


class PredictionsResponse(BaseModel):
    """Predictions response."""
    horizon: str
    as_of_utc: Optional[str]
    run_date: str
    timezone: str
    git_sha: str
    items: list[PredictionItem]


class SubscriptionResponse(BaseModel):
    """Subscription response."""
    status: str
    email: str
    subscription_id: str
    email_enabled: bool
    message: str


class UnsubscribeResponse(BaseModel):
    """Unsubscribe response."""
    status: str
    email: str


class ErrorDetail(BaseModel):
    """Error detail."""
    code: str
    message: str
    request_id: Optional[str] = None


class ErrorResponse(BaseModel):
    """Error response envelope."""
    error: ErrorDetail

