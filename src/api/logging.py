"""Structured logging for NorthBound API."""

import json
import logging
import time
from typing import Any, Optional

from src.api.config import config

# Configure root logger
logger = logging.getLogger("northbound_api")
logger.setLevel(logging.INFO)

# Remove default handlers
logger.handlers.clear()

# Add JSON formatter handler
handler = logging.StreamHandler()


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logs."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # Add extra fields
        if hasattr(record, "request_id"):
            log_data["request_id"] = record.request_id
        if hasattr(record, "method"):
            log_data["method"] = record.method
        if hasattr(record, "path"):
            log_data["path"] = record.path
        if hasattr(record, "status"):
            log_data["status"] = record.status
        if hasattr(record, "duration_ms"):
            log_data["duration_ms"] = record.duration_ms
        if hasattr(record, "cache_hit"):
            log_data["cache_hit"] = record.cache_hit
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_data)


handler.setFormatter(JSONFormatter())
logger.addHandler(handler)


def log_request(
    method: str,
    path: str,
    status: int,
    duration_ms: float,
    request_id: Optional[str] = None,
    cache_hit: Optional[bool] = None,
    **kwargs: Any,
) -> None:
    """Log HTTP request with structured data."""
    extra = {
        "method": method,
        "path": path,
        "status": status,
        "duration_ms": round(duration_ms, 2),
    }
    if request_id:
        extra["request_id"] = request_id
    if cache_hit is not None:
        extra["cache_hit"] = cache_hit
    extra.update(kwargs)
    
    logger.info(f"{method} {path} {status}", extra=extra)


def log_error(
    message: str,
    error_code: str,
    request_id: Optional[str] = None,
    exc_info: Optional[Exception] = None,
) -> None:
    """Log error with structured data."""
    extra = {"error_code": error_code}
    if request_id:
        extra["request_id"] = request_id
    
    logger.error(message, extra=extra, exc_info=exc_info)

