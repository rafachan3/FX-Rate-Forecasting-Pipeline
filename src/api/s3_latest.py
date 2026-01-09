"""S3 loader and cache for latest predictions."""

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from src.api.config import config
from src.api.models import Direction, PredictionItem

# Simple in-memory cache with timestamps
_cache: dict[str, tuple[dict, float]] = {}

# Conditional boto3 import (only needed for S3 mode)
try:
    import boto3
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False


def _cache_key(path: str, is_local: bool = False) -> str:
    """Generate cache key from path (S3 or local)."""
    if is_local:
        return f"local:{path}"
    return f"s3:{config.S3_BUCKET}:{path}"


def _is_cache_valid(key: str) -> bool:
    """Check if cache entry is still valid."""
    if key not in _cache:
        return False
    _, timestamp = _cache[key]
    age = datetime.now(timezone.utc).timestamp() - timestamp
    return age < config.CACHE_TTL


def _get_from_cache(key: str) -> Optional[dict]:
    """Get value from cache if valid."""
    if _is_cache_valid(key):
        value, _ = _cache[key]
        return value
    return None


def _set_cache(key: str, value: dict) -> None:
    """Set cache value with current timestamp."""
    _cache[key] = (value, datetime.now(timezone.utc).timestamp())


def _get_s3_client():
    """Get boto3 S3 client."""
    if not BOTO3_AVAILABLE:
        raise RuntimeError("boto3 not available. Install with: pip install boto3")
    return boto3.client("s3", region_name=config.AWS_REGION)


def _load_manifest_local() -> dict:
    """Load manifest.json from local filesystem."""
    manifest_path = config.local_manifest_path
    
    if not os.path.exists(manifest_path):
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    
    with open(manifest_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_manifest_s3() -> dict:
    """Load manifest.json from S3."""
    s3_client = _get_s3_client()
    try:
        response = s3_client.get_object(
            Bucket=config.S3_BUCKET,
            Key=config.s3_manifest_path,
        )
        content = response["Body"].read().decode("utf-8")
        return json.loads(content)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "S3_READ_FAILED")
        raise RuntimeError(f"Failed to load manifest from S3: {error_code}") from e


def load_manifest() -> dict:
    """Load manifest.json (from S3 or local filesystem) with caching."""
    if config.is_local_mode:
        cache_key = _cache_key(config.local_manifest_path, is_local=True)
    else:
        cache_key = _cache_key(config.s3_manifest_path)
    
    # Check cache
    cached = _get_from_cache(cache_key)
    if cached is not None:
        return cached
    
    # Load from source
    if config.is_local_mode:
        manifest = _load_manifest_local()
    else:
        manifest = _load_manifest_s3()
    
    _set_cache(cache_key, manifest)
    return manifest


def _load_latest_json_local(pair: str) -> Optional[dict]:
    """Load latest_{pair}_h7.json from local filesystem."""
    json_path = config.local_latest_json_path(pair)
    
    if not os.path.exists(json_path):
        return None
    
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_latest_json_s3(pair: str) -> Optional[dict]:
    """Load latest_{pair}_h7.json from S3."""
    s3_client = _get_s3_client()
    try:
        response = s3_client.get_object(
            Bucket=config.S3_BUCKET,
            Key=config.s3_latest_json_path(pair),
        )
        content = response["Body"].read().decode("utf-8")
        return json.loads(content)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "NoSuchKey")
        if error_code == "NoSuchKey":
            return None
        raise RuntimeError(f"Failed to load latest JSON for {pair} from S3: {error_code}") from e


def load_latest_json(pair: str) -> Optional[dict]:
    """Load latest_{pair}_h7.json (from S3 or local filesystem) with caching."""
    if config.is_local_mode:
        cache_key = _cache_key(config.local_latest_json_path(pair), is_local=True)
    else:
        cache_key = _cache_key(config.s3_latest_json_path(pair))
    
    # Check cache
    cached = _get_from_cache(cache_key)
    if cached is not None:
        return cached
    
    # Load from source
    if config.is_local_mode:
        data = _load_latest_json_local(pair)
    else:
        data = _load_latest_json_s3(pair)
    
    if data is not None:
        _set_cache(cache_key, data)
    
    return data


def compute_confidence(p_up: float) -> float:
    """Compute confidence score from probability.
    
    Confidence is max(p_up, 1 - p_up), so 0.5 means low confidence,
    and values closer to 0 or 1 mean higher confidence.
    """
    return max(p_up, 1.0 - p_up)


def map_action_to_direction(action: str) -> Direction:
    """Map action_logreg to Direction enum."""
    action_upper = action.upper()
    if action_upper == "UP":
        return Direction.UP
    elif action_upper == "DOWN":
        return Direction.DOWN
    else:
        return Direction.ABSTAIN


def format_pair_label(pair: str) -> str:
    """Format pair code to label (e.g., USD_CAD -> USD/CAD)."""
    return pair.replace("_", "/")


def get_latest_predictions(
    pairs: list[str],
    limit: int = 1,
) -> list[PredictionItem]:
    """Get latest predictions for requested pairs.
    
    Args:
        pairs: List of pair codes (e.g., ["USD_CAD", "EUR_CAD"])
        limit: Number of rows per pair (currently only 1 is supported)
    
    Returns:
        List of PredictionItem, one per pair (latest row only)
    """
    items = []
    
    for pair in pairs:
        pair_upper = pair.upper().replace("/", "_")
        
        # Load latest JSON for this pair
        latest_data = load_latest_json(pair_upper)
        
        if latest_data is None:
            # Pair file missing - return ABSTAIN with placeholder
            items.append(
                PredictionItem(
                    pair=pair_upper,
                    pair_label=format_pair_label(pair_upper),
                    generated_at=datetime.now(timezone.utc).isoformat(),
                    obs_date="",
                    direction=Direction.ABSTAIN,
                    confidence=0.0,
                    model="logreg",
                    raw={"p_up": 0.5},
                )
            )
            continue
        
        # Extract rows and find latest by obs_date
        rows = latest_data.get("rows", [])
        if not rows:
            # Empty rows - return ABSTAIN
            items.append(
                PredictionItem(
                    pair=pair_upper,
                    pair_label=format_pair_label(pair_upper),
                    generated_at=latest_data.get("generated_at", ""),
                    obs_date="",
                    direction=Direction.ABSTAIN,
                    confidence=0.0,
                    model="logreg",
                    raw={"p_up": 0.5},
                )
            )
            continue
        
        # Sort by obs_date descending and take latest
        sorted_rows = sorted(rows, key=lambda r: r.get("obs_date", ""), reverse=True)
        latest_row = sorted_rows[0]
        
        # Extract fields
        obs_date = latest_row.get("obs_date", "")
        p_up = latest_row.get("p_up_logreg", 0.5)
        action = latest_row.get("action_logreg", "ABSTAIN")
        
        # Map to response model
        items.append(
            PredictionItem(
                pair=pair_upper,
                pair_label=format_pair_label(pair_upper),
                generated_at=latest_data.get("generated_at", ""),
                obs_date=obs_date,
                direction=map_action_to_direction(action),
                confidence=compute_confidence(p_up),
                model="logreg",
                raw={"p_up": p_up},
            )
        )
    
    return items


def get_manifest_metadata() -> dict:
    """Get metadata from manifest for response."""
    manifest = load_manifest()
    
    # Convert run_timestamp to UTC ISO if possible
    as_of_utc = None
    run_timestamp = manifest.get("run_timestamp")
    if run_timestamp:
        try:
            # Try to parse and convert to UTC
            dt = datetime.fromisoformat(run_timestamp.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                # Assume Toronto timezone if naive (use UTC offset approximation)
                # Note: For production, consider using pytz or zoneinfo if available
                dt = dt.replace(tzinfo=timezone.utc)  # Fallback to UTC if naive
            as_of_utc = dt.astimezone(timezone.utc).isoformat()
        except (ValueError, AttributeError):
            # If parsing fails, use as-is or None
            as_of_utc = run_timestamp
    
    return {
        "horizon": "h7",
        "as_of_utc": as_of_utc,
        "run_date": manifest.get("run_date", ""),
        "timezone": manifest.get("timezone", "America/Toronto"),
        "git_sha": manifest.get("git_sha", ""),
    }

