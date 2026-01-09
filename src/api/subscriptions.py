"""DynamoDB operations for subscriptions."""

import hashlib
from datetime import datetime, timezone
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from src.api.config import config
from src.api.models import Frequency, MonthlyTiming, SubscriptionRequest, WeeklyDay


def _get_ddb_client():
    """Get boto3 DynamoDB client."""
    return boto3.client("dynamodb", region_name=config.AWS_REGION)


def _normalize_email(email: str) -> str:
    """Normalize email to lowercase."""
    return email.lower().strip()


def _generate_subscription_id(email: str) -> str:
    """Generate deterministic subscription ID from email."""
    normalized = _normalize_email(email)
    hash_obj = hashlib.sha256(normalized.encode())
    return hash_obj.hexdigest()[:16]


def create_or_update_subscription(request: SubscriptionRequest) -> dict:
    """Create or update subscription in DynamoDB."""
    ddb_client = _get_ddb_client()
    normalized_email = _normalize_email(request.email)
    subscription_id = _generate_subscription_id(normalized_email)
    now = datetime.now(timezone.utc).isoformat()
    
    # Prepare item
    item = {
        "email": {"S": normalized_email},
        "subscription_id": {"S": subscription_id},
        "pairs": {"SS": request.pairs},
        "frequency": {"S": request.frequency.value},
        "status": {"S": "active"},
        "updated_at": {"S": now},
    }
    
    # Add optional fields
    if request.weekly_day:
        item["weekly_day"] = {"S": request.weekly_day.value}
    if request.monthly_timing:
        item["monthly_timing"] = {"S": request.monthly_timing.value}
    
    # Check if exists to set created_at
    try:
        existing = ddb_client.get_item(
            TableName=config.DDB_TABLE,
            Key={"email": {"S": normalized_email}},
        )
        if "Item" in existing:
            # Update existing
            item["created_at"] = existing["Item"].get("created_at", {"S": now})
        else:
            # New subscription
            item["created_at"] = {"S": now}
    except ClientError:
        # If get fails, assume new
        item["created_at"] = {"S": now}
    
    # Put item
    try:
        ddb_client.put_item(
            TableName=config.DDB_TABLE,
            Item=item,
        )
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "DDB_WRITE_FAILED")
        raise RuntimeError(f"Failed to save subscription: {error_code}") from e
    
    return {
        "status": "created_or_updated",
        "email": normalized_email,
        "subscription_id": subscription_id,
        "email_enabled": config.EMAIL_ENABLED,
        "message": (
            "Subscription saved. Email delivery is temporarily disabled."
            if not config.EMAIL_ENABLED
            else "Subscription saved."
        ),
    }


def unsubscribe(email: str) -> dict:
    """Unsubscribe email (set status to inactive)."""
    ddb_client = _get_ddb_client()
    normalized_email = _normalize_email(email)
    now = datetime.now(timezone.utc).isoformat()
    
    try:
        # Update status to inactive
        ddb_client.update_item(
            TableName=config.DDB_TABLE,
            Key={"email": {"S": normalized_email}},
            UpdateExpression="SET #status = :status, updated_at = :now",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": {"S": "inactive"},
                ":now": {"S": now},
            },
            ConditionExpression="attribute_exists(email)",
        )
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "DDB_UPDATE_FAILED")
        if error_code == "ConditionalCheckFailedException":
            # Email doesn't exist - still return success for idempotency
            return {
                "status": "unsubscribed",
                "email": normalized_email,
            }
        raise RuntimeError(f"Failed to unsubscribe: {error_code}") from e
    
    return {
        "status": "unsubscribed",
        "email": normalized_email,
    }

