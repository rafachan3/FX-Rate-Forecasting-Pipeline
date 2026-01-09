"""Configuration for NorthBound API."""

import os
from typing import Optional


class Config:
    """Application configuration from environment variables."""
    
    # Environment
    STAGE: str = os.getenv("STAGE", "local")
    
    # S3 Configuration
    S3_BUCKET: str = os.getenv("S3_BUCKET", "fx-rate-pipeline-dev")
    S3_PREFIX: str = os.getenv("S3_PREFIX", "predictions/h7/latest/")
    
    # DynamoDB Configuration
    DDB_TABLE: str = os.getenv("DDB_TABLE", "northbound_subscriptions")
    AWS_REGION: str = os.getenv("AWS_REGION", "us-east-2")
    
    # CORS
    CORS_ORIGINS: list[str] = os.getenv("CORS_ORIGINS", "*").split(",")
    
    # Security
    SUBSCRIBE_API_KEY: Optional[str] = os.getenv("SUBSCRIBE_API_KEY")
    
    # Feature Flags
    EMAIL_ENABLED: bool = os.getenv("EMAIL_ENABLED", "false").lower() == "true"
    
    # Cache TTL (seconds)
    CACHE_TTL: int = int(os.getenv("CACHE_TTL", "60"))
    
    # Local dev mode (if set, load from filesystem instead of S3)
    LOCAL_LATEST_DIR: Optional[str] = os.getenv("LOCAL_LATEST_DIR") or None
    
    @property
    def is_local_mode(self) -> bool:
        """Check if running in local mode (filesystem instead of S3)."""
        return self.LOCAL_LATEST_DIR is not None and self.LOCAL_LATEST_DIR != ""
    
    @property
    def s3_manifest_path(self) -> str:
        """Full S3 path to manifest.json."""
        return f"{self.S3_PREFIX}manifest.json"
    
    def s3_latest_json_path(self, pair: str) -> str:
        """Full S3 path to latest_{pair}_h7.json."""
        return f"{self.S3_PREFIX}latest_{pair}_h7.json"
    
    @property
    def local_manifest_path(self) -> str:
        """Local filesystem path to manifest.json."""
        if not self.is_local_mode:
            raise ValueError("LOCAL_LATEST_DIR not set")
        return os.path.join(self.LOCAL_LATEST_DIR, "manifest.json")
    
    def local_latest_json_path(self, pair: str) -> str:
        """Local filesystem path to latest_{pair}_h7.json."""
        if not self.is_local_mode:
            raise ValueError("LOCAL_LATEST_DIR not set")
        return os.path.join(self.LOCAL_LATEST_DIR, f"latest_{pair}_h7.json")


config = Config()

