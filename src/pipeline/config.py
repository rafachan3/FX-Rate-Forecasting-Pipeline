"""Strict pipeline configuration schema and loader."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import os

@dataclass
class SeriesConfig:
    """Configuration for a single FX series."""
    
    series_id: str
    gold_local_path: str
    
    def __post_init__(self) -> None:
        """Validate series configuration."""
        if not self.series_id or not isinstance(self.series_id, str):
            raise ValueError("series_id must be a non-empty string")
        if not self.gold_local_path or not isinstance(self.gold_local_path, str):
            raise ValueError("gold_local_path must be a non-empty string")


@dataclass
class S3Config:
    """S3 storage configuration."""
    
    bucket: str
    prefix_template: str
    filename: str
    profile: str | None = None
    
    def __post_init__(self) -> None:
        """Validate S3 configuration."""
        if not self.bucket or not isinstance(self.bucket, str):
            raise ValueError("bucket must be a non-empty string")
        if not self.prefix_template or not isinstance(self.prefix_template, str):
            raise ValueError("prefix_template must be a non-empty string")
        if "{series_id}" not in self.prefix_template:
            raise ValueError('prefix_template must contain "{series_id}" placeholder')
        if not self.filename or not isinstance(self.filename, str):
            raise ValueError("filename must be a non-empty string")
        if not self.filename.endswith(".parquet"):
            raise ValueError('filename must end with ".parquet"')
        if self.profile is not None and (not isinstance(self.profile, str) or not self.profile):
            raise ValueError("s3.profile must be None or a non-empty string")
    
    def s3_key_for_series(self, series_id: str) -> str:
        """
        Generate S3 key for a given series.
        
        Args:
            series_id: Series identifier
            
        Returns:
            S3 key (without bucket, e.g., "gold/source=BoC/series=FXUSDCAD/data.parquet")
        """
        prefix = self.prefix_template.format(series_id=series_id)
        return f"{prefix}{self.filename}"


@dataclass
class ArtifactsConfig:
    """Model artifacts configuration."""
    
    dir: str
    model_file: str
    features_file: str
    metadata_file: str
    
    def __post_init__(self) -> None:
        """Validate artifacts configuration."""
        if not self.dir or not isinstance(self.dir, str):
            raise ValueError("artifacts.dir must be a non-empty string")
        if not self.model_file or not isinstance(self.model_file, str):
            raise ValueError("artifacts.model_file must be a non-empty string")
        if not self.features_file or not isinstance(self.features_file, str):
            raise ValueError("artifacts.features_file must be a non-empty string")
        if not self.metadata_file or not isinstance(self.metadata_file, str):
            raise ValueError("artifacts.metadata_file must be a non-empty string")


@dataclass
class OutputsConfig:
    """Output directories configuration."""
    
    runs_dir: str
    latest_dir: str
    
    def __post_init__(self) -> None:
        """Validate outputs configuration."""
        if not self.runs_dir or not isinstance(self.runs_dir, str):
            raise ValueError("outputs.runs_dir must be a non-empty string")
        if not self.latest_dir or not isinstance(self.latest_dir, str):
            raise ValueError("outputs.latest_dir must be a non-empty string")


@dataclass
class PublishConfig:
    """S3 publishing configuration."""
    
    bucket: str
    profile: str | None
    prefix_runs_template: str
    prefix_latest: str
    
    def __post_init__(self) -> None:
        """Validate publish configuration."""
        if not self.bucket or not isinstance(self.bucket, str):
            raise ValueError("publish.bucket must be a non-empty string")
        if self.profile is not None and (not isinstance(self.profile, str) or not self.profile):
            raise ValueError("publish.profile must be None or a non-empty string")
        if not self.prefix_runs_template or not isinstance(self.prefix_runs_template, str):
            raise ValueError("publish.prefix_runs_template must be a non-empty string")
        if "{horizon}" not in self.prefix_runs_template:
            raise ValueError('publish.prefix_runs_template must contain "{horizon}" placeholder')
        if "{run_date}" not in self.prefix_runs_template:
            raise ValueError('publish.prefix_runs_template must contain "{run_date}" placeholder')
        if not self.prefix_latest or not isinstance(self.prefix_latest, str):
            raise ValueError("publish.prefix_latest must be a non-empty string")
        if "{horizon}" not in self.prefix_latest:
            raise ValueError('publish.prefix_latest must contain "{horizon}" placeholder')


@dataclass
class EmailConfig:
    """Email delivery configuration (SendGrid)."""
    
    api_key: str
    from_email: str
    to_emails: list[str]
    subject_template: str
    body_format: str = "text"
    
    def __post_init__(self) -> None:
        """Validate email configuration."""
        # Resolve environment variable for api_key if needed
        if self.api_key and self.api_key.startswith("${") and self.api_key.endswith("}"):
            env_var = self.api_key[2:-1]
            resolved_key = os.environ.get(env_var)
            if not resolved_key:
                raise ValueError(
                    f"Environment variable {env_var} not set for email.api_key. "
                    f"Set it or provide the API key directly."
                )
            object.__setattr__(self, 'api_key', resolved_key)
        
        if not self.api_key or not isinstance(self.api_key, str):
            raise ValueError("email.api_key must be a non-empty string")
        if not self.from_email or not isinstance(self.from_email, str):
            raise ValueError("email.from_email must be a non-empty string")
        if not isinstance(self.to_emails, list) or len(self.to_emails) == 0:
            raise ValueError("email.to_emails must be a non-empty list")
        for i, email in enumerate(self.to_emails):
            if not email or not isinstance(email, str):
                raise ValueError(f"email.to_emails[{i}] must be a non-empty string")
        if not self.subject_template or not isinstance(self.subject_template, str):
            raise ValueError("email.subject_template must be a non-empty string")
        if "{horizon}" not in self.subject_template:
            raise ValueError('email.subject_template must contain "{horizon}" placeholder')
        if "{run_date}" not in self.subject_template:
            raise ValueError('email.subject_template must contain "{run_date}" placeholder')
        if self.body_format not in ("text",):
            raise ValueError(f'email.body_format must be "text", got "{self.body_format}"')


@dataclass
class PipelineConfig:
    """Complete pipeline configuration."""
    
    horizon: str
    timezone: str
    series: list[SeriesConfig]
    s3: S3Config
    artifacts: ArtifactsConfig
    outputs: OutputsConfig
    publish: PublishConfig | None = None
    email: EmailConfig | None = None
    
    def __post_init__(self) -> None:
        """Validate pipeline configuration."""
        if self.horizon != "h7":
            raise ValueError(f'horizon must be "h7", got "{self.horizon}"')
        if self.timezone != "America/Toronto":
            raise ValueError(
                f'timezone must be "America/Toronto", got "{self.timezone}"'
            )
        if not self.series:
            raise ValueError("series must be a non-empty list")
    
    def s3_key_for_series(self, series_id: str) -> str:
        """
        Generate S3 key for a given series.
        
        Args:
            series_id: Series identifier
            
        Returns:
            S3 key (without bucket)
        """
        return self.s3.s3_key_for_series(series_id)


def _validate_no_unknown_keys(
    data: dict[str, Any],
    known_keys: set[str],
    context: str = "top-level",
) -> None:
    """
    Validate that no unknown keys are present.
    
    Args:
        data: Dictionary to validate
        known_keys: Set of known/expected keys
        context: Context string for error messages
        
    Raises:
        ValueError: If unknown keys are found
    """
    unknown = set(data.keys()) - known_keys
    if unknown:
        raise ValueError(
            f"Unknown keys in {context}: {sorted(unknown)}. "
            f"Known keys: {sorted(known_keys)}"
        )


def load_pipeline_config(path: str | Path) -> PipelineConfig:
    """
    Load and validate pipeline configuration from JSON file.
    
    Args:
        path: Path to JSON configuration file
        
    Returns:
        Validated PipelineConfig instance
        
    Raises:
        FileNotFoundError: If config file does not exist
        ValueError: If config is invalid or contains unknown keys
        json.JSONDecodeError: If JSON is malformed
    """
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, "r") as f:
        data = json.load(f)
    
    if not isinstance(data, dict):
        raise ValueError(f"Configuration must be a JSON object, got {type(data)}")
    
    # Validate top-level keys
    top_level_keys = {"horizon", "timezone", "series", "s3", "artifacts", "outputs", "publish", "email"}
    _validate_no_unknown_keys(data, top_level_keys, "top-level")
    
    # Validate horizon
    horizon = data.get("horizon")
    if horizon != "h7":
        raise ValueError(f'horizon must be "h7", got "{horizon}"')
    
    # Validate timezone
    timezone = data.get("timezone")
    if timezone != "America/Toronto":
        raise ValueError(f'timezone must be "America/Toronto", got "{timezone}"')
    
    # Validate and build series configs
    series_data = data.get("series", [])
    if not isinstance(series_data, list):
        raise ValueError("series must be a list")
    if not series_data:
        raise ValueError("series must be a non-empty list")
    
    series_configs = []
    for i, series_item in enumerate(series_data):
        if not isinstance(series_item, dict):
            raise ValueError(f"series[{i}] must be an object")
        series_keys = {"series_id", "gold_local_path"}
        _validate_no_unknown_keys(series_item, series_keys, f"series[{i}]")
        series_configs.append(
            SeriesConfig(
                series_id=series_item["series_id"],
                gold_local_path=series_item["gold_local_path"],
            )
        )
    
    # Validate and build S3 config
    s3_data = data.get("s3", {})
    if not isinstance(s3_data, dict):
        raise ValueError("s3 must be an object")
    s3_keys = {"bucket", "prefix_template", "filename", "profile"}
    _validate_no_unknown_keys(s3_data, s3_keys, "s3")
    # Handle null profile (JSON null becomes None in Python)
    s3_profile_value = s3_data.get("profile")

    s3_config = S3Config(
        bucket=s3_data["bucket"],
        prefix_template=s3_data["prefix_template"],
        filename=s3_data["filename"],
        profile=s3_profile_value,
    )
    
    # Validate and build artifacts config
    artifacts_data = data.get("artifacts", {})
    if not isinstance(artifacts_data, dict):
        raise ValueError("artifacts must be an object")
    artifacts_keys = {"dir", "model_file", "features_file", "metadata_file"}
    _validate_no_unknown_keys(artifacts_data, artifacts_keys, "artifacts")
    artifacts_config = ArtifactsConfig(
        dir=artifacts_data["dir"],
        model_file=artifacts_data["model_file"],
        features_file=artifacts_data["features_file"],
        metadata_file=artifacts_data["metadata_file"],
    )
    
    # Validate and build outputs config
    outputs_data = data.get("outputs", {})
    if not isinstance(outputs_data, dict):
        raise ValueError("outputs must be an object")
    outputs_keys = {"runs_dir", "latest_dir"}
    _validate_no_unknown_keys(outputs_data, outputs_keys, "outputs")
    outputs_config = OutputsConfig(
        runs_dir=outputs_data["runs_dir"],
        latest_dir=outputs_data["latest_dir"],
    )
    
    # Validate and build publish config (optional)
    publish_config = None
    if "publish" in data:
        publish_data = data.get("publish", {})
        if not isinstance(publish_data, dict):
            raise ValueError("publish must be an object")
        publish_keys = {"bucket", "profile", "prefix_runs_template", "prefix_latest"}
        _validate_no_unknown_keys(publish_data, publish_keys, "publish")
        # Handle null profile (JSON null becomes None in Python)
        profile_value = publish_data.get("profile")
        if profile_value is None:
            profile_value = None
        publish_config = PublishConfig(
            bucket=publish_data["bucket"],
            profile=profile_value,
            prefix_runs_template=publish_data["prefix_runs_template"],
            prefix_latest=publish_data["prefix_latest"],
        )
    
# Validate and build email config (optional)
    email_config = None
    if "email" in data:
        email_data = data.get("email", {})
        if not isinstance(email_data, dict):
            raise ValueError("email must be an object")
        email_keys = {"api_key", "from_email", "to_emails", "subject_template", "body_format"}
        _validate_no_unknown_keys(email_data, email_keys, "email")
        email_config = EmailConfig(
            api_key=email_data["api_key"],
            from_email=email_data["from_email"],
            to_emails=email_data["to_emails"],
            subject_template=email_data["subject_template"],
            body_format=email_data.get("body_format", "text"),
        )
    
    # Build and return pipeline config
    return PipelineConfig(
        horizon=horizon,
        timezone=timezone,
        series=series_configs,
        s3=s3_config,
        artifacts=artifacts_config,
        outputs=outputs_config,
        publish=publish_config,
        email=email_config,
    )

