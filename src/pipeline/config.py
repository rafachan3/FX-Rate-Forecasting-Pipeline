"""Strict pipeline configuration schema and loader."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


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
class PipelineConfig:
    """Complete pipeline configuration."""
    
    horizon: str
    timezone: str
    series: list[SeriesConfig]
    s3: S3Config
    artifacts: ArtifactsConfig
    outputs: OutputsConfig
    
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
    top_level_keys = {"horizon", "timezone", "series", "s3", "artifacts", "outputs"}
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
    s3_keys = {"bucket", "prefix_template", "filename"}
    _validate_no_unknown_keys(s3_data, s3_keys, "s3")
    s3_config = S3Config(
        bucket=s3_data["bucket"],
        prefix_template=s3_data["prefix_template"],
        filename=s3_data["filename"],
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
    
    # Build and return pipeline config
    return PipelineConfig(
        horizon=horizon,
        timezone=timezone,
        series=series_configs,
        s3=s3_config,
        artifacts=artifacts_config,
        outputs=outputs_config,
    )

