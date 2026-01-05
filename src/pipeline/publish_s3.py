"""S3 publishing for pipeline outputs using AWS CLI."""
from __future__ import annotations

import subprocess
from pathlib import Path


def s3_uri(bucket: str, key: str) -> str:
    """
    Construct S3 URI from bucket and key.
    
    Args:
        bucket: S3 bucket name
        key: S3 key (path within bucket)
        
    Returns:
        S3 URI (e.g., "s3://bucket-name/path/to/file")
    """
    return f"s3://{bucket}/{key}"


def aws_s3_cp(local_path: str | Path, bucket: str, key: str, profile: str) -> None:
    """
    Upload a local file to S3 using AWS CLI.
    
    Args:
        local_path: Local file path to upload
        bucket: S3 bucket name
        key: S3 key (path within bucket)
        profile: AWS profile name
        
    Raises:
        RuntimeError: If AWS CLI command fails, including full command and stderr
    """
    local_path_obj = Path(local_path)
    if not local_path_obj.exists():
        raise FileNotFoundError(f"Local file not found: {local_path}")
    
    s3_dest = s3_uri(bucket, key)
    
    cmd = [
        "aws",
        "s3",
        "cp",
        str(local_path_obj),
        s3_dest,
        "--profile",
        profile,
        "--only-show-errors",
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    
    if result.returncode != 0:
        cmd_str = " ".join(cmd)
        raise RuntimeError(
            f"AWS S3 copy failed (exit code {result.returncode}):\n"
            f"Command: {cmd_str}\n"
            f"Stderr: {result.stderr}"
        )


def publish_run_outputs(
    *,
    run_dir: str | Path,
    horizon: str,
    run_date: str,
    bucket: str,
    profile: str,
    prefix_runs_template: str,
) -> None:
    """
    Publish run outputs (predictions and manifest) to S3.
    
    Args:
        run_dir: Local run directory containing decision_predictions_h7.parquet and manifest.json
        horizon: Horizon identifier (e.g., "h7")
        run_date: Run date in YYYY-MM-DD format
        bucket: S3 bucket name
        profile: AWS profile name
        prefix_runs_template: S3 prefix template with {horizon} and {run_date} placeholders
        
    Raises:
        FileNotFoundError: If required files are missing
        RuntimeError: If AWS CLI command fails
    """
    run_dir_obj = Path(run_dir)
    
    predictions_file = run_dir_obj / "decision_predictions_h7.parquet"
    manifest_file = run_dir_obj / "manifest.json"
    
    if not predictions_file.exists():
        raise FileNotFoundError(f"Predictions file not found: {predictions_file}")
    if not manifest_file.exists():
        raise FileNotFoundError(f"Manifest file not found: {manifest_file}")
    
    # Render prefix template
    prefix = prefix_runs_template.format(horizon=horizon, run_date=run_date)
    
    # Upload predictions
    predictions_key = f"{prefix}decision_predictions_h7.parquet"
    aws_s3_cp(predictions_file, bucket, predictions_key, profile)
    
    # Upload manifest
    manifest_key = f"{prefix}manifest.json"
    aws_s3_cp(manifest_file, bucket, manifest_key, profile)


def publish_latest_outputs(
    *,
    latest_dir: str | Path,
    horizon: str,
    bucket: str,
    profile: str,
    prefix_latest: str,
) -> None:
    """
    Publish latest outputs (predictions and manifest) to S3.
    
    Args:
        latest_dir: Local latest directory containing decision_predictions_h7.parquet and manifest.json
        horizon: Horizon identifier (e.g., "h7")
        bucket: S3 bucket name
        profile: AWS profile name
        prefix_latest: S3 prefix template with {horizon} placeholder
        
    Raises:
        FileNotFoundError: If required files are missing
        RuntimeError: If AWS CLI command fails
    """
    latest_dir_obj = Path(latest_dir)
    
    predictions_file = latest_dir_obj / "decision_predictions_h7.parquet"
    manifest_file = latest_dir_obj / "manifest.json"
    
    if not predictions_file.exists():
        raise FileNotFoundError(f"Predictions file not found: {predictions_file}")
    if not manifest_file.exists():
        raise FileNotFoundError(f"Manifest file not found: {manifest_file}")
    
    # Render prefix template
    prefix = prefix_latest.format(horizon=horizon)
    
    # Upload predictions
    predictions_key = f"{prefix}decision_predictions_h7.parquet"
    aws_s3_cp(predictions_file, bucket, predictions_key, profile)
    
    # Upload manifest
    manifest_key = f"{prefix}manifest.json"
    aws_s3_cp(manifest_file, bucket, manifest_key, profile)

