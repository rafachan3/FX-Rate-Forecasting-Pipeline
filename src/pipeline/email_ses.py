"""Email delivery via Amazon SES for pipeline outputs."""
from __future__ import annotations

import json
from pathlib import Path

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    raise RuntimeError("boto3 required. Install with: pip install boto3")

try:
    import pandas as pd
except ImportError:
    raise RuntimeError("pandas required. Install with: pip install pandas")

from src.pipeline.config import EmailConfig


def build_email_subject(cfg: EmailConfig, horizon: str, run_date: str) -> str:
    """
    Build email subject from template.
    
    Args:
        cfg: Email configuration
        horizon: Horizon identifier (e.g., "h7")
        run_date: Run date in YYYY-MM-DD format
        
    Returns:
        Rendered subject string
    """
    return cfg.subject_template.format(horizon=horizon, run_date=run_date)


def build_email_body_text(
    *,
    horizon: str,
    run_date: str,
    latest_dir: str | Path,
    manifest_path: str | Path | None = None,
    publish_config: dict | None = None,
) -> str:
    """
    Build deterministic email body text from latest outputs.
    
    Args:
        horizon: Horizon identifier (e.g., "h7")
        run_date: Run date in YYYY-MM-DD format
        latest_dir: Path to latest outputs directory
        manifest_path: Optional path to manifest.json (if None, looks in latest_dir)
        publish_config: Optional dict with "runs_prefix" and "latest_prefix" if publish enabled
        
    Returns:
        Email body text (deterministic, sorted by series_id)
        
    Raises:
        FileNotFoundError: If required files are missing
        ValueError: If parquet doesn't meet contract (missing required columns)
    """
    # Convert latest_dir to Path if needed
    if not isinstance(latest_dir, (str, Path)):
        raise TypeError(f"latest_dir must be str or Path, got {type(latest_dir)}")
    latest_dir_obj = Path(latest_dir)
    predictions_file = latest_dir_obj / f"decision_predictions_{horizon}.parquet"
    
    if not predictions_file.exists():
        raise FileNotFoundError(
            f"Predictions file not found: {predictions_file}. "
            "Cannot build email body without latest predictions."
        )
    
    # Load predictions parquet
    df_pred = pd.read_parquet(predictions_file)
    
    # Enforce output contract: exact columns required
    REQUIRED_COLS = {"obs_date", "series_id", "p_up_logreg", "action_logreg"}
    missing_cols = REQUIRED_COLS - set(df_pred.columns)
    if missing_cols:
        raise ValueError(
            f"Predictions parquet missing required columns: {sorted(missing_cols)}. "
            f"Available columns: {sorted(df_pred.columns)}"
        )
    
    # Get latest obs_date per series_id (deterministic: sorted by series_id)
    # For each series, get the row with the maximum obs_date
    top_lines = []
    for series_id in sorted(df_pred["series_id"].unique()):
        series_df = df_pred[df_pred["series_id"] == series_id]
        latest_date = series_df["obs_date"].max()
        
        # Get the row(s) with latest obs_date (should be one, but take first if multiple)
        latest_row = series_df[series_df["obs_date"] == latest_date].iloc[0]
        
        p_up = latest_row["p_up_logreg"]
        action = latest_row["action_logreg"]
        date_str = pd.Timestamp(latest_date).strftime("%Y-%m-%d")
        
        top_lines.append(f"  {series_id}: {action} (p={p_up:.3f}, date={date_str})")
    
    # Load manifest if available
    if manifest_path is None:
        manifest_path_obj = latest_dir_obj / "manifest.json"
    else:
        if not isinstance(manifest_path, (str, Path)):
            raise TypeError(f"manifest_path must be str, Path, or None, got {type(manifest_path)}")
        manifest_path_obj = Path(manifest_path)
    
    manifest_info = ""
    if manifest_path_obj.exists():
        with open(manifest_path_obj) as f:
            manifest = json.load(f)
        
        manifest_parts = []
        if "model_artifacts" in manifest:
            artifacts = manifest["model_artifacts"]
            if "files" in artifacts:
                # Try to extract version from metadata if available
                metadata_file = None
                for fname, fpath in artifacts["files"].items():
                    if "metadata" in fname.lower():
                        metadata_file = fpath
                        break
                
                if metadata_file:
                    # metadata_file from manifest is a string path, convert to Path
                    try:
                        metadata_path = Path(str(metadata_file))
                        if metadata_path.exists():
                            with open(metadata_path) as mf:
                                metadata = json.load(mf)
                                if "version" in metadata:
                                    manifest_parts.append(f"Model version: {metadata['version']}")
                    except Exception:
                        pass
        
        if "predictions" in manifest and "by_series_rows" in manifest["predictions"]:
            by_series = manifest["predictions"]["by_series_rows"]
            # Sort keys for determinism
            sorted_series = sorted(by_series.keys())
            row_counts = ", ".join(f"{s}: {by_series[s]} rows" for s in sorted_series)
            manifest_parts.append(f"Row counts: {row_counts}")
        
        if manifest_parts:
            manifest_info = "\n".join(manifest_parts) + "\n"
    
    # Build body text
    body_parts = [
        f"FX Signal Pipeline — {horizon}",
        f"Run date: {run_date}",
        "",
        "Latest signals (by series):",
    ]
    body_parts.extend(top_lines)
    body_parts.append("")
    
    # Add manifest info if available
    if manifest_info:
        body_parts.append("Manifest:")
        body_parts.append(manifest_info)
    
    # Add local paths
    body_parts.append("Local outputs:")
    body_parts.append(f"  Run directory: outputs/runs/{run_date}/")
    body_parts.append(f"  Latest directory: {str(latest_dir_obj)}")
    body_parts.append("")
    
    # Add S3 locations if publish enabled
    if publish_config:
        runs_prefix = publish_config.get("runs_prefix", "")
        latest_prefix = publish_config.get("latest_prefix", "")
        if runs_prefix or latest_prefix:
            body_parts.append("S3 locations:")
            if runs_prefix:
                body_parts.append(f"  Runs: s3://{publish_config.get('bucket', '')}/{runs_prefix}")
            if latest_prefix:
                body_parts.append(f"  Latest: s3://{publish_config.get('bucket', '')}/{latest_prefix}")
            body_parts.append("")
    
    return "\n".join(body_parts)


def send_email_ses(cfg: EmailConfig, subject: str, body_text: str) -> None:
    """
    Send email via Amazon SES.
    
    Args:
        cfg: Email configuration
        subject: Email subject
        body_text: Email body text
        
    Raises:
        RuntimeError: If SES send fails, including error code/message and from/to addresses
    """
    # Create boto3 session
    if cfg.aws_profile:
        session = boto3.Session(profile_name=cfg.aws_profile, region_name=cfg.region)
    else:
        session = boto3.Session(region_name=cfg.region)
    
    # Try SESv2 first, fallback to SES v1
    try:
        client = session.client("sesv2")
        response = client.send_email(
            FromEmailAddress=cfg.from_email,
            Destination={"ToAddresses": cfg.to_emails},
            Content={
                "Simple": {
                    "Subject": {"Data": subject, "Charset": "UTF-8"},
                    "Body": {"Text": {"Data": body_text, "Charset": "UTF-8"}},
                }
            },
        )
    except ClientError as e:
        # Fallback to SES v1 if SESv2 not available
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        if error_code == "InvalidAction" or "sesv2" in str(e).lower():
            try:
                client = session.client("ses")
                response = client.send_email(
                    Source=cfg.from_email,
                    Destination={"ToAddresses": cfg.to_emails},
                    Message={
                        "Subject": {"Data": subject, "Charset": "UTF-8"},
                        "Body": {"Text": {"Data": body_text, "Charset": "UTF-8"}},
                    },
                )
            except ClientError as e2:
                error_code = e2.response.get("Error", {}).get("Code", "Unknown")
                error_msg = e2.response.get("Error", {}).get("Message", str(e2))
                raise RuntimeError(
                    f"Failed to send email via SES (v1): {error_code}: {error_msg}. "
                    f"From: {cfg.from_email}, To: {cfg.to_emails}"
                ) from e2
        else:
            error_msg = e.response.get("Error", {}).get("Message", str(e))
            raise RuntimeError(
                f"Failed to send email via SES (v2): {error_code}: {error_msg}. "
                f"From: {cfg.from_email}, To: {cfg.to_emails}"
            ) from e

