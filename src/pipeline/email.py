"""Email delivery via SendGrid for pipeline outputs."""
from __future__ import annotations

import json
from pathlib import Path

try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail
except ImportError:
    raise RuntimeError("sendgrid required. Install with: pip install sendgrid")

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
    if not isinstance(latest_dir, (str, Path)):
        raise TypeError(f"latest_dir must be str or Path, got {type(latest_dir)}")
    latest_dir_obj = Path(latest_dir)
    predictions_file = latest_dir_obj / f"decision_predictions_{horizon}.parquet"
    
    if not predictions_file.exists():
        raise FileNotFoundError(
            f"Predictions file not found: {predictions_file}. "
            "Cannot build email body without latest predictions."
        )
    
    df_pred = pd.read_parquet(predictions_file)
    
    REQUIRED_COLS = {"obs_date", "series_id", "p_up_logreg", "action_logreg"}
    missing_cols = REQUIRED_COLS - set(df_pred.columns)
    if missing_cols:
        raise ValueError(
            f"Predictions parquet missing required columns: {sorted(missing_cols)}. "
            f"Available columns: {sorted(df_pred.columns)}"
        )
    
    top_lines = []
    for series_id in sorted(df_pred["series_id"].unique()):
        series_df = df_pred[df_pred["series_id"] == series_id]
        latest_date = series_df["obs_date"].max()
        latest_row = series_df[series_df["obs_date"] == latest_date].iloc[0]
        
        p_up = latest_row["p_up_logreg"]
        action = latest_row["action_logreg"]
        date_str = pd.Timestamp(latest_date).strftime("%Y-%m-%d")
        
        top_lines.append(f"  {series_id}: {action} (p={p_up:.3f}, date={date_str})")
    
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
                metadata_file = None
                for fname, fpath in artifacts["files"].items():
                    if "metadata" in fname.lower():
                        metadata_file = fpath
                        break
                
                if metadata_file:
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
            sorted_series = sorted(by_series.keys())
            row_counts = ", ".join(f"{s}: {by_series[s]} rows" for s in sorted_series)
            manifest_parts.append(f"Row counts: {row_counts}")
        
        if manifest_parts:
            manifest_info = "\n".join(manifest_parts) + "\n"
    
    body_parts = [
        f"FX Signal Pipeline — {horizon}",
        f"Run date: {run_date}",
        "",
        "Latest signals (by series):",
    ]
    body_parts.extend(top_lines)
    body_parts.append("")
    
    if manifest_info:
        body_parts.append("Manifest:")
        body_parts.append(manifest_info)
    
    body_parts.append("Local outputs:")
    body_parts.append(f"  Run directory: outputs/runs/{run_date}/")
    body_parts.append(f"  Latest directory: {str(latest_dir_obj)}")
    body_parts.append("")
    
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


def send_email(cfg: EmailConfig, subject: str, body_text: str) -> None:
    """
    Send email via SendGrid.
    
    Args:
        cfg: Email configuration
        subject: Email subject
        body_text: Email body text
        
    Raises:
        RuntimeError: If SendGrid send fails
        ValueError: If api_key is not configured
    """
    if not cfg.api_key:
        raise ValueError(
            "SendGrid API key not configured. Set email.api_key in config or "
            "SENDGRID_API_KEY environment variable."
        )
    
    message = Mail(
        from_email=cfg.from_email,
        to_emails=cfg.to_emails,
        subject=subject,
        plain_text_content=body_text,
    )
    
    try:
        sg = SendGridAPIClient(cfg.api_key)
        response = sg.send(message)
        
        if response.status_code >= 300:
            raise RuntimeError(
                f"SendGrid returned non-success status: {response.status_code}. "
                f"Body: {response.body}. "
                f"From: {cfg.from_email}, To: {cfg.to_emails}"
            )
            
    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        raise RuntimeError(
            f"Failed to send email via SendGrid: {type(e).__name__}: {str(e)}. "
            f"From: {cfg.from_email}, To: {cfg.to_emails}"
        ) from e
