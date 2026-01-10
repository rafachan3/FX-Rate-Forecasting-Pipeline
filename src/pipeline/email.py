"""Email delivery via SendGrid for pipeline outputs."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.pipeline.config import EmailConfig


# Currency pair display names
CURRENCY_NAMES = {
    "FXUSDCAD": ("USD", "CAD"),
    "FXEURCAD": ("EUR", "CAD"),
    "FXGBPCAD": ("GBP", "CAD"),
    "FXJPYCAD": ("JPY", "CAD"),
    "FXCHFCAD": ("CHF", "CAD"),
    "FXAUDCAD": ("AUD", "CAD"),
    "FXNZDCAD": ("NZD", "CAD"),
}

# Horizon display names
HORIZON_DISPLAY = {
    "h7": "7-Day",
    "h14": "14-Day",
    "h30": "30-Day",
}


def _format_currency_pair(series_id: str) -> str:
    """Format series_id into readable currency pair."""
    if series_id in CURRENCY_NAMES:
        base, quote = CURRENCY_NAMES[series_id]
        return f"{base}/{quote}"
    # Fallback: extract from FX prefix
    if series_id.startswith("FX") and len(series_id) >= 8:
        base = series_id[2:5]
        quote = series_id[5:8]
        return f"{base}/{quote}"
    return series_id


def _get_signal_description(action: str, p_up: float) -> tuple[str, str]:
    """Get human-readable signal description and confidence level."""
    if action == "HOLD":
        return "Neutral", "Low"
    
    direction = "Bullish" if action == "UP" else "Bearish"
    
    # Confidence based on probability distance from 0.5
    confidence_score = abs(p_up - 0.5) * 2  # Scale to 0-1
    if confidence_score >= 0.4:
        confidence = "High"
    elif confidence_score >= 0.2:
        confidence = "Moderate"
    else:
        confidence = "Low"
    
    return direction, confidence


def _format_date_readable(run_date: str) -> str:
    """Format date as readable string."""
    try:
        dt = datetime.strptime(run_date, "%Y-%m-%d")
        return dt.strftime("%B %d, %Y")  # e.g., "January 15, 2024"
    except ValueError:
        return run_date


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


def _extract_predictions_data(
    predictions_file: Path,
) -> list[dict]:
    """
    Extract latest prediction data for each series.
    
    Returns:
        List of dicts with series_id, p_up, action, date_str, sorted by series_id
    """
    df_pred = pd.read_parquet(predictions_file)
    
    REQUIRED_COLS = {"obs_date", "series_id", "p_up_logreg", "action_logreg"}
    missing_cols = REQUIRED_COLS - set(df_pred.columns)
    if missing_cols:
        raise ValueError(
            f"Predictions parquet missing required columns: {sorted(missing_cols)}. "
            f"Available columns: {sorted(df_pred.columns)}"
        )
    
    predictions = []
    for series_id in sorted(df_pred["series_id"].unique()):
        series_df = df_pred[df_pred["series_id"] == series_id]
        latest_date = series_df["obs_date"].max()
        latest_row = series_df[series_df["obs_date"] == latest_date].iloc[0]
        
        predictions.append({
            "series_id": series_id,
            "p_up": float(latest_row["p_up_logreg"]),
            "action": str(latest_row["action_logreg"]),
            "date_str": pd.Timestamp(latest_date).strftime("%Y-%m-%d"),
        })
    
    return predictions


def _extract_manifest_data(manifest_path: Path) -> dict:
    """Extract relevant data from manifest file."""
    if not manifest_path.exists():
        return {}
    
    with open(manifest_path) as f:
        manifest = json.load(f)
    
    result = {}
    
    # Extract model version
    if "model_artifacts" in manifest:
        artifacts = manifest["model_artifacts"]
        if "files" in artifacts:
            for fname, fpath in artifacts["files"].items():
                if "metadata" in fname.lower():
                    try:
                        metadata_path = Path(str(fpath))
                        if metadata_path.exists():
                            with open(metadata_path) as mf:
                                metadata = json.load(mf)
                                if "version" in metadata:
                                    result["model_version"] = metadata["version"]
                    except Exception:
                        pass
                    break
    
    # Extract row counts
    if "predictions" in manifest and "by_series_rows" in manifest["predictions"]:
        result["row_counts"] = manifest["predictions"]["by_series_rows"]
    
    return result


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
    
    # Extract prediction data
    predictions = _extract_predictions_data(predictions_file)
    
    # Extract manifest data
    if manifest_path is None:
        manifest_path_obj = latest_dir_obj / "manifest.json"
    else:
        if not isinstance(manifest_path, (str, Path)):
            raise TypeError(f"manifest_path must be str, Path, or None, got {type(manifest_path)}")
        manifest_path_obj = Path(manifest_path)
    
    manifest_data = _extract_manifest_data(manifest_path_obj)
    
    # Build the email body
    horizon_display = HORIZON_DISPLAY.get(horizon, horizon.upper())
    readable_date = _format_date_readable(run_date)
    
    lines = [
        "═" * 50,
        f"  FX FORECAST SIGNALS — {horizon_display} Horizon",
        f"  Generated: {readable_date}",
        "═" * 50,
        "",
    ]
    
    # Signal summary section
    lines.append("📊 TODAY'S SIGNALS")
    lines.append("─" * 50)
    lines.append("")
    
    for pred in predictions:
        pair = _format_currency_pair(pred["series_id"])
        signal, confidence = _get_signal_description(pred["action"], pred["p_up"])
        
        # Action indicator
        if pred["action"] == "UP":
            indicator = "▲"
        elif pred["action"] == "DOWN":
            indicator = "▼"
        else:
            indicator = "◆"
        
        lines.append(f"  {indicator} {pair}")
        lines.append(f"    Signal: {signal}")
        lines.append(f"    Probability: {pred['p_up']:.1%}")
        lines.append(f"    Confidence: {confidence}")
        lines.append(f"    Action: {pred['action']}")
        lines.append("")
    
    # Quick reference table
    lines.append("─" * 50)
    lines.append("📋 QUICK REFERENCE")
    lines.append("─" * 50)
    lines.append("")
    lines.append("  Pair         Signal      Prob.    Action")
    lines.append("  " + "─" * 44)
    
    for pred in predictions:
        pair = _format_currency_pair(pred["series_id"])
        signal, _ = _get_signal_description(pred["action"], pred["p_up"])
        prob_str = f"{pred['p_up']:.1%}"
        lines.append(f"  {pair:<12} {signal:<11} {prob_str:<8} {pred['action']}")
    
    lines.append("")
    
    # Interpretation guide
    lines.append("─" * 50)
    lines.append("📖 HOW TO READ THESE SIGNALS")
    lines.append("─" * 50)
    lines.append("")
    lines.append("  • Probability > 60%: Bullish signal (UP action)")
    lines.append("  • Probability < 40%: Bearish signal (DOWN action)")
    lines.append("  • Probability 40-60%: Neutral (HOLD action)")
    lines.append("")
    lines.append(f"  Forecast horizon: {horizon_display}")
    lines.append("  Model: Logistic Regression classifier")
    lines.append("")
    
    # Technical details (collapsed)
    lines.append("─" * 50)
    lines.append("🔧 TECHNICAL DETAILS")
    lines.append("─" * 50)
    lines.append("")
    
    if manifest_data.get("row_counts"):
        sorted_series = sorted(manifest_data["row_counts"].keys())
        for series_id in sorted_series:
            pair = _format_currency_pair(series_id)
            count = manifest_data["row_counts"][series_id]
            lines.append(f"  {pair}: {count} historical data points")
    
    if publish_config:
        bucket = publish_config.get("bucket", "")
        latest_prefix = publish_config.get("latest_prefix", "")
        if bucket and latest_prefix:
            lines.append("")
            lines.append(f"  Data available at: s3://{bucket}/{latest_prefix}")
    
    lines.append("")
    lines.append("═" * 50)
    lines.append("  This is an automated forecast from the FX Pipeline.")
    lines.append("  Not financial advice. Use at your own discretion.")
    lines.append("═" * 50)
    
    return "\n".join(lines)


def build_email_body_html(
    *,
    horizon: str,
    run_date: str,
    latest_dir: str | Path,
    manifest_path: str | Path | None = None,
    publish_config: dict | None = None,
) -> str:
    """
    Build HTML email body from latest outputs.
    
    Args:
        horizon: Horizon identifier (e.g., "h7")
        run_date: Run date in YYYY-MM-DD format
        latest_dir: Path to latest outputs directory
        manifest_path: Optional path to manifest.json (if None, looks in latest_dir)
        publish_config: Optional dict with "runs_prefix" and "latest_prefix" if publish enabled
        
    Returns:
        Email body HTML (deterministic, sorted by series_id)
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
    
    # Extract prediction data
    predictions = _extract_predictions_data(predictions_file)
    
    # Extract manifest data
    if manifest_path is None:
        manifest_path_obj = latest_dir_obj / "manifest.json"
    else:
        if not isinstance(manifest_path, (str, Path)):
            raise TypeError(f"manifest_path must be str, Path, or None, got {type(manifest_path)}")
        manifest_path_obj = Path(manifest_path)
    
    manifest_data = _extract_manifest_data(manifest_path_obj)
    
    # Build HTML
    horizon_display = HORIZON_DISPLAY.get(horizon, horizon.upper())
    readable_date = _format_date_readable(run_date)
    
    # Generate signal cards
    signal_cards = []
    for pred in predictions:
        pair = _format_currency_pair(pred["series_id"])
        signal, confidence = _get_signal_description(pred["action"], pred["p_up"])
        
        # Colors based on signal
        if pred["action"] == "UP":
            bg_color = "#0d9488"  # Teal
            border_color = "#14b8a6"
            icon = "↑"
        elif pred["action"] == "DOWN":
            bg_color = "#dc2626"  # Red
            border_color = "#ef4444"
            icon = "↓"
        else:
            bg_color = "#6b7280"  # Gray
            border_color = "#9ca3af"
            icon = "◆"
        
        # Confidence badge color
        if confidence == "High":
            conf_color = "#059669"
        elif confidence == "Moderate":
            conf_color = "#d97706"
        else:
            conf_color = "#6b7280"
        
        card = f"""
        <div style="background: linear-gradient(135deg, {bg_color} 0%, #1e293b 100%); border-radius: 12px; padding: 20px; margin-bottom: 16px; border-left: 4px solid {border_color};">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                <span style="font-size: 24px; font-weight: 700; color: #ffffff; letter-spacing: 0.5px;">{pair}</span>
                <span style="font-size: 32px; color: #ffffff;">{icon}</span>
            </div>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px;">
                <div>
                    <div style="color: #94a3b8; font-size: 11px; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 4px;">Signal</div>
                    <div style="color: #ffffff; font-size: 18px; font-weight: 600;">{signal}</div>
                </div>
                <div>
                    <div style="color: #94a3b8; font-size: 11px; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 4px;">Probability</div>
                    <div style="color: #ffffff; font-size: 18px; font-weight: 600;">{pred['p_up']:.1%}</div>
                </div>
            </div>
            <div style="margin-top: 12px;">
                <span style="background: {conf_color}; color: #ffffff; padding: 4px 10px; border-radius: 12px; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px;">{confidence} Confidence</span>
            </div>
        </div>
        """
        signal_cards.append(card)
    
    # Generate table rows
    table_rows = []
    for pred in predictions:
        pair = _format_currency_pair(pred["series_id"])
        signal, confidence = _get_signal_description(pred["action"], pred["p_up"])
        
        if pred["action"] == "UP":
            action_style = "color: #10b981; font-weight: 600;"
        elif pred["action"] == "DOWN":
            action_style = "color: #ef4444; font-weight: 600;"
        else:
            action_style = "color: #6b7280; font-weight: 600;"
        
        row = f"""
        <tr>
            <td style="padding: 12px 16px; border-bottom: 1px solid #334155; color: #f1f5f9; font-weight: 500;">{pair}</td>
            <td style="padding: 12px 16px; border-bottom: 1px solid #334155; color: #cbd5e1;">{signal}</td>
            <td style="padding: 12px 16px; border-bottom: 1px solid #334155; color: #cbd5e1;">{pred['p_up']:.1%}</td>
            <td style="padding: 12px 16px; border-bottom: 1px solid #334155; {action_style}">{pred['action']}</td>
        </tr>
        """
        table_rows.append(row)
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; background-color: #0f172a;">
    <div style="max-width: 600px; margin: 0 auto; background-color: #1e293b;">
        
        <!-- Header -->
        <div style="background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0f172a 100%); padding: 32px 24px; text-align: center; border-bottom: 1px solid #334155;">
            <div style="font-size: 12px; text-transform: uppercase; letter-spacing: 2px; color: #64748b; margin-bottom: 8px;">FX Forecast Pipeline</div>
            <h1 style="margin: 0; font-size: 28px; font-weight: 700; color: #f8fafc; letter-spacing: -0.5px;">{horizon_display} Signals</h1>
            <div style="color: #94a3b8; font-size: 14px; margin-top: 8px;">{readable_date}</div>
        </div>
        
        <!-- Signal Cards -->
        <div style="padding: 24px;">
            <h2 style="color: #f1f5f9; font-size: 14px; text-transform: uppercase; letter-spacing: 1.5px; margin: 0 0 16px 0; padding-bottom: 8px; border-bottom: 1px solid #334155;">Today's Signals</h2>
            {''.join(signal_cards)}
        </div>
        
        <!-- Summary Table -->
        <div style="padding: 0 24px 24px;">
            <h2 style="color: #f1f5f9; font-size: 14px; text-transform: uppercase; letter-spacing: 1.5px; margin: 0 0 16px 0; padding-bottom: 8px; border-bottom: 1px solid #334155;">Quick Reference</h2>
            <table style="width: 100%; border-collapse: collapse; background-color: #0f172a; border-radius: 8px; overflow: hidden;">
                <thead>
                    <tr style="background-color: #1e293b;">
                        <th style="padding: 12px 16px; text-align: left; color: #64748b; font-size: 11px; text-transform: uppercase; letter-spacing: 1px; font-weight: 600;">Pair</th>
                        <th style="padding: 12px 16px; text-align: left; color: #64748b; font-size: 11px; text-transform: uppercase; letter-spacing: 1px; font-weight: 600;">Signal</th>
                        <th style="padding: 12px 16px; text-align: left; color: #64748b; font-size: 11px; text-transform: uppercase; letter-spacing: 1px; font-weight: 600;">Prob.</th>
                        <th style="padding: 12px 16px; text-align: left; color: #64748b; font-size: 11px; text-transform: uppercase; letter-spacing: 1px; font-weight: 600;">Action</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(table_rows)}
                </tbody>
            </table>
        </div>
        
        <!-- Interpretation Guide -->
        <div style="padding: 0 24px 24px;">
            <div style="background-color: #0f172a; border-radius: 8px; padding: 20px; border: 1px solid #334155;">
                <h3 style="color: #f1f5f9; font-size: 13px; text-transform: uppercase; letter-spacing: 1px; margin: 0 0 12px 0;">How to Read These Signals</h3>
                <div style="color: #94a3b8; font-size: 13px; line-height: 1.6;">
                    <p style="margin: 0 0 8px 0;"><span style="color: #10b981;">●</span> <strong style="color: #f1f5f9;">Probability &gt; 60%</strong> — Bullish signal (UP)</p>
                    <p style="margin: 0 0 8px 0;"><span style="color: #ef4444;">●</span> <strong style="color: #f1f5f9;">Probability &lt; 40%</strong> — Bearish signal (DOWN)</p>
                    <p style="margin: 0;"><span style="color: #6b7280;">●</span> <strong style="color: #f1f5f9;">Probability 40-60%</strong> — Neutral (HOLD)</p>
                </div>
            </div>
        </div>
        
        <!-- Technical Details -->
        <div style="padding: 0 24px 24px;">
            <div style="background-color: #0f172a; border-radius: 8px; padding: 16px; border: 1px solid #334155;">
                <div style="color: #64748b; font-size: 11px; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 4px;">Forecast Horizon</div>
                <div style="color: #f1f5f9; font-size: 16px; font-weight: 600;">{horizon_display}</div>
            </div>
        </div>
        
        <!-- Footer -->
        <div style="padding: 24px; background-color: #0f172a; text-align: center; border-top: 1px solid #334155;">
            <p style="margin: 0 0 8px 0; color: #64748b; font-size: 12px;">This is an automated forecast from the FX Pipeline.</p>
            <p style="margin: 0; color: #475569; font-size: 11px;">Not financial advice. Use at your own discretion.</p>
        </div>
        
    </div>
</body>
</html>
    """
    
    return html


def send_email(
    cfg: EmailConfig,
    subject: str,
    body_text: str,
    body_html: str | None = None,
) -> None:
    """
    Send email via SendGrid.
    
    Args:
        cfg: Email configuration
        subject: Email subject
        body_text: Email body text (plain text fallback)
        body_html: Optional HTML email body
        
    Raises:
        RuntimeError: If SendGrid send fails or sendgrid is not installed
        ValueError: If api_key is not configured
    """
    # Check api_key FIRST before attempting any sendgrid import
    if not cfg.api_key:
        raise ValueError(
            "SendGrid API key not configured. Set email.api_key in config or "
            "SENDGRID_API_KEY environment variable."
        )
    
    # Lazy import of sendgrid - only required when actually sending
    try:
        from sendgrid import SendGridAPIClient
        from sendgrid.helpers.mail import Mail
    except ImportError:
        raise RuntimeError("sendgrid required. Install with: pip install sendgrid")
    
    message = Mail(
        from_email=cfg.from_email,
        to_emails=cfg.to_emails,
        subject=subject,
        plain_text_content=body_text,
        html_content=body_html,
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
