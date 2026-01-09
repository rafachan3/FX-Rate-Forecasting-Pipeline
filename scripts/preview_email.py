#!/usr/bin/env python3
"""
Preview script to generate and view email output locally.

Usage:
    python scripts/preview_email.py

This will generate sample prediction data and create both HTML and text
versions of the email for preview.
"""
from __future__ import annotations

import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.pipeline.email import build_email_body_html, build_email_body_text


def main():
    """Generate sample email and save for preview."""
    # Create temp directory with sample predictions
    with tempfile.TemporaryDirectory() as tmpdir:
        latest_dir = Path(tmpdir) / "latest"
        latest_dir.mkdir()
        
        # Create realistic sample prediction data
        # Using dates leading up to "today"
        today = datetime.now().strftime("%Y-%m-%d")
        dates = pd.date_range(end=today, periods=30, freq="D")
        
        df_pred = pd.DataFrame({
            "obs_date": dates,
            "series_id": ["FXUSDCAD"] * 30,
            "p_up_logreg": [0.45, 0.48, 0.52, 0.55, 0.58, 0.62, 0.65, 0.63, 0.68, 0.72,
                           0.70, 0.68, 0.65, 0.62, 0.58, 0.55, 0.52, 0.48, 0.45, 0.42,
                           0.38, 0.35, 0.38, 0.42, 0.48, 0.55, 0.62, 0.68, 0.72, 0.75],
            "action_logreg": ["HOLD", "HOLD", "HOLD", "HOLD", "HOLD", "UP", "UP", "UP", "UP", "UP",
                              "UP", "UP", "UP", "UP", "HOLD", "HOLD", "HOLD", "HOLD", "HOLD", "HOLD",
                              "DOWN", "DOWN", "DOWN", "HOLD", "HOLD", "HOLD", "UP", "UP", "UP", "UP"],
        })
        
        predictions_file = latest_dir / "decision_predictions_h7.parquet"
        df_pred.to_parquet(predictions_file, index=False)
        
        # Create a simple manifest
        import json
        manifest = {
            "model_artifacts": {
                "files": {}
            },
            "predictions": {
                "by_series_rows": {
                    "FXUSDCAD": 30
                }
            }
        }
        manifest_path = latest_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f)
        
        # Generate emails
        run_date = today
        
        print("=" * 60)
        print("Generating email preview...")
        print("=" * 60)
        
        # Generate text version
        text_body = build_email_body_text(
            horizon="h7",
            run_date=run_date,
            latest_dir=latest_dir,
            publish_config={
                "bucket": "fx-rate-pipeline-dev",
                "runs_prefix": f"predictions/h7/runs/{run_date}/",
                "latest_prefix": "predictions/h7/latest/",
            },
        )
        
        # Generate HTML version
        html_body = build_email_body_html(
            horizon="h7",
            run_date=run_date,
            latest_dir=latest_dir,
            publish_config={
                "bucket": "fx-rate-pipeline-dev",
                "runs_prefix": f"predictions/h7/runs/{run_date}/",
                "latest_prefix": "predictions/h7/latest/",
            },
        )
        
        # Save to files
        output_dir = Path("outputs/email_preview")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        text_file = output_dir / "email_preview.txt"
        html_file = output_dir / "email_preview.html"
        
        with open(text_file, "w") as f:
            f.write(text_body)
        
        with open(html_file, "w") as f:
            f.write(html_body)
        
        print(f"\n✅ Email previews generated!")
        print(f"\n📄 Text version: {text_file}")
        print(f"🌐 HTML version: {html_file}")
        print(f"\nTo preview the HTML email, run:")
        print(f"    open {html_file}")
        print()
        print("=" * 60)
        print("TEXT EMAIL PREVIEW:")
        print("=" * 60)
        print(text_body)


if __name__ == "__main__":
    main()

