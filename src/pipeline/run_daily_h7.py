"""Daily runner CLI for h7 pipeline."""
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from src.artifacts.manifest import build_run_manifest
from src.artifacts.write_latest import promote_to_latest
from src.data_access.sync_gold import sync_gold_from_config
from src.pipeline.config import PipelineConfig, load_pipeline_config
from src.pipeline.paths import (
    get_run_dir,
    get_run_manifest_path,
    get_run_predictions_path,
)
from src.pipeline.email_ses import build_email_body_text, build_email_subject, send_email_ses
from src.pipeline.publish_s3 import publish_latest_outputs, publish_run_outputs
from src.pipeline.run_date import toronto_now_iso, toronto_today


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Run daily h7 pipeline: sync gold, run inference, build manifest, promote to latest."
    )
    parser.add_argument(
        "--config",
        required=True,
        type=str,
        help="Path to pipeline configuration JSON file",
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        help="Sync gold data from S3 before running inference",
    )
    parser.add_argument(
        "--run-date",
        type=str,
        default=None,
        help="Override run date (YYYY-MM-DD format). If not provided, uses Toronto today.",
    )
    parser.add_argument(
        "--models-dir",
        type=str,
        default=None,
        help="Override models directory. If not provided, uses config.artifacts.dir",
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help="Publish outputs to S3 after local promotion (requires publish config)",
    )
    parser.add_argument(
        "--email",
        action="store_true",
        help="Send email notification after promotion (requires email config)",
    )
    return parser.parse_args()


def validate_run_date(run_date: str) -> None:
    """
    Validate run date format.
    
    Args:
        run_date: Run date string
        
    Raises:
        ValueError: If run_date is not in YYYY-MM-DD format
    """
    try:
        from datetime import datetime
        datetime.strptime(run_date, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"run_date must be in YYYY-MM-DD format, got: {run_date}") from e


def main() -> None:
    """Main entry point for daily runner."""
    args = parse_args()
    
    # Load configuration
    config = load_pipeline_config(args.config)
    
    # Determine run date
    if args.run_date:
        validate_run_date(args.run_date)
        run_date = args.run_date
    else:
        run_date = toronto_today().isoformat()
    
    # Get run timestamp
    run_timestamp = toronto_now_iso()
    
    # Determine models directory
    models_dir = args.models_dir if args.models_dir else config.artifacts.dir
    
    # Create run directory
    run_dir = get_run_dir(runs_dir=config.outputs.runs_dir, run_date=run_date)
    Path(run_dir).mkdir(parents=True, exist_ok=True)
    
    # Sync gold (if requested)
    if args.sync:
        sync_gold_from_config(cfg=config)
    
    # Get paths
    run_predictions_path = get_run_predictions_path(
        runs_dir=config.outputs.runs_dir, run_date=run_date
    )
    run_manifest_path = get_run_manifest_path(
        runs_dir=config.outputs.runs_dir, run_date=run_date
    )
    
    # Run inference using subprocess (module CLI)
    # Find gold root directory (common parent of all gold files)
    gold_paths = [Path(s.gold_local_path) for s in config.series]
    if gold_paths:
        # Use parent directory of first gold file as root (assumes consistent structure)
        gold_root = gold_paths[0].parent
    else:
        gold_root = Path("data/gold")
    
    inference_cmd = [
        "python",
        "-m",
        "src.models.run_inference_h7",
        "--gold-root",
        str(gold_root),
        "--out",
        run_predictions_path,
        "--model-dir",
        models_dir,
        "--glob-pattern",
        "**/*.parquet",
    ]
    
    result = subprocess.run(inference_cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"Inference failed (exit code {result.returncode}):\n{result.stderr}"
        )
    
    # Build manifest
    gold_inputs = [
        {"series_id": s.series_id, "path": s.gold_local_path}
        for s in sorted(config.series, key=lambda x: x.series_id)
    ]
    
    # Build model artifacts dict
    artifacts_dir = Path(models_dir)
    model_files = {
        config.artifacts.model_file: artifacts_dir / config.artifacts.model_file,
        config.artifacts.features_file: artifacts_dir / config.artifacts.features_file,
        config.artifacts.metadata_file: artifacts_dir / config.artifacts.metadata_file,
    }
    
    manifest = build_run_manifest(
        run_date=run_date,
        run_timestamp=run_timestamp,
        gold_inputs=gold_inputs,
        model_artifacts={"dir": str(artifacts_dir), "files": model_files},
        predictions_path=run_predictions_path,
    )
    
    # Write manifest JSON with deterministic formatting
    Path(run_manifest_path).parent.mkdir(parents=True, exist_ok=True)
    with open(run_manifest_path, "w") as f:
        json.dump(manifest, f, sort_keys=True, indent=2)
    
    # Atomically promote to latest
    promote_to_latest(
        latest_dir=config.outputs.latest_dir,
        files=[
            (run_predictions_path, "decision_predictions_h7.parquet"),
            (run_manifest_path, "manifest.json"),
        ],
    )
    
    # Publish to S3 (if requested)
    published_info = ""
    if args.publish:
        if config.publish is None:
            raise ValueError(
                "--publish flag provided but publish configuration is missing in config file"
            )
        
        # Publish run outputs first
        publish_run_outputs(
            run_dir=run_dir,
            horizon=config.horizon,
            run_date=run_date,
            bucket=config.publish.bucket,
            profile=config.publish.profile,
            prefix_runs_template=config.publish.prefix_runs_template,
        )
        
        # Then publish latest outputs
        publish_latest_outputs(
            latest_dir=config.outputs.latest_dir,
            horizon=config.horizon,
            bucket=config.publish.bucket,
            profile=config.publish.profile,
            prefix_latest=config.publish.prefix_latest,
        )
        
        # Build published info string
        runs_prefix = config.publish.prefix_runs_template.format(
            horizon=config.horizon, run_date=run_date
        )
        latest_prefix = config.publish.prefix_latest.format(horizon=config.horizon)
        published_info = f" published=runs:{runs_prefix},latest:{latest_prefix}"
    
    # Send email (if requested)
    emailed_info = ""
    if args.email:
        if config.email is None:
            raise ValueError(
                "--email flag provided but email configuration is missing in config file"
            )
        
        # Build publish config dict for email body (if publish was enabled)
        publish_config_dict = None
        if args.publish and config.publish:
            runs_prefix = config.publish.prefix_runs_template.format(
                horizon=config.horizon, run_date=run_date
            )
            latest_prefix = config.publish.prefix_latest.format(horizon=config.horizon)
            publish_config_dict = {
                "bucket": config.publish.bucket,
                "runs_prefix": runs_prefix,
                "latest_prefix": latest_prefix,
            }
        
        # Build email subject and body
        subject = build_email_subject(config.email, config.horizon, run_date)
        body_text = build_email_body_text(
            horizon=config.horizon,
            run_date=run_date,
            latest_dir=config.outputs.latest_dir,
            manifest_path=None,  # Will auto-detect from latest_dir
            publish_config=publish_config_dict,
        )
        
        # Send email
        send_email_ses(config.email, subject, body_text)
        emailed_info = " emailed=true"
    
    # Print single success line
    print(
        f"[OK] run_date={run_date} promoted=decision_predictions_h7.parquet,manifest.json{published_info}{emailed_info}"
    )


if __name__ == "__main__":
    main()

