"""Gold data synchronization from S3 using AWS CLI."""
from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path

from src.pipeline.config import PipelineConfig


def sync_gold_series(*, bucket: str, key: str, dst_path: str, profile: str | None = None) -> None:
    """
    Sync a single gold series from S3 to local path using AWS CLI.
    
    Uses atomic file replacement: downloads to temporary file, then replaces destination.
    
    Args:
        bucket: S3 bucket name
        key: S3 key (without bucket prefix)
        dst_path: Local destination path
        profile: AWS profile name (optional; if None, uses ambient credentials)
        
    Raises:
        FileNotFoundError: If AWS CLI is not found
        RuntimeError: If AWS CLI command fails or file is missing/empty after download
    """
    dst = Path(dst_path)
    
    # Create parent directories
    dst.parent.mkdir(parents=True, exist_ok=True)
    
    # Create temporary file in same directory as destination (same filesystem)
    with tempfile.NamedTemporaryFile(
        dir=dst.parent, delete=False, suffix=".tmp"
    ) as tmp_file:
        tmp_path = tmp_file.name
    
    try:
        # Run AWS CLI command
        s3_uri = f"s3://{bucket}/{key}"
        cmd = [
            "aws",
            "s3",
            "cp",
            s3_uri,
            tmp_path,
            "--only-show-errors",
        ]
        
        # Only add --profile if provided
        if profile:
            cmd.extend(["--profile", profile])
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
        
        # Check if AWS CLI exists
        if result.returncode == 127:  # Command not found
            raise FileNotFoundError(
                "AWS CLI not found. Install with: pip install awscli or brew install awscli"
            )
        
        # Check if command succeeded
        if result.returncode != 0:
            # Build command string for error message
            cmd_str = " ".join(cmd)
            stderr_trimmed = result.stderr[:500] if len(result.stderr) > 500 else result.stderr
            raise RuntimeError(
                f"AWS CLI failed (exit={result.returncode}). cmd='{cmd_str}'. stderr='{stderr_trimmed}'"
            )
        
        # Verify file exists and is not empty
        tmp_file_path = Path(tmp_path)
        if not tmp_file_path.exists():
            raise RuntimeError(f"Downloaded file not found: {tmp_path}")
        
        if tmp_file_path.stat().st_size == 0:
            raise RuntimeError(f"Downloaded file is empty: {tmp_path}")
        
        # Atomically replace destination
        os.replace(tmp_path, dst_path)
        
    except Exception:
        # Cleanup temp file on error
        try:
            if Path(tmp_path).exists():
                os.unlink(tmp_path)
        except Exception:
            pass  # Ignore cleanup errors
        raise


def sync_gold_from_config(*, cfg: PipelineConfig) -> None:
    """
    Sync all gold series from S3 based on pipeline configuration.
    
    Args:
        cfg: Pipeline configuration
        
    Raises:
        FileNotFoundError: If AWS CLI is not found
        RuntimeError: If any sync operation fails
    """
    # Sort series by series_id for determinism
    series_sorted = sorted(cfg.series, key=lambda s: s.series_id)
    
    for series in series_sorted:
        series_id = series.series_id
        key = cfg.s3_key_for_series(series_id)
        # gold_local_path is now a directory, append the standard filename
        dst_path = str(Path(series.gold_local_path) / "data.parquet")
        print(f"[sync_gold] {series_id} -> {dst_path} (key={key})") 
        sync_gold_series(
            bucket=cfg.s3.bucket,
            key=key,
            dst_path=dst_path,
            profile=cfg.s3.profile,
        )

def main() -> None:
    import argparse
    from src.pipeline.config import load_pipeline_config

    parser = argparse.ArgumentParser(description="Sync Gold parquets from S3 to local paths.")
    parser.add_argument("--config", required=True, help="Path to pipeline config JSON (e.g. config/pipeline_h7.json)")
    parser.add_argument("--run-date", required=False, help="Run date (currently unused by sync, kept for CLI consistency)")
    args = parser.parse_args()

    cfg = load_pipeline_config(args.config)

    # Minimal visibility so we never debug blind again
    print(f"[sync_gold] series={len(cfg.series)} bucket={cfg.s3.bucket} profile={cfg.s3.profile}")

    sync_gold_from_config(cfg=cfg)
    print("[sync_gold] done")


if __name__ == "__main__":
    main()
