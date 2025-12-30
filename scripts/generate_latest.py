"""
Generate UI-ready "latest" artifacts from gold parquet.

This is a deterministic local entrypoint intended to be reused later by AWS
(scheduled jobs) without changing modeling logic.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from src.artifacts.write_latest import build_latest, write_artifacts


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate latest UI artifacts for USD/CAD h7.")
    p.add_argument(
        "--outputs-dir",
        type=str,
        default="outputs",
        help="Directory that contains decision_predictions_{horizon}.parquet.",
    )
    p.add_argument(
        "--out-dir",
        type=str,
        default="outputs/latest",
        help="Output directory for latest artifacts (JSON + CSV).",
    )
    p.add_argument(
        "--pair",
        type=str,
        default="USD_CAD",
        help="Pair identifier used in artifact naming.",
    )
    p.add_argument(
        "--horizon",
        type=str,
        default="h7",
        help="Horizon identifier used in artifact naming.",
    )
    p.add_argument(
        "--limit-rows",
        type=int,
        default=90,
        help="Number of most recent rows to export.",
    )
    p.add_argument(
        "--threshold",
        type=float,
        default=0.55,
        help="Decision threshold used to derive actions when needed.",
    )
    p.add_argument(
        "--sha",
        type=str,
        default="local",
        help="Commit SHA to embed in artifacts (use 'local' for dev).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate inputs and build artifact but do not write files.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    outputs_dir = Path(args.outputs_dir)
    out_dir = Path(args.out_dir)

    # build_latest expects outputs_dir to contain decision_predictions_{horizon}.parquet
    required = outputs_dir / f"decision_predictions_{args.horizon}.parquet"
    if not required.exists():
        print(
            f"[ERROR] missing required upstream file: {required}\n"
            f"Run notebooks (06/07) to generate decision_predictions_{args.horizon}.parquet first.",
            file=sys.stderr,
        )
        return 2

    out_dir.mkdir(parents=True, exist_ok=True)

    artifact = build_latest(
        outputs_dir=outputs_dir,
        sha=args.sha,
        pair=args.pair,
        horizon=args.horizon,
        limit_rows=args.limit_rows,
        threshold=args.threshold,
    )

    if args.dry_run:
        print("[OK] dry-run: artifact built successfully (no files written)")
        return 0

    json_path, csv_path = write_artifacts(out_dir, artifact)

    print(f"[OK] wrote: {json_path}")
    print(f"[OK] wrote: {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
