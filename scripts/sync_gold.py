from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from src.data_access.gold_s3 import GoldLocation, download_s3_object, load_watermark


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Sync FX Gold parquet from S3 (cross-account role via AWS profile)."
    )

    p.add_argument(
        "--series",
        required=True,
        help="Series id, e.g. FXUSDCAD",
    )
    p.add_argument(
        "--out",
        required=True,
        help="Output parquet file path, e.g. data/data-USD-CAD.parquet",
    )
    p.add_argument(
        "--bucket",
        default="fx-rate-pipeline-dev",
        help="S3 bucket name (default: fx-rate-pipeline-dev)",
    )
    p.add_argument(
        "--profile",
        default=None,
        help="AWS CLI profile name to use (e.g. fx-gold). If omitted, uses default env/credentials.",
    )
    p.add_argument(
        "--region",
        default="us-east-1",
        help="AWS region (default: us-east-1)",
    )
    p.add_argument(
        "--source",
        default="BoC",
        help="Gold source partition (default: BoC)",
    )
    p.add_argument(
        "--with-watermark",
        action="store_true",
        help="Also fetch and print the _watermark.json for this series.",
    )

    return p.parse_args()


def main() -> int:
    args = parse_args()

    series = args.series.strip()
    out_path = Path(args.out)

    loc = GoldLocation(bucket=args.bucket, source=args.source, region=args.region)

    parquet_key = loc.parquet_key(series)
    watermark_key = loc.watermark_key(series)

    try:
        download_s3_object(
            bucket=loc.bucket,
            key=parquet_key,
            dest=out_path,
            profile=args.profile,
            region=loc.region,
        )
        print(f"[OK] downloaded: s3://{loc.bucket}/{parquet_key}")
        print(f"[OK] saved to: {out_path}")
    except Exception as e:
        print(
            f"[ERROR] failed to download parquet from s3://{loc.bucket}/{parquet_key}\n{e}",
            file=sys.stderr,
        )
        return 1

    if args.with_watermark:
        try:
            wm = load_watermark(
                bucket=loc.bucket,
                key=watermark_key,
                profile=args.profile,
                region=loc.region,
            )
            print(f"[OK] watermark: s3://{loc.bucket}/{watermark_key}")
            print(json.dumps(wm, indent=2))
        except Exception as e:
            print(
                f"[WARN] failed to read watermark s3://{loc.bucket}/{watermark_key}\n{e}",
                file=sys.stderr,
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
