from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import boto3


@dataclass(frozen=True)
class GoldLocation:
    """
    Location + defaults for the Gold S3 layout.

    Expected keys:
      gold/source=BoC/series=<SERIES>/{data.parquet,_watermark.json}
    """
    bucket: str
    source: str = "BoC"
    prefix: str = "gold"
    region: str = "us-east-1"

    def series_prefix(self, series: str) -> str:
        return f"{self.prefix}/source={self.source}/series={series}"

    def parquet_key(self, series: str) -> str:
        return f"{self.series_prefix(series)}/data.parquet"

    def watermark_key(self, series: str) -> str:
        return f"{self.series_prefix(series)}/_watermark.json"


def _session(profile: Optional[str]) -> boto3.session.Session:
    # If profile is set, boto3 will use ~/.aws/config + ~/.aws/credentials
    # (including role_arn assume-role profiles).
    if profile:
        return boto3.session.Session(profile_name=profile)
    return boto3.session.Session()


def _s3_client(profile: Optional[str], region: str):
    return _session(profile).client("s3", region_name=region)


def download_s3_object(
    *,
    bucket: str,
    key: str,
    dest: Path,
    profile: Optional[str] = None,
    region: str = "us-east-1",
) -> None:
    """
    Download S3 object to local path.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    s3 = _s3_client(profile, region)

    # download_file is efficient and avoids loading into memory
    s3.download_file(bucket, key, str(dest))


def load_watermark(
    *,
    bucket: str,
    key: str,
    profile: Optional[str] = None,
    region: str = "us-east-1",
) -> Dict[str, Any]:
    """
    Read watermark JSON from S3 and return as dict.
    """
    s3 = _s3_client(profile, region)
    obj = s3.get_object(Bucket=bucket, Key=key)
    raw = obj["Body"].read()
    return json.loads(raw)
