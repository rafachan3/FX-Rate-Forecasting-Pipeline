# src/artifacts/write_latest.py
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

import pandas as pd


def _slug(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s).strip("_")


def _ensure_datetime_index_or_col(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # If obs_date is the index, keep it and also create a column for export
    if isinstance(df.index, pd.DatetimeIndex):
        if df.index.name is None:
            df.index.name = "obs_date"
        if df.index.name != "obs_date":
            # rename index name to obs_date for consistency
            df.index.name = "obs_date"
        df = df.sort_index()
        df["obs_date"] = df.index.tz_localize(None)
        return df

    # Else require obs_date column (fallback)
    if "obs_date" not in df.columns:
        raise ValueError("Expected a DatetimeIndex or an obs_date column.")
    df["obs_date"] = pd.to_datetime(df["obs_date"], errors="coerce").dt.tz_localize(None)
    df = df.sort_values("obs_date")
    return df


def _safe_float(x) -> Optional[float]:
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def _derive_action(p: Optional[float], threshold: float) -> Optional[str]:
    if p is None:
        return None
    # Simple directional signal for UI (you can change labels later)
    return "UP" if p >= threshold else "DOWN"


@dataclass(frozen=True)
class LatestRow:
    obs_date: str
    pair: str

    # export both models if present
    p_up_logreg: Optional[float]
    p_up_tree: Optional[float]

    # optional, derived
    action_logreg: Optional[str]
    action_tree: Optional[str]


@dataclass(frozen=True)
class LatestArtifact:
    sha: str
    pair: str
    horizon: str
    generated_at: str
    rows: list[LatestRow]


def build_latest(
    outputs_dir: Path,
    sha: str,
    pair: str,
    horizon: str,
    limit_rows: int,
    threshold: float,
) -> LatestArtifact:
    # Your file name is decision_predictions_h7.parquet
    path = outputs_dir / f"decision_predictions_{horizon}.parquet"
    if not path.exists():
        raise FileNotFoundError(str(path))

    df = pd.read_parquet(path)
    df = _ensure_datetime_index_or_col(df)

    # Identify probability columns (your current schema)
    p_logreg = "p_up_logreg" if "p_up_logreg" in df.columns else None
    p_tree = "p_up_tree" if "p_up_tree" in df.columns else None

    # If a “richer” schema exists, support it too
    # (for future: p_up_raw / p_up_cal, etc.)
    if p_logreg is None and "p_up_raw" in df.columns:
        p_logreg = "p_up_raw"

    df = df.tail(limit_rows)

    rows: list[LatestRow] = []
    for _, r in df.iterrows():
        pl = _safe_float(r[p_logreg]) if p_logreg else None
        pt = _safe_float(r[p_tree]) if p_tree else None

        rows.append(
            LatestRow(
                obs_date=pd.Timestamp(r["obs_date"]).date().isoformat(),
                pair=pair,
                p_up_logreg=pl,
                p_up_tree=pt,
                action_logreg=_derive_action(pl, threshold) if pl is not None else None,
                action_tree=_derive_action(pt, threshold) if pt is not None else None,
            )
        )

    return LatestArtifact(
        sha=sha,
        pair=pair,
        horizon=horizon,
        generated_at=pd.Timestamp.utcnow().isoformat(timespec="seconds") + "Z",
        rows=rows,
    )


def write_artifacts(outputs_dir: Path, artifact: LatestArtifact) -> tuple[Path, Path]:
    # If caller already points to a "latest" directory, don't nest another one.
    target_dir = outputs_dir if outputs_dir.name == "latest" else (outputs_dir / "latest")
    target_dir.mkdir(parents=True, exist_ok=True)

    pair_slug = _slug(artifact.pair)
    json_path = target_dir / f"latest_{pair_slug}_{artifact.horizon}.json"
    csv_path = target_dir / f"latest_{pair_slug}_{artifact.horizon}.csv"

    payload = asdict(artifact)
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    df = pd.DataFrame([asdict(r) for r in artifact.rows])
    df.to_csv(csv_path, index=False)

    return json_path, csv_path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Write UI-friendly latest artifacts.")
    p.add_argument("--outputs", required=True, help="Path to outputs/ directory")
    p.add_argument("--sha", required=True, help="Git SHA to stamp artifacts with")
    p.add_argument("--pair", default="USD/CAD", help="Label for the pair (used in filenames + payload)")
    p.add_argument("--horizon", default="h7", help="Horizon tag, e.g. h7")
    p.add_argument("--limit", type=int, default=365, help="Max rows to keep (most recent)")
    p.add_argument("--threshold", type=float, default=0.5, help="Threshold to derive UP/DOWN actions")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    outputs_dir = Path(args.outputs)

    artifact = build_latest(
        outputs_dir=outputs_dir,
        sha=args.sha,
        pair=args.pair,
        horizon=args.horizon,
        limit_rows=args.limit,
        threshold=args.threshold,
    )
    json_path, csv_path = write_artifacts(outputs_dir, artifact)
    print(f"Wrote: {json_path}")
    print(f"Wrote: {csv_path}")
    print(f"Rows: {len(artifact.rows)}")


if __name__ == "__main__":
    main()
