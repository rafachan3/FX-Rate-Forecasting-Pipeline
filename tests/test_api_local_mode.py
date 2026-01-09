"""Tests for local dev mode (filesystem instead of S3)."""

import importlib
import json
import os
from pathlib import Path

import pytest


@pytest.fixture
def local_test_dir(tmp_path):
    """Create temporary directory with deterministic test data."""
    test_dir = tmp_path / "latest"
    test_dir.mkdir()
    
    # Create manifest.json with deterministic values
    manifest = {
        "run_date": "2024-01-01",
        "run_timestamp": "2024-01-01T17:40:00-05:00",
        "timezone": "America/Toronto",
        "git_sha": "deadbeef",
        "predictions": {
            "horizon": "h7",
        },
    }
    (test_dir / "manifest.json").write_text(json.dumps(manifest))
    
    # Create latest_USD_CAD_h7.json with deterministic values
    latest_usd = {
        "pair": "USD_CAD",
        "horizon": "h7",
        "generated_at": "2024-01-01T17:40:00Z",
        "rows": [
            {
                "obs_date": "2024-01-01",
                "p_up_logreg": 0.8,
                "action_logreg": "UP",
            }
        ],
    }
    (test_dir / "latest_USD_CAD_h7.json").write_text(json.dumps(latest_usd))
    
    # Create latest_EUR_CAD_h7.json with deterministic values
    latest_eur = {
        "pair": "EUR_CAD",
        "horizon": "h7",
        "generated_at": "2024-01-01T17:40:00Z",
        "rows": [
            {
                "obs_date": "2024-01-01",
                "p_up_logreg": 0.2,
                "action_logreg": "DOWN",
            }
        ],
    }
    (test_dir / "latest_EUR_CAD_h7.json").write_text(json.dumps(latest_eur))
    
    return str(test_dir)


def _reload_modules():
    """Reload config and s3_latest modules to pick up env var changes."""
    import src.api.config
    import src.api.s3_latest
    importlib.reload(src.api.config)
    importlib.reload(src.api.s3_latest)


def test_load_manifest_local(local_test_dir, monkeypatch):
    """Test loading manifest from local filesystem."""
    monkeypatch.setenv("LOCAL_LATEST_DIR", local_test_dir)
    _reload_modules()
    
    from src.api.s3_latest import load_manifest
    
    manifest = load_manifest()
    
    assert manifest["run_date"] == "2024-01-01"
    assert manifest["timezone"] == "America/Toronto"
    assert manifest["git_sha"] == "deadbeef"
    assert manifest["run_timestamp"] == "2024-01-01T17:40:00-05:00"


def test_load_latest_json_local(local_test_dir, monkeypatch):
    """Test loading latest JSON from local filesystem."""
    monkeypatch.setenv("LOCAL_LATEST_DIR", local_test_dir)
    _reload_modules()
    
    from src.api.s3_latest import load_latest_json
    
    data = load_latest_json("USD_CAD")
    
    assert data is not None
    assert data["pair"] == "USD_CAD"
    assert len(data["rows"]) == 1
    assert data["rows"][0]["obs_date"] == "2024-01-01"
    assert data["rows"][0]["p_up_logreg"] == 0.8
    assert data["rows"][0]["action_logreg"] == "UP"


def test_load_latest_json_missing_pair(local_test_dir, monkeypatch):
    """Test loading non-existent pair returns None."""
    monkeypatch.setenv("LOCAL_LATEST_DIR", local_test_dir)
    _reload_modules()
    
    from src.api.s3_latest import load_latest_json
    
    data = load_latest_json("GBP_CAD")
    
    assert data is None


def test_get_latest_predictions_local(local_test_dir, monkeypatch):
    """Test getting predictions in local mode."""
    monkeypatch.setenv("LOCAL_LATEST_DIR", local_test_dir)
    _reload_modules()
    
    from src.api.s3_latest import get_latest_predictions
    
    items = get_latest_predictions(["USD_CAD", "EUR_CAD"])
    
    assert len(items) == 2
    
    usd_item = next(item for item in items if item.pair == "USD_CAD")
    assert usd_item.direction.value == "UP"
    assert usd_item.confidence == 0.8  # max(0.8, 1-0.8) = 0.8
    assert usd_item.obs_date == "2024-01-01"
    assert usd_item.raw["p_up"] == 0.8
    
    eur_item = next(item for item in items if item.pair == "EUR_CAD")
    assert eur_item.direction.value == "DOWN"
    assert eur_item.confidence == 0.8  # max(0.2, 1-0.2) = max(0.2, 0.8) = 0.8
    assert eur_item.obs_date == "2024-01-01"
    assert eur_item.raw["p_up"] == 0.2


def test_get_manifest_metadata_local(local_test_dir, monkeypatch):
    """Test getting manifest metadata in local mode."""
    monkeypatch.setenv("LOCAL_LATEST_DIR", local_test_dir)
    _reload_modules()
    
    from src.api.s3_latest import get_manifest_metadata
    
    metadata = get_manifest_metadata()
    
    assert metadata["horizon"] == "h7"
    assert metadata["run_date"] == "2024-01-01"
    assert metadata["timezone"] == "America/Toronto"
    assert metadata["git_sha"] == "deadbeef"
    assert metadata["as_of_utc"] is not None
    # Verify UTC conversion (should convert -05:00 to UTC)
    assert "2024-01-01" in metadata["as_of_utc"]

