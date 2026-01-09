"""
Tests for src.artifacts.write_latest module.
"""
import json
import pytest
import pandas as pd
import tempfile
from pathlib import Path

from src.artifacts.write_latest import build_latest, LatestRow, promote_to_latest


def test_build_latest_with_decision_confidence():
    """Test that build_latest uses decision/confidence from input if available."""
    with tempfile.TemporaryDirectory() as tmpdir:
        outputs_dir = Path(tmpdir)
        
        # Create a parquet file with decision and confidence columns
        df = pd.DataFrame({
            "obs_date": pd.date_range("2024-01-01", periods=5, freq="D"),
            "p_up_logreg": [0.7, 0.3, 0.5, 0.8, 0.2],
            "p_up_tree": [0.65, 0.35, 0.55, 0.75, 0.25],
            "decision": ["UP", "DOWN", "SIDEWAYS", "UP", "DOWN"],
            "confidence": [0.5, 0.7, 0.0, 0.8, 0.6],
        })
        df = df.set_index("obs_date")
        
        parquet_path = outputs_dir / "decision_predictions_h7.parquet"
        df.to_parquet(parquet_path)
        
        artifact = build_latest(
            outputs_dir=outputs_dir,
            sha="test123",
            pair="USD/CAD",
            horizon="h7",
            limit_rows=5,
            threshold=0.6,
        )
        
        assert len(artifact.rows) == 5
        assert artifact.rows[0].decision == "UP"
        assert artifact.rows[0].confidence == pytest.approx(0.5, abs=1e-6)
        assert artifact.rows[1].decision == "DOWN"
        assert artifact.rows[1].confidence == pytest.approx(0.7, abs=1e-6)
        assert artifact.rows[2].decision == "SIDEWAYS"
        assert artifact.rows[2].confidence == pytest.approx(0.0, abs=1e-6)


def test_build_latest_normalizes_abstain():
    """Test that ABSTAIN labels are normalized to SIDEWAYS."""
    with tempfile.TemporaryDirectory() as tmpdir:
        outputs_dir = Path(tmpdir)
        
        df = pd.DataFrame({
            "obs_date": pd.date_range("2024-01-01", periods=3, freq="D"),
            "p_up_logreg": [0.7, 0.3, 0.5],
            "decision": ["UP", "DOWN", "ABSTAIN"],  # Old label
            "confidence": [0.5, 0.7, 0.0],
        })
        df = df.set_index("obs_date")
        
        parquet_path = outputs_dir / "decision_predictions_h7.parquet"
        df.to_parquet(parquet_path)
        
        artifact = build_latest(
            outputs_dir=outputs_dir,
            sha="test123",
            pair="USD/CAD",
            horizon="h7",
            limit_rows=3,
            threshold=0.6,
        )
        
        assert artifact.rows[2].decision == "SIDEWAYS"  # Normalized from ABSTAIN


def test_build_latest_derives_from_probabilities():
    """Test that build_latest derives decision/confidence from p_up when not in input."""
    with tempfile.TemporaryDirectory() as tmpdir:
        outputs_dir = Path(tmpdir)
        
        # Create parquet with only probability columns
        df = pd.DataFrame({
            "obs_date": pd.date_range("2024-01-01", periods=5, freq="D"),
            "p_up_logreg": [0.7, 0.3, 0.5, 0.8, 0.2],
            "p_up_tree": [0.65, 0.35, 0.55, 0.75, 0.25],
        })
        df = df.set_index("obs_date")
        
        parquet_path = outputs_dir / "decision_predictions_h7.parquet"
        df.to_parquet(parquet_path)
        
        artifact = build_latest(
            outputs_dir=outputs_dir,
            sha="test123",
            pair="USD/CAD",
            horizon="h7",
            limit_rows=5,
            threshold=0.6,
        )
        
        assert len(artifact.rows) == 5
        
        # Check first row: p=0.7 >= 0.6 -> UP
        assert artifact.rows[0].decision == "UP"
        assert artifact.rows[0].confidence is not None
        assert artifact.rows[0].confidence > 0.0
        
        # Check second row: p=0.3 <= 0.4 (1-0.6) -> DOWN
        assert artifact.rows[1].decision == "DOWN"
        assert artifact.rows[1].confidence is not None
        assert artifact.rows[1].confidence > 0.0
        
        # Check third row: p=0.5 in band (0.4 < 0.5 < 0.6) -> SIDEWAYS
        assert artifact.rows[2].decision == "SIDEWAYS"
        assert artifact.rows[2].confidence == pytest.approx(0.0, abs=1e-6)


def test_build_latest_backward_compat():
    """Test that backward compat fields (action_logreg, action_tree) still work."""
    with tempfile.TemporaryDirectory() as tmpdir:
        outputs_dir = Path(tmpdir)
        
        df = pd.DataFrame({
            "obs_date": pd.date_range("2024-01-01", periods=3, freq="D"),
            "p_up_logreg": [0.7, 0.3, 0.5],
            "p_up_tree": [0.65, 0.35, 0.55],
        })
        df = df.set_index("obs_date")
        
        parquet_path = outputs_dir / "decision_predictions_h7.parquet"
        df.to_parquet(parquet_path)
        
        artifact = build_latest(
            outputs_dir=outputs_dir,
            sha="test123",
            pair="USD/CAD",
            horizon="h7",
            limit_rows=3,
            threshold=0.6,
        )
        
        # Check backward compat fields
        assert artifact.rows[0].action_logreg == "UP"
        assert artifact.rows[0].action_tree == "UP"
        assert artifact.rows[1].action_logreg == "DOWN"
        assert artifact.rows[1].action_tree == "DOWN"
        assert artifact.rows[2].action_logreg == "SIDEWAYS"
        assert artifact.rows[2].action_tree == "SIDEWAYS"


def test_build_latest_uses_logreg_when_available():
    """Test that decision/confidence derivation prefers logreg over tree."""
    with tempfile.TemporaryDirectory() as tmpdir:
        outputs_dir = Path(tmpdir)
        
        df = pd.DataFrame({
            "obs_date": pd.date_range("2024-01-01", periods=2, freq="D"),
            "p_up_logreg": [0.7, None],  # First has logreg, second doesn't
            "p_up_tree": [0.65, 0.3],
        })
        df = df.set_index("obs_date")
        
        parquet_path = outputs_dir / "decision_predictions_h7.parquet"
        df.to_parquet(parquet_path)
        
        artifact = build_latest(
            outputs_dir=outputs_dir,
            sha="test123",
            pair="USD/CAD",
            horizon="h7",
            limit_rows=2,
            threshold=0.6,
        )
        
        # First row: uses logreg (0.7 >= 0.6 -> UP)
        assert artifact.rows[0].decision == "UP"
        
        # Second row: uses tree (0.3 <= 0.4 -> DOWN)
        assert artifact.rows[1].decision == "DOWN"


def test_promote_to_latest_atomic(tmp_path: Path):
    """Test that promote_to_latest atomically promotes files."""
    latest_dir = tmp_path / "latest"
    latest_dir.mkdir()
    
    # Create source files
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    src_parquet = src_dir / "predictions.parquet"
    src_json = src_dir / "manifest.json"
    
    df = pd.DataFrame({"col": [1, 2, 3]})
    df.to_parquet(src_parquet, index=False)
    src_json.write_text(json.dumps({"test": "data"}), encoding="utf-8")
    
    # Promote files
    promote_to_latest(
        latest_dir=str(latest_dir),
        files=[
            (str(src_parquet), "predictions.parquet"),
            (str(src_json), "manifest.json"),
        ],
    )
    
    # Verify files exist in latest
    assert (latest_dir / "predictions.parquet").exists()
    assert (latest_dir / "manifest.json").exists()
    
    # Verify temp directory was cleaned up
    temp_dirs = [d for d in latest_dir.iterdir() if d.name.startswith(".tmp_")]
    assert len(temp_dirs) == 0
    
    # Verify file contents match
    df_read = pd.read_parquet(latest_dir / "predictions.parquet")
    assert len(df_read) == 3
    
    manifest_read = json.loads((latest_dir / "manifest.json").read_text())
    assert manifest_read == {"test": "data"}


def test_promote_to_latest_missing_source_raises(tmp_path: Path):
    """Test that missing source file raises before modifying latest."""
    latest_dir = tmp_path / "latest"
    latest_dir.mkdir()
    
    # Create one source file
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    src_parquet = src_dir / "predictions.parquet"
    df = pd.DataFrame({"col": [1, 2, 3]})
    df.to_parquet(src_parquet, index=False)
    
    missing_file = src_dir / "missing.parquet"
    
    # Try to promote with missing file
    with pytest.raises(FileNotFoundError, match="Source file not found"):
        promote_to_latest(
            latest_dir=str(latest_dir),
            files=[
                (str(src_parquet), "predictions.parquet"),
                (str(missing_file), "missing.parquet"),
            ],
        )
    
    # Verify latest directory was not modified
    assert not (latest_dir / "predictions.parquet").exists()
    assert not (latest_dir / "missing.parquet").exists()


def test_promote_to_latest_deterministic(tmp_path: Path):
    """Test that promote_to_latest produces deterministic results."""
    latest_dir = tmp_path / "latest"
    latest_dir.mkdir()
    
    # Create source file
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    src_file = src_dir / "data.parquet"
    df = pd.DataFrame({"col": [1, 2, 3]})
    df.to_parquet(src_file, index=False)
    
    # Promote twice
    promote_to_latest(
        latest_dir=str(latest_dir),
        files=[(str(src_file), "data.parquet")],
    )
    
    # Modify source
    df2 = pd.DataFrame({"col": [4, 5, 6]})
    df2.to_parquet(src_file, index=False)
    
    # Promote again
    promote_to_latest(
        latest_dir=str(latest_dir),
        files=[(str(src_file), "data.parquet")],
    )
    
    # Verify latest contains new content
    df_read = pd.read_parquet(latest_dir / "data.parquet")
    assert df_read["col"].tolist() == [4, 5, 6]


def test_promote_to_latest_overwrites_existing(tmp_path: Path):
    """Test that promote_to_latest overwrites existing files atomically."""
    latest_dir = tmp_path / "latest"
    latest_dir.mkdir()
    
    # Create initial file in latest
    old_file = latest_dir / "data.parquet"
    df_old = pd.DataFrame({"col": [1, 2, 3]})
    df_old.to_parquet(old_file, index=False)
    
    # Create new source file
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    src_file = src_dir / "data.parquet"
    df_new = pd.DataFrame({"col": [4, 5, 6]})
    df_new.to_parquet(src_file, index=False)
    
    # Promote new file
    promote_to_latest(
        latest_dir=str(latest_dir),
        files=[(str(src_file), "data.parquet")],
    )
    
    # Verify old file was replaced
    df_read = pd.read_parquet(latest_dir / "data.parquet")
    assert df_read["col"].tolist() == [4, 5, 6]

