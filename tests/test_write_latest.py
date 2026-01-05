"""
Tests for src.artifacts.write_latest module.
"""
import pytest
import pandas as pd
import tempfile
from pathlib import Path

from src.artifacts.write_latest import build_latest, LatestRow


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

