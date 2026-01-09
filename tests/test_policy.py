"""
Tests for src.signals.policy module.
"""
import pytest
import pandas as pd
import numpy as np

from src.signals.policy import (
    apply_threshold_policy,
    confidence_from_p,
    normalize_label,
)


def test_normalize_label():
    """Test label normalization."""
    assert normalize_label("ABSTAIN") == "SIDEWAYS"
    assert normalize_label("abstain") == "SIDEWAYS"
    assert normalize_label("UP") == "UP"
    assert normalize_label("DOWN") == "DOWN"
    assert normalize_label("SIDEWAYS") == "SIDEWAYS"
    assert normalize_label(None) is None


def test_apply_threshold_policy():
    """Test threshold policy application."""
    t = 0.6
    
    # Test cases around the band
    p_up = pd.Series([0.0, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 1.0])
    
    result = apply_threshold_policy(p_up, t=t)
    
    expected = pd.Series([
        "DOWN",  # 0.0 <= 0.4 (1 - t)
        "DOWN",  # 0.3 <= 0.4
        "DOWN",  # 0.4 <= 0.4
        "SIDEWAYS",  # 0.5 in band (0.4 < 0.5 < 0.6)
        "UP",  # 0.6 >= 0.6 (t)
        "UP",  # 0.7 >= 0.6
        "UP",  # 0.8 >= 0.6
        "UP",  # 1.0 >= 0.6
    ])
    
    pd.testing.assert_series_equal(result, expected, check_names=False)


def test_apply_threshold_policy_symmetric():
    """Test that policy is symmetric around 0.5."""
    t = 0.55
    p_up = pd.Series([0.45, 0.5, 0.55])
    
    result = apply_threshold_policy(p_up, t=t)
    
    # 0.45 <= 0.45 (1 - t) -> DOWN (boundary inclusive)
    # 0.5 in band (0.45 < 0.5 < 0.55) -> SIDEWAYS
    # 0.55 >= 0.55 (t) -> UP (boundary inclusive)
    
    assert result.iloc[0] == "DOWN"
    assert result.iloc[1] == "SIDEWAYS"
    assert result.iloc[2] == "UP"


def test_apply_threshold_policy_boundary_inclusive():
    """Test that boundary conditions are inclusive (<= and >=)."""
    t = 0.55
    # Test exact boundaries
    p_up = pd.Series([0.45, 0.55])  # Exactly at boundaries
    
    result = apply_threshold_policy(p_up, t=t)
    
    # 0.45 <= 0.45 (1 - t) -> DOWN (must be DOWN, not SIDEWAYS)
    # 0.55 >= 0.55 (t) -> UP (must be UP, not SIDEWAYS)
    
    assert result.iloc[0] == "DOWN", f"p=0.45 at boundary (1-t=0.45) should be DOWN, got {result.iloc[0]}"
    assert result.iloc[1] == "UP", f"p=0.55 at boundary (t=0.55) should be UP, got {result.iloc[1]}"


def test_confidence_from_p():
    """Test confidence computation."""
    t = 0.6
    
    # Test UP cases
    p_up = pd.Series([0.6, 0.7, 0.8, 1.0])
    conf = confidence_from_p(p_up, t=t)
    
    # For UP: confidence = (p - t) / (1 - t)
    # p=0.6: (0.6 - 0.6) / 0.4 = 0.0
    # p=0.7: (0.7 - 0.6) / 0.4 = 0.25
    # p=0.8: (0.8 - 0.6) / 0.4 = 0.5
    # p=1.0: (1.0 - 0.6) / 0.4 = 1.0
    
    assert conf.iloc[0] == pytest.approx(0.0, abs=1e-6)
    assert conf.iloc[1] == pytest.approx(0.25, abs=1e-6)
    assert conf.iloc[2] == pytest.approx(0.5, abs=1e-6)
    assert conf.iloc[3] == pytest.approx(1.0, abs=1e-6)
    
    # Test DOWN cases
    p_down = pd.Series([0.0, 0.3, 0.4])
    conf_down = confidence_from_p(p_down, t=t)
    
    # For DOWN: confidence = ((1 - t) - p) / (1 - t)
    # p=0.0: (0.4 - 0.0) / 0.4 = 1.0
    # p=0.3: (0.4 - 0.3) / 0.4 = 0.25
    # p=0.4: (0.4 - 0.4) / 0.4 = 0.0
    
    assert conf_down.iloc[0] == pytest.approx(1.0, abs=1e-6)
    assert conf_down.iloc[1] == pytest.approx(0.25, abs=1e-6)
    assert conf_down.iloc[2] == pytest.approx(0.0, abs=1e-6)
    
    # Test SIDEWAYS case
    p_sideways = pd.Series([0.5])
    conf_sideways = confidence_from_p(p_sideways, t=t)
    
    assert conf_sideways.iloc[0] == pytest.approx(0.0, abs=1e-6)


def test_confidence_from_p_clipping():
    """Test that confidence is clipped to [0, 1]."""
    t = 0.6
    
    # Edge cases that might go outside bounds
    p_up = pd.Series([0.5, 0.6, 1.0])
    conf = confidence_from_p(p_up, t=t)
    
    # All should be in [0, 1]
    assert (conf >= 0.0).all()
    assert (conf <= 1.0).all()


def test_confidence_monotonic():
    """Test that confidence increases monotonically with distance from band."""
    t = 0.6
    
    # UP: confidence should increase as p increases
    p_up = pd.Series([0.6, 0.65, 0.7, 0.8, 0.9])
    conf_up = confidence_from_p(p_up, t=t)
    
    assert conf_up.is_monotonic_increasing
    
    # DOWN: confidence should increase as p decreases
    p_down = pd.Series([0.4, 0.35, 0.3, 0.2, 0.1])
    conf_down = confidence_from_p(p_down, t=t)
    
    assert conf_down.is_monotonic_increasing

