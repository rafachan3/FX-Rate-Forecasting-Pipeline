"""Tests for API prediction mapping logic."""

import pytest

from src.api.models import Direction
from src.api.s3_latest import (
    compute_confidence,
    format_pair_label,
    map_action_to_direction,
)


def test_compute_confidence():
    """Test confidence computation."""
    # High confidence cases
    assert compute_confidence(0.9) == 0.9
    assert compute_confidence(0.1) == 0.9  # max(0.1, 0.9)
    assert compute_confidence(0.95) == 0.95
    
    # Low confidence
    assert compute_confidence(0.5) == 0.5
    assert abs(compute_confidence(0.51) - 0.51) < 0.01
    
    # Edge cases
    assert compute_confidence(0.0) == 1.0
    assert compute_confidence(1.0) == 1.0


def test_map_action_to_direction():
    """Test action to direction mapping."""
    assert map_action_to_direction("UP") == Direction.UP
    assert map_action_to_direction("up") == Direction.UP
    assert map_action_to_direction("DOWN") == Direction.DOWN
    assert map_action_to_direction("down") == Direction.DOWN
    assert map_action_to_direction("ABSTAIN") == Direction.ABSTAIN
    assert map_action_to_direction("SIDEWAYS") == Direction.ABSTAIN
    assert map_action_to_direction("unknown") == Direction.ABSTAIN


def test_format_pair_label():
    """Test pair label formatting."""
    assert format_pair_label("USD_CAD") == "USD/CAD"
    assert format_pair_label("EUR_CAD") == "EUR/CAD"
    assert format_pair_label("GBP_CAD") == "GBP/CAD"
    assert format_pair_label("USD_CAD") == "USD/CAD"

