"""
Decision policy module for converting probabilities to directional signals.

Standard labels: "UP", "DOWN", "SIDEWAYS"
"""
from __future__ import annotations

import pandas as pd
import numpy as np


def normalize_label(label: str | None) -> str | None:
    """
    Normalize signal labels to standard terminology.
    
    Converts "ABSTAIN" -> "SIDEWAYS" for backward compatibility.
    Other labels are returned as-is.
    
    Args:
        label: Input label (e.g., "ABSTAIN", "UP", "DOWN", "SIDEWAYS")
        
    Returns:
        Normalized label or None if input is None
    """
    if label is None:
        return None
    label_str = str(label).upper().strip()
    if label_str == "ABSTAIN":
        return "SIDEWAYS"
    return label_str if label_str in ("UP", "DOWN", "SIDEWAYS") else label_str


def apply_threshold_policy(p_up: pd.Series, t: float) -> pd.Series:
    """
    Apply symmetric threshold policy to probability series.
    
    Creates a "sideways band" around 0.5:
    - UP if p >= t
    - DOWN if p <= 1 - t
    - SIDEWAYS otherwise
    
    Args:
        p_up: Series of probabilities (P(y=1))
        t: Threshold (typically 0.5 < t <= 1.0)
        
    Returns:
        Series of string labels: "UP", "DOWN", or "SIDEWAYS"
    """
    if not (0.5 < t < 1.0):
        raise ValueError("t must be in (0.5, 1.0)")
    
    # Use small epsilon to handle floating point precision at boundaries
    EPS = 1e-10
    threshold_low = 1.0 - t
    
    up_mask = p_up >= (t - EPS)
    down_mask = p_up <= (threshold_low + EPS)
    
    out = pd.Series("SIDEWAYS", index=p_up.index, dtype="object")
    out[down_mask] = "DOWN"
    out[up_mask] = "UP"
    return out


def confidence_from_p(p_up: pd.Series, t: float) -> pd.Series:
    """
    Compute confidence scores from probabilities using threshold policy.
    
    Confidence is computed as:
    - For UP: confidence = (p - t) / (1 - t) clipped to [0, 1]
    - For DOWN: confidence = ((1 - t) - p) / (1 - t) clipped to [0, 1]
    - For SIDEWAYS: confidence = 0.0
    
    Args:
        p_up: Series of probabilities (P(y=1))
        t: Threshold (typically 0.5 < t <= 1.0)
        
    Returns:
        Series of confidence values in [0, 1]
    """
    decisions = apply_threshold_policy(p_up, t)
    confidence = pd.Series(0.0, index=p_up.index, dtype=float)
    
    # UP case: confidence = (p - t) / (1 - t)
    up_mask = decisions == "UP"
    if up_mask.any():
        p_up_vals = p_up.loc[up_mask]
        conf_up = (p_up_vals - t) / (1.0 - t)
        confidence.loc[up_mask] = np.clip(conf_up, 0.0, 1.0)
    
    # DOWN case: confidence = ((1 - t) - p) / (1 - t)
    down_mask = decisions == "DOWN"
    if down_mask.any():
        p_down_vals = p_up.loc[down_mask]
        conf_down = ((1.0 - t) - p_down_vals) / (1.0 - t)
        confidence.loc[down_mask] = np.clip(conf_down, 0.0, 1.0)
    
    # SIDEWAYS case: confidence = 0.0 (already set)
    
    return confidence

