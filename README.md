# FX Rate Forecasting Pipeline

This repository contains a research-oriented pipeline for **directional FX forecasting**, currently focused on the **USD/CAD** pair at a **daily (business-day)** frequency.

The project is explicitly framed around:
- probabilistic **directional prediction** (UP / DOWN),
- **selective decision-making** via confidence gating,
- stability, calibration, and interpretability over raw accuracy.

This is **not** a point-forecasting system and does not attempt to maximize headline accuracy.

---

## Project Scope and Framing

### Key design choices
- **Target**: Direction of cumulative return over a 7-business-day horizon.
- **Output**: Probabilities, not point estimates.
- **Evaluation philosophy**:
  - Weak signals are acceptable if they are stable and well-characterized.
  - Acting less often (abstaining) is preferable to acting noisily.
  - Decision logic is treated as a first-class layer, separate from modeling.

### What this project is *not*
- No intraday or high-frequency modeling.
- No hyperparameter tuning or model ensembling via probability averaging.
- No deep learning.
- No production trading system.

---

## Data Contract

### FX Pair
- USD/CAD

### Frequency
- Daily (business days only)
- Weekend gaps are expected and validated

### Gold Layer Schema (strict)
Each gold parquet file must contain:

| Column       | Description                                  |
|-------------|----------------------------------------------|
| `obs_date`  | Observation date (business day)              |
| `series_id` | Single identifier per file                   |
| `value`     | FX rate value                                |
| `prev_value`| Exact lag-1 value (no gaps, no recomputation)|

Any deviation from this contract is treated as an error.

---

## Repository Structure
```text
.
├── notebooks/
│   ├── 01_gold_loader_and_qc.ipynb
│   ├── 02_direction_backtest_baselines.ipynb
│   ├── 03_direction_feature_engineering.ipynb
│   ├── 04_logistic_regression_direction.ipynb
│   ├── 05_tree_model_direction.ipynb
│   └── 06_decision_policy_confidence_gating.ipynb
│
├── outputs/        # generated artifacts (ignored by git)
├── src/            # shared utilities (minimal, optional)
├── README.md
└── .gitignore
```
---

## Notebook Overview

### 01 — Gold Loader and QC
- Loads gold-layer parquet data.
- Enforces schema, ordering, and lag integrity.
- Validates date continuity (business days only).

### 02 — Direction Backtest Baselines
- Defines directional target (7-day horizon).
- Implements naive and heuristic baselines.
- Introduces rolling backtest protocol.
- Evaluates confidence buckets and coverage vs accuracy.

### 03 — Direction Feature Engineering
- Leakage-safe feature construction.
- Feature groups include:
  - lagged returns,
  - rolling volatility,
  - momentum and z-scores,
  - regime flags,
  - calendar effects.
- Explicit validation against gold data contract.

### 04 — Logistic Regression (Directional Anchor)
- Expanding-window, monthly refit backtest.
- Logistic regression used as an interpretable probability anchor.
- Evaluation includes:
  - overall metrics,
  - confidence buckets,
  - coefficient stability over time.

### 05 — Tree-Based Direction Model
- Controlled non-linear model (HistGradientBoosting).
- Same backtest protocol as logistic regression.
- Feature importance inspection.
- Conclusion: non-linear capacity does not materially improve the signal.

### 06 — Decision Policy and Confidence Gating
- Introduces a **decision layer** on top of model probabilities.
- Evaluates selective prediction strategies:
  - single-model confidence thresholds,
  - agreement gating (logreg + tree),
  - balanced top-k confidence selection.
- Metrics are computed **only on acted subsets**:
  - conditional accuracy,
  - logloss,
  - Brier score,
  - ECE (Expected Calibration Error).
- Includes:
  - coverage tradeoff curves,
  - regime-conditional diagnostics,
  - rolling 1Y stability analysis,
  - worst-year stress tests.
- Locks a default operating policy based on empirical tradeoffs.

---

## Key Findings (Current State)

- The directional signal in USD/CAD exists but is **weak**.
- Confidence stratification is meaningful:
  - higher confidence → better conditional performance.
- Logistic regression provides the most reliable probability estimates.
- Tree models add value primarily through **agreement filtering**, not probability quality.
- Decision logic (when to act vs abstain) has more impact than model complexity.
- The system is stable across regimes but exhibits expected drawdowns.

---

## Outputs

Generated artifacts (ignored by git) include:
- Per-date out-of-sample prediction tables.
- Coverage vs metric sweep CSVs.
- Regime-conditional performance summaries.

These are intended for analysis and visualization, not as committed artifacts.

---


## Status and Next Steps

- Research protocol through decision-layer evaluation is complete.
- Current focus is on:
  - optional post-hoc probability calibration,
  - or freezing the research stack before infrastructure work.

The project prioritizes correctness, interpretability, and stability over aggressive optimization.

---

## Disclaimer

This repository is for research and experimentation only.
It does not constitute trading advice or a production trading system.
