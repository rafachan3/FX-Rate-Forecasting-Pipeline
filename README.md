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

### 07 — Post-hoc Probability Calibration

This notebook evaluates **post-hoc probability calibration** under a strictly out-of-sample protocol. Calibration is treated as a probability-quality tool, not a signal-enhancement technique.

Calibration protocol:
- Expanding-window monthly refit.
- A rolling calibration window drawn strictly from past data.
- Calibrators are fit only on historical predictions and applied to future test periods.
- No leakage into the test month.

Methods:
- Isotonic regression with safe fallback to identity when data is insufficient.

Evaluation focuses on:
- LogLoss, Brier score, and ECE (10- and 20-bin).
- Reliability (calibration) curves.
- Impact of calibration on decision policies defined in Notebook 06.

Key findings:
- Calibration materially improves probability reliability (lower ECE), especially for:
  - balanced top-k policies,
  - tree-based probabilities.
- Calibration does **not** improve directional accuracy.
- Calibration can degrade threshold-gating performance by shrinking extreme probabilities.
- Probability treatment must therefore be **policy-aware**.

Operational takeaway:
- Logistic regression:
  - raw probabilities preferred for threshold-based gating,
  - calibrated probabilities preferred for ranking-based (top-k) policies.
- Tree models:
  - calibrated probabilities preferred in all cases.

---

## Output Artifacts (UI Contract)

The pipeline produces a small number of stable artifacts intended for UI consumption and scheduled inference runs. All artifacts are written to `outputs/` and are ignored by git.

### Canonical daily artifact (UI-facing)
**`outputs/predictions_latest_h7.parquet`**

One row per business day (`obs_date`) containing model probabilities, decision outputs, and metadata.

Stable schema:
- `obs_date`
- `pair` (e.g., `USDCAD`)
- `horizon_bdays` (fixed at 7)
- `model_version` (git SHA)
- `p_up_logreg_raw`, `p_up_tree_raw`
- `p_up_logreg_cal`, `p_up_tree_cal` (nullable if calibration disabled)
- `policy_name`
- `policy_params` (JSON string)
- `action` ∈ {`UP`, `DOWN`, `ABSTAIN`}
- `confidence` ∈ [0, 1]
- `coverage_target` (nullable)
- `regime` (nullable)

This artifact is the primary input for any UI or downstream consumer.

### Run metadata
**`outputs/run_metadata_h7.json`**

Small JSON file describing the most recent inference run:
- `pair`
- `horizon_bdays`
- `as_of_date`
- `data_start`, `data_end`
- `model_version`
- `calibration_enabled`
- `policy_name`
- `policy_params`

### Research and diagnostic artifacts
These are generated by notebooks and are intended for diagnostics and UI visualization, not for daily inference logic:
- `outputs/decision_policy_sweep.csv`
- `outputs/decision_policy_by_regime.csv`
- `outputs/calibration_metrics_overall.csv`
- `outputs/calibration_reliability_bins.csv`
- `outputs/calibration_policy_sweep.csv`

---

## Current Status and Next Steps

At this stage, the research pipeline includes:
- directional modeling,
- confidence-gated decision policies,
- regime-aware diagnostics,
- post-hoc probability calibration.

The signal is weak but stable, and decision behavior is well-characterized.

Next steps are primarily **operational**, not statistical:
- scheduled inference (e.g., AWS),
- artifact materialization for UI consumption,
- monitoring for data drift and policy stability.

No further model complexity is planned at this stage.

---

## Disclaimer

This repository is for research and experimentation only.
It does not constitute trading advice or a production trading system.