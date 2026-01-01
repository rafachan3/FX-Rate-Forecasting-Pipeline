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

---

## Data Contract

### FX Pair
- USD/CAD

### Frequency
- Daily (business days only)
- Weekend gaps are expected and validated

### Gold Layer Schema (strict)
Each gold parquet file must contain:

| Column        | Description                                  |
|---------------|----------------------------------------------|
| `obs_date`    | Observation date (business day)              |
| `series_id`   | Single identifier per file                   |
| `value`       | FX rate value                                |
| `prev_value`  | Exact lag-1 value (no gaps, no recomputation)|

Any deviation from this contract is treated as an error.

---

## Gold Data Source (Read-Only)

This project consumes **Gold-layer FX data** produced by an upstream ingestion pipeline.

Key properties:
- Gold data is **authoritative and immutable**
- This repository **does not generate raw FX data**
- All modeling, evaluation, and artifacts assume the Gold contract is already satisfied

For local research and development, Gold data can be synced into a local parquet file via a small CLI utility.  
The local copy is treated as **read-only input** and is excluded from version control.

---

## Syncing Gold Data Locally

Gold data can be downloaded locally using the provided sync script.

Example (USD/CAD):

```bash
python -m scripts.sync_gold \
  --series FXUSDCAD \
  --out data/data-USD-CAD.parquet \
  --with-watermark
```
This command:
- downloads the latest Gold parquet for the requested series,
- optionally retrieves the associated watermark metadata,
- writes a local, UI-ready parquet file for downstream notebooks and scripts.


## Repository Structure

```text
FX-Rate-Forecasting-Pipeline
│
├── data/
│   └── data-USD-CAD.parquet          # gold-layer FX data (local, ignored)
│
├── notebooks/
│   ├── 01_gold_loader_and_qc.ipynb
│   ├── 02_direction_backtest_baselines.ipynb
│   ├── 03_direction_feature_engineering.ipynb
│   ├── 04_logistic_regression_direction.ipynb
│   ├── 05_tree_model_direction.ipynb
│   ├── 06_decision_policy_confidence_gating.ipynb
│   └── 07_probability_calibration.ipynb
│
├── src/
│   └── artifacts/
│       └── write_latest.py           # UI-ready artifact writer
│
├── outputs/                          # generated artifacts (git-ignored)
│
├── requirements.txt                  # minimal, intentional dependencies
├── README.md
└── .gitignore

```
## Notebook Overview

### 01 — Gold Loader and QC
- Loads gold-layer parquet data.
- Enforces schema, ordering, and lag integrity.
- Validates date continuity (business days only).

---

### 02 — Direction Backtest Baselines
- Defines directional target (7-day horizon).
- Implements naive and heuristic baselines.
- Introduces rolling backtest protocol.
- Evaluates confidence buckets and coverage vs accuracy.

---

### 03 — Direction Feature Engineering
- Leakage-safe feature construction.
- Feature groups include:
  - lagged returns,
  - rolling volatility,
  - momentum and z-scores,
  - regime flags,
  - calendar effects.
- Explicit validation against gold data contract.

---

### 04 — Logistic Regression (Directional Anchor)
- Expanding-window, monthly refit backtest.
- Logistic regression used as an interpretable probability anchor.
- Evaluation includes:
  - overall metrics,
  - confidence buckets,
  - coefficient stability over time.

---

### 05 — Tree-Based Direction Model
- Controlled non-linear model (HistGradientBoosting).
- Same backtest protocol as logistic regression.
- Feature importance inspection.
- Conclusion: non-linear capacity does not materially improve the signal.

---

### 06 — Decision Policy and Confidence Gating
- Introduces a **decision layer** on top of model probabilities.
- Evaluates selective prediction strategies:
  - single-model confidence thresholds,
  - agreement gating (logreg + tree),
  - balanced top-k confidence selection.
- Metrics are computed **only on acted subsets**:
  - conditional accuracy,
  - log loss,
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

#### Calibration protocol
- Expanding-window monthly refit.
- Rolling calibration window drawn strictly from past data.
- Calibrators fit only on historical predictions.
- No leakage into test months.

#### Methods
- Isotonic regression with safe fallback to identity when data is insufficient.

#### Evaluation focuses on
- LogLoss, Brier score, and ECE (10- and 20-bin).
- Reliability (calibration) curves.
- Impact of calibration on decision policies from Notebook 06.

#### Key findings
- Calibration materially improves probability reliability (lower ECE).
- No improvement in directional accuracy.
- Threshold-gating policies may degrade under calibration.
- Probability treatment must be **policy-aware**.

#### Operational takeaway
- Logistic regression:
  - raw probabilities preferred for threshold gating,
  - calibrated probabilities preferred for ranking-based policies.
- Tree models:
  - calibrated probabilities preferred in all cases.

---

## Output Artifacts (UI Contract)

The pipeline produces stable artifacts intended for UI consumption and scheduled inference runs.  
All artifacts are written to `outputs/` and are ignored by git.

### Canonical daily artifact (UI-facing)

**`outputs/predictions_latest_h7.parquet`**

One row per business day (`obs_date`) containing probabilities, decisions, and metadata.

#### Stable schema
- `obs_date`
- `pair`
- `horizon_bdays`
- `model_version` (git SHA)
- `p_up_logreg_raw`, `p_up_tree_raw`
- `p_up_logreg_cal`, `p_up_tree_cal`
- `policy_name`
- `policy_params` (JSON)
- `action` ∈ {`UP`, `DOWN`, `ABSTAIN`}
- `confidence` ∈ [0, 1]
- `coverage_target` (nullable)
- `regime` (nullable)

---

### Run metadata

**`outputs/run_metadata_h7.json`**

- `pair`
- `horizon_bdays`
- `as_of_date`
- `data_start`, `data_end`
- `model_version`
- `calibration_enabled`
- `policy_name`
- `policy_params`

---

### Research and diagnostic artifacts

Generated by notebooks for diagnostics and visualization:
- `outputs/decision_policy_sweep.csv`
- `outputs/decision_policy_by_regime.csv`
- `outputs/calibration_metrics_overall.csv`
- `outputs/calibration_reliability_bins.csv`
- `outputs/calibration_policy_sweep.csv`

---

## Current Status and Next Steps

The research pipeline is feature-complete and stable:
- directional modeling,
- confidence-gated decision policies,
- regime-aware diagnostics,
- post-hoc probability calibration.

The signal is weak but stable, and decision behavior is well-characterized.

**Next steps are operational, not statistical**
- scheduled inference (e.g., AWS),
- artifact materialization for UI consumption,
- monitoring for data drift and policy stability.

No further model complexity is planned.

---

## Disclaimer

This repository is for research and experimentation only.  
It does not constitute trading advice or a production trading system.
