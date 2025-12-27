# FX Rate Forecasting Pipeline (USD/CAD) — Direction + Confidence

This project builds a reproducible forecasting pipeline for **USD/CAD** using daily data.
Instead of predicting exact FX levels (often close to a random walk), the current focus is:

> **Predict the direction of USD/CAD over the next 7 business days + a confidence score.**

The work is currently **notebook-first** to maximize iteration speed and documentation quality.
Once the approach is stable, we will refactor notebooks into a clean `src/` package.

---

## Data

Primary dataset: **Gold-layer parquet** containing USD/CAD observations and engineered features.

- File: `data/data-USD-CAD.parquet`
- Key columns:
  - `obs_date` (business-day index)
  - `value` (USD/CAD FX rate)
  - engineered features: returns, lags, rolling stats, calendar flags


---

## Current Modeling Target

We define a 7-business-day forward return:

- `target_return_7d = value[t+7] / value[t] - 1`
- `target_direction_7d = 1 if target_return_7d > 0 else 0`

---

## Evaluation Philosophy

We evaluate both:
- **Classification accuracy** (direction)
- **Probabilistic quality**: log loss, Brier score
- **Confidence gating**: performance vs. coverage (e.g., accuracy when confidence ≥ 0.60)

This matches a realistic decision-support product: models should be allowed to say **"no strong signal"**.

---

## Notebooks

### 01 — Gold Loader + QC
`notebooks/01_gold_loader_and_qc.ipynb`

- Loads gold parquet
- Validates date range and missingness
- Defines target for `H=7`
- Builds feature matrix `X` and labels `y`

### 02 — Direction Baselines Backtest
`notebooks/02_direction_backtest_baselines.ipynb`

- Rolling expanding-window backtest for direction at `H=7`
- Baselines:
  - Coinflip probability (0.5)
  - Momentum probability (return / volatility → sigmoid)
  - Logistic regression baseline
- Saves outputs to `outputs/`

---

## Outputs

By default, the backtest notebook saves:

- `outputs/direction_baseline_backtest_rows.csv`
- `outputs/direction_baseline_metrics_overall.csv`
- `outputs/direction_baseline_metrics_by_confidence.csv`

(Outputs are typically ignored from version control.)

---

## Next Step

Add a stronger model (XGBoost) and compare against baselines with the same rolling backtest + confidence metrics.
