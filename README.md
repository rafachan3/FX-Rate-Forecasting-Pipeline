# FX Rate Forecasting (CAD Focus)

End-to-end pipeline to ingest official FX rates (e.g., USD/CAD, EUR/CAD), store them in Bronze/Silver/Gold layers, run short-horizon forecasts (7–14 days), and serve results via an API + lightweight dashboard.

## Planned Stages
- Bronze: ingest raw FX data (BoC, ECB)
- Silver: normalize into a clean time-series table
- Gold: features + forecasts + metrics
- Modeling: baselines + ETS/ARIMA/Prophet with backtesting
- Serving: FastAPI endpoints
- UI: Streamlit/Dash dashboard

## Quick Start
1. Create and activate a virtual environment
2. Install dependencies (to be added)
3. Run ingestion (to be added)

## Notes
This repo is structured to support reproducible forecasting and evaluation (rolling backtests, metrics tracking, and versioned outputs).
