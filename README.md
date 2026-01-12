# NorthBound FX

[![H7 Daily Pipeline](https://github.com/rafachan3/FX-Rate-Forecasting-Pipeline/actions/workflows/h7_daily.yml/badge.svg)](https://github.com/rafachan3/FX-Rate-Forecasting-Pipeline/actions/workflows/h7_daily.yml)
[![Live Site](https://img.shields.io/badge/Live-northbound--fx.com-blue)](https://www.northbound-fx.com/)

An end-to-end machine learning pipeline for **directional FX forecasting** with automated daily predictions, email alerts, and a [live web dashboard](https://www.northbound-fx.com/).

## 🎯 What It Does

Predicts whether currency pairs (against CAD) will go **UP**, **DOWN**, or **SIDEWAYS** over a 7-business-day horizon, with confidence-gated decision-making.

**Key Design Principles:**
- Probabilistic predictions, not point forecasts
- Confidence gating: abstain when uncertain rather than guess
- Calibration and interpretability over raw accuracy
- Fully automated: data ingestion and ML inference run daily

**Live at:** [northbound-fx.com](https://www.northbound-fx.com/)

### Supported Currency Pairs (23)

All pairs are quoted against the Canadian Dollar (CAD):

| Americas | Europe | Asia-Pacific | Other |
|----------|--------|--------------|-------|
| USD/CAD | EUR/CAD | AUD/CAD | ZAR/CAD |
| MXN/CAD | GBP/CAD | JPY/CAD | SAR/CAD |
| BRL/CAD | CHF/CAD | CNY/CAD | |
| PEN/CAD | NOK/CAD | HKD/CAD | |
| | SEK/CAD | SGD/CAD | |
| | TRY/CAD | KRW/CAD | |
| | RUB/CAD | TWD/CAD | |
| | | INR/CAD | |
| | | NZD/CAD | |
| | | IDR/CAD | |

## 🏗️ Architecture

<!-- TODO: Replace with actual architecture diagram -->
![Architecture Diagram](docs/architecture.png)

*Architecture diagram coming soon*

The system consists of two automated pipelines:

### 1. Data Ingestion Pipeline (AWS Step Functions)
Runs **weekdays at 5:30 PM ET** via EventBridge — 30 minutes after the Bank of Canada Valet API releases daily FX rates.

```
BronzeIngestion → BronzeToSilver → SilverToGold
     (Lambda)         (Lambda)         (Lambda)
```

Each step has success/failure checks with error handling. Data flows through the medallion architecture (Bronze → Silver → Gold) and is stored in S3.

### 2. ML Inference Pipeline (GitHub Actions)
Runs **daily at 6 AM UTC** to generate predictions from the latest gold data.

- Syncs gold data from S3
- Runs logistic regression inference
- Publishes predictions to S3
- Sends email alerts via SendGrid

## ✨ Features

| Component | Description |
|-----------|-------------|
| **Data Pipeline** | Step Functions orchestrating Lambda functions for medallion architecture (Bronze → Silver → Gold) |
| **ML Pipeline** | Logistic regression with expanding-window backtesting, feature engineering, and probability calibration |
| **Daily Automation** | EventBridge triggers data ingestion; GitHub Actions runs ML inference |
| **REST API** | FastAPI on AWS Lambda serving predictions for the web dashboard |
| **Web Dashboard** | Next.js frontend on Vercel at [northbound-fx.com](https://www.northbound-fx.com/) |
| **Email Alerts** | Daily forecast summaries via SendGrid (Mon–Fri at 5:30 PM ET) |

## 🛠️ Tech Stack

**Data Engineering**
- AWS Step Functions (orchestration)
- AWS Lambda (Bronze → Silver → Gold transformations)
- AWS EventBridge (scheduling)
- AWS S3 (data lake)
- Bank of Canada Valet API (data source)

**ML & Analysis**
- Python 3.12, scikit-learn, pandas, NumPy, PyArrow
- Jupyter notebooks for research and backtesting

**API & Backend**
- FastAPI on AWS Lambda + API Gateway
- Neon Postgres (subscriptions)

**CI/CD & Deployment**
- GitHub Actions with OIDC authentication
- Vercel (frontend)
- SendGrid (email delivery)

**Frontend**
- Next.js 14, TypeScript, Tailwind CSS

## 📁 Project Structure

```
FX-Rate-Forecasting-Pipeline/
├── apps/web/              # Next.js frontend (deployed to Vercel)
├── config/                # Pipeline configuration (pairs, S3 paths, email)
├── data/gold/             # Sample gold data for local experimentation
│   └── FXUSDCAD/          # USD/CAD sample included in repo
├── infra/sam/             # AWS SAM templates for API deployment
├── models/                # Trained model artifacts (.joblib, .json)
├── notebooks/             # Research & experimentation (01-07)
├── outputs/               # Generated predictions and manifests (git-ignored)
├── scripts/               # CLI utilities
├── src/
│   ├── api/               # FastAPI application
│   ├── artifacts/         # Manifest building, artifact promotion
│   ├── data_access/       # S3 sync, gold data loading
│   ├── features/          # Feature engineering (h7)
│   ├── lambdas/           # Step Functions Lambda handlers
│   │   ├── bronze_ingestion/   # Bank of Canada API → Bronze
│   │   ├── bronze_to_silver/   # Bronze → Silver transformation
│   │   └── silver_to_gold/     # Silver → Gold transformation
│   ├── models/            # Training, export, inference
│   ├── pipeline/          # Daily runner, config, email, S3 publish
│   └── signals/           # Decision policy (confidence gating)
└── tests/                 # Unit and integration tests
```

## 🚀 Getting Started

### Prerequisites

- Python 3.12+
- Node.js 18+ (for frontend development)

### Installation

```bash
git clone https://github.com/rafachan3/FX-Rate-Forecasting-Pipeline.git
cd FX-Rate-Forecasting-Pipeline

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run tests
pytest -q
```

### Running the Notebooks

The repository includes sample gold data for USD/CAD at `data/gold/FXUSDCAD/data.parquet`. You can run all research notebooks using this sample:

```bash
jupyter notebook notebooks/
```

The notebooks cover the complete ML journey from data QC through model training and calibration.

### Data Access

> **Note:** The S3 bucket containing full gold-layer data is private. The included USD/CAD sample file is sufficient for exploring the notebooks and understanding the pipeline.
>
> If you need access to the complete dataset or production pipeline, please contact the repository owners.

## 📓 Research Notebooks

| # | Notebook | Purpose |
|---|----------|---------|
| 01 | Gold Loader & QC | Data loading, schema validation, date continuity checks |
| 02 | Direction Backtest Baselines | Naive baselines, rolling backtest protocol |
| 03 | Feature Engineering | Leakage-safe features: lagged returns, volatility, momentum, regime flags |
| 04 | Logistic Regression | Expanding-window monthly refit, coefficient stability |
| 05 | Tree-Based Models | HistGradientBoosting comparison (no material improvement) |
| 06 | Decision Policy | Confidence gating, coverage vs accuracy tradeoffs |
| 07 | Probability Calibration | Isotonic regression, ECE, policy-aware calibration |

## 🌐 API

The API powers the [NorthBound website](https://www.northbound-fx.com/) and is publicly accessible.

See [README_API.md](README_API.md) for full documentation.

**Key Endpoints:**

```bash
# Health check
GET /v1/health

# Latest predictions
GET /v1/predictions/h7/latest?pairs=USD_CAD,EUR_CAD

# Subscribe to email alerts
POST /v1/subscriptions
```

## 📊 Output Format

Each prediction includes:

| Field | Description |
|-------|-------------|
| `pair` | Currency pair (e.g., USD_CAD) |
| `direction` | UP, DOWN, or SIDEWAYS |
| `confidence` | Model confidence [0, 1] |
| `obs_date` | Observation date |
| `horizon_bdays` | Forecast horizon (7 business days) |

## 🤝 Contributors

Built by **Rafael Chantres Garcia** and **Ian Vicente Aburto**.

## ⚠️ Disclaimer

This project is for **educational and portfolio purposes only**. It does not constitute financial advice and should not be used for actual trading decisions. Past performance does not guarantee future results.

---

<p align="center">
  <a href="https://www.northbound-fx.com/">northbound-fx.com</a>
</p>
