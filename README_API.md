# NorthBound API

Production-ready API for serving FX predictions and managing subscriptions.

## Architecture

- **Runtime**: FastAPI on AWS Lambda
- **API Gateway**: HTTP API (v2)
- **Storage**: 
  - Predictions: S3 (`fx-rate-pipeline-dev/predictions/h7/latest/`)
  - Subscriptions: DynamoDB (`northbound_subscriptions`)
- **Caching**: In-process TTL cache (60s default)

## Local Development

### Prerequisites

```bash
# Install API dependencies
pip install -r requirements-api.txt
```

### Run Locally

#### Option 1: Local Mode (No AWS Required)

Load predictions from local filesystem instead of S3:

```bash
# Set environment variables
export STAGE=local
export LOCAL_LATEST_DIR=outputs/latest  # Path to local predictions directory
export DDB_TABLE=northbound_subscriptions  # Can use dummy value if not testing subscriptions
export AWS_REGION=us-east-2
export CORS_ORIGINS=*

# Ensure local files exist:
# - outputs/latest/manifest.json
# - outputs/latest/latest_USD_CAD_h7.json
# - outputs/latest/latest_EUR_CAD_h7.json
# etc.

# Run with uvicorn
uvicorn src.api.app:app --reload --host 0.0.0.0 --port 8000
```

#### Option 2: S3 Mode (Requires AWS Credentials)

```bash
# Set environment variables
export STAGE=local
export S3_BUCKET=fx-rate-pipeline-dev
export S3_PREFIX=predictions/h7/latest/
export DDB_TABLE=northbound_subscriptions
export AWS_REGION=us-east-2
export CORS_ORIGINS=*

# Run with uvicorn
uvicorn src.api.app:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at `http://localhost:8000`

**Note**: When `LOCAL_LATEST_DIR` is set, the API loads predictions from the local filesystem and does not require AWS credentials or S3 permissions.

### Test Endpoints Locally

```bash
# Health check
curl http://localhost:8000/v1/health

# Get predictions (default pairs)
curl http://localhost:8000/v1/predictions/h7/latest

# Get predictions (specific pairs)
curl "http://localhost:8000/v1/predictions/h7/latest?pairs=USD_CAD,EUR_CAD"

# Create subscription
curl -X POST http://localhost:8000/v1/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "email": "test@example.com",
    "pairs": ["USD_CAD", "EUR_CAD"],
    "frequency": "WEEKLY",
    "weekly_day": "WED"
  }'

# Unsubscribe
curl -X POST http://localhost:8000/v1/subscriptions/unsubscribe \
  -H "Content-Type: application/json" \
  -d '{"email": "test@example.com"}'
```

## AWS Deployment (SAM)

### Prerequisites

```bash
# Install AWS SAM CLI
# macOS: brew install aws-sam-cli
# Linux: https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html

# Verify installation
sam --version
```

### Build

```bash
# Build SAM application
cd infra/sam
sam build
```

### Deploy

```bash
# Guided deploy (first time)
sam deploy --guided

# Subsequent deploys
sam deploy

# Deploy with specific parameters
sam deploy \
  --parameter-overrides \
    Stage=dev \
    CorsOrigins=https://yourdomain.com \
    SubscribeApiKey=your-secret-key
```

### Deploy Outputs

After deployment, SAM will output:
- `ApiUrl`: API Gateway endpoint URL
- `SubscriptionsTableName`: DynamoDB table name

### Test Deployed API

```bash
# Replace <API_URL> with output from deployment
API_URL="https://xxxxx.execute-api.us-east-2.amazonaws.com/dev"

# Health check
curl $API_URL/v1/health

# Get predictions
curl "$API_URL/v1/predictions/h7/latest?pairs=USD_CAD"

# Create subscription (with API key if configured)
curl -X POST $API_URL/v1/subscriptions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-api-key" \
  -d '{
    "email": "test@example.com",
    "pairs": ["USD_CAD"],
    "frequency": "WEEKLY"
  }'
```

## Testing

```bash
# Run API tests
pytest tests/test_api_mapping.py tests/test_subscriptions_validation.py -v
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `STAGE` | `local` | Deployment stage |
| `S3_BUCKET` | `fx-rate-pipeline-dev` | S3 bucket name |
| `S3_PREFIX` | `predictions/h7/latest/` | S3 prefix for predictions |
| `DDB_TABLE` | `northbound_subscriptions` | DynamoDB table name |
| `AWS_REGION` | `us-east-2` | AWS region |
| `CORS_ORIGINS` | `*` | Comma-separated CORS origins |
| `SUBSCRIBE_API_KEY` | (none) | Optional API key for subscribe endpoints |
| `EMAIL_ENABLED` | `false` | Enable email delivery (currently disabled) |
| `CACHE_TTL` | `60` | Cache TTL in seconds |

## API Endpoints

### GET /v1/health

Health check endpoint.

**Response:**
```json
{
  "ok": true,
  "service": "northbound-api",
  "env": "dev",
  "time_utc": "2024-01-01T12:00:00Z",
  "email_enabled": false
}
```

### GET /v1/predictions/h7/latest

Get latest predictions for FX pairs.

**Query Parameters:**
- `pairs` (optional): Comma-separated pair codes (e.g., `USD_CAD,EUR_CAD`). Default: `USD_CAD,EUR_CAD`
- `limit` (optional): Number of rows per pair. Default: `3` (currently only `1` is supported)

**Response:**
```json
{
  "horizon": "h7",
  "as_of_utc": "2024-01-01T12:00:00Z",
  "run_date": "2024-01-01",
  "timezone": "America/Toronto",
  "git_sha": "abc123...",
  "items": [
    {
      "pair": "USD_CAD",
      "pair_label": "USD/CAD",
      "generated_at": "2024-01-01T12:00:00Z",
      "obs_date": "2024-01-01",
      "direction": "UP",
      "confidence": 0.82,
      "model": "logreg",
      "raw": {
        "p_up": 0.82
      }
    }
  ]
}
```

### POST /v1/subscriptions

Create or update subscription.

**Request:**
```json
{
  "email": "user@example.com",
  "pairs": ["USD_CAD", "EUR_CAD"],
  "frequency": "WEEKLY",
  "weekly_day": "WED",
  "monthly_timing": null
}
```

**Response:**
```json
{
  "status": "created_or_updated",
  "email": "user@example.com",
  "subscription_id": "abc123def456",
  "email_enabled": false,
  "message": "Subscription saved. Email delivery is temporarily disabled."
}
```

### POST /v1/subscriptions/unsubscribe

Unsubscribe email.

**Request:**
```json
{
  "email": "user@example.com"
}
```

**Response:**
```json
{
  "status": "unsubscribed",
  "email": "user@example.com"
}
```

## Error Responses

All errors follow this format:

```json
{
  "error": {
    "code": "ERROR_CODE",
    "message": "Human-readable error message",
    "request_id": "request-id-here"
  }
}
```

## Notes

- **Local Dev Mode**: Set `LOCAL_LATEST_DIR=outputs/latest` to load predictions from local filesystem. No AWS credentials required. Files must match S3 structure: `manifest.json` and `latest_{PAIR}_h7.json` in the specified directory.
- **Caching**: Predictions are cached in-process for 60 seconds (configurable via `CACHE_TTL`). Cache works for both S3 and local modes.
- **Missing Pairs**: If a requested pair file doesn't exist (S3 or local), the API returns `ABSTAIN` with `confidence: 0.0` rather than failing
- **Email Delivery**: Currently disabled (`EMAIL_ENABLED=false`). Subscriptions are stored but not processed
- **API Key**: Optional. If `SUBSCRIBE_API_KEY` is set, subscribe endpoints require `Authorization: Bearer <key>` header

