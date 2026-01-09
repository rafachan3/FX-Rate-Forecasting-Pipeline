"""FastAPI application for NorthBound API."""

import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from src.api.config import config
from src.api.logging import log_error, log_request
from src.api.models import (
    ErrorDetail,
    ErrorResponse,
    HealthResponse,
    PredictionsResponse,
    SubscriptionRequest,
    SubscriptionResponse,
    UnsubscribeRequest,
    UnsubscribeResponse,
)
from src.api.s3_latest import get_latest_predictions, get_manifest_metadata
from src.api.subscriptions import create_or_update_subscription, unsubscribe


def get_request_id(request: Request) -> str:
    """Extract request ID from headers or generate one."""
    # API Gateway provides request ID in headers
    request_id = request.headers.get("x-request-id") or request.headers.get(
        "x-amzn-requestid"
    )
    if not request_id:
        # Generate simple ID from timestamp
        request_id = f"local-{int(time.time() * 1000)}"
    return request_id


def verify_api_key(authorization: Optional[str] = Header(None)) -> None:
    """Verify API key if configured."""
    if not config.SUBSCRIBE_API_KEY:
        return  # No key required
    
    if not authorization:
        raise HTTPException(
            status_code=401,
            detail={"error": {"code": "MISSING_API_KEY", "message": "API key required"}},
        )
    
    # Extract key from "Bearer <key>" or just "<key>"
    key = authorization.replace("Bearer ", "").strip()
    if key != config.SUBSCRIBE_API_KEY:
        raise HTTPException(
            status_code=401,
            detail={"error": {"code": "INVALID_API_KEY", "message": "Invalid API key"}},
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown."""
    # Startup
    yield
    # Shutdown (if needed)


app = FastAPI(
    title="NorthBound API",
    description="API for FX signal predictions and subscriptions",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/v1/health", response_model=HealthResponse)
async def health():
    """Health check endpoint."""
    return HealthResponse(
        ok=True,
        service="northbound-api",
        env=config.STAGE,
        time_utc=datetime.now(timezone.utc).isoformat(),
        email_enabled=config.EMAIL_ENABLED,
    )


@app.get("/v1/predictions/h7/latest", response_model=PredictionsResponse)
async def get_predictions(
    request: Request,
    pairs: Optional[str] = Query(
        None,
        description="Comma-separated pair codes (e.g., USD_CAD,EUR_CAD)",
    ),
    limit: int = Query(3, ge=1, le=10, description="Number of rows per pair (currently only 1 supported)"),
):
    """Get latest predictions for requested pairs."""
    start_time = time.time()
    request_id = get_request_id(request)
    
    try:
        # Parse pairs
        if pairs:
            pair_list = [p.strip().upper() for p in pairs.split(",")]
        else:
            pair_list = ["USD_CAD", "EUR_CAD"]
        
        # Get predictions
        items = get_latest_predictions(pair_list, limit=1)  # Only support limit=1 for now
        
        # Get manifest metadata
        metadata = get_manifest_metadata()
        
        # Build response
        response = PredictionsResponse(
            horizon=metadata["horizon"],
            as_of_utc=metadata["as_of_utc"],
            run_date=metadata["run_date"],
            timezone=metadata["timezone"],
            git_sha=metadata["git_sha"],
            items=items,
        )
        
        duration_ms = (time.time() - start_time) * 1000
        log_request(
            method="GET",
            path="/v1/predictions/h7/latest",
            status=200,
            duration_ms=duration_ms,
            request_id=request_id,
            pairs=len(pair_list),
        )
        
        # Set cache headers
        headers = {
            "Cache-Control": "public, max-age=30, stale-while-revalidate=120",
        }
        
        return Response(
            content=response.model_dump_json(),
            media_type="application/json",
            headers=headers,
        )
    
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        log_error(
            message=f"Failed to get predictions: {str(e)}",
            error_code="PREDICTIONS_ERROR",
            request_id=request_id,
            exc_info=e,
        )
        
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "code": "PREDICTIONS_ERROR",
                    "message": "Failed to load predictions",
                    "request_id": request_id,
                }
            },
        ) from e


@app.post("/v1/subscriptions", response_model=SubscriptionResponse)
async def create_subscription(
    request: Request,
    subscription: SubscriptionRequest,
    authorization: Optional[str] = Header(None),
):
    """Create or update subscription."""
    start_time = time.time()
    request_id = get_request_id(request)
    
    # Verify API key if configured
    try:
        verify_api_key(authorization)
    except HTTPException:
        raise
    
    try:
        result = create_or_update_subscription(subscription)
        
        duration_ms = (time.time() - start_time) * 1000
        log_request(
            method="POST",
            path="/v1/subscriptions",
            status=200,
            duration_ms=duration_ms,
            request_id=request_id,
        )
        
        return SubscriptionResponse(**result)
    
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        log_error(
            message=f"Failed to create subscription: {str(e)}",
            error_code="SUBSCRIPTION_ERROR",
            request_id=request_id,
            exc_info=e,
        )
        
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "code": "SUBSCRIPTION_ERROR",
                    "message": "Failed to save subscription",
                    "request_id": request_id,
                }
            },
        ) from e


@app.post("/v1/subscriptions/unsubscribe", response_model=UnsubscribeResponse)
async def unsubscribe_endpoint(
    request: Request,
    unsubscribe_req: UnsubscribeRequest,
    authorization: Optional[str] = Header(None),
):
    """Unsubscribe email."""
    start_time = time.time()
    request_id = get_request_id(request)
    
    # Verify API key if configured
    try:
        verify_api_key(authorization)
    except HTTPException:
        raise
    
    try:
        result = unsubscribe(unsubscribe_req.email)
        
        duration_ms = (time.time() - start_time) * 1000
        log_request(
            method="POST",
            path="/v1/subscriptions/unsubscribe",
            status=200,
            duration_ms=duration_ms,
            request_id=request_id,
        )
        
        return UnsubscribeResponse(**result)
    
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        log_error(
            message=f"Failed to unsubscribe: {str(e)}",
            error_code="UNSUBSCRIBE_ERROR",
            request_id=request_id,
            exc_info=e,
        )
        
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "code": "UNSUBSCRIBE_ERROR",
                    "message": "Failed to unsubscribe",
                    "request_id": request_id,
                }
            },
        ) from e


# Lambda handler for AWS
def handler(event, context):
    """AWS Lambda handler using Mangum."""
    try:
        from mangum import Mangum
        mangum_app = Mangum(app)
        return mangum_app(event, context)
    except ImportError:
        raise RuntimeError(
            "mangum is required for Lambda deployment. Install with: pip install mangum"
        )

