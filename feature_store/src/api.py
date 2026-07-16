"""
Fraud Feature Serving API.

Exposes precomputed fraud features from Redis over HTTP.
Includes basic latency monitoring and API versioning.

12-Factor III compliance: All configuration sourced from config.settings.
12-Factor XI compliance: Structured JSON logs via config.logging_config.
No hardcoded secrets, host addresses, or port numbers appear in this module.
"""

import json
import time
import uuid
from collections.abc import Generator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import redis
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, Security
from fastapi.responses import JSONResponse
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel, Field

from config.logging_config import configure_logging
from config.settings import settings

# Structured JSON logger — output parseable by Cloud Logging / Datadog / Splunk.
# Filter in production: jsonPayload.service = "feature_api" AND level = "ERROR"
logger = configure_logging("feature_api")

# --- Configuration & State ---
FRESHNESS_LIMIT_HOURS = 25
API_KEY_NAME = "X-API-Key"

api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=True)

# Global connection pool managed by lifespan
redis_client: redis.Redis = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    global redis_client
    redis_client = redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=0,
        decode_responses=True,
    )
    try:
        redis_client.ping()
        logger.info(
            "Feature API started — Redis pool ready",
            extra={"redis_host": settings.redis_host, "redis_port": settings.redis_port},
        )
    except redis.ConnectionError:
        logger.error(
            "Feature API started but Redis is unreachable",
            extra={"redis_host": settings.redis_host, "redis_port": settings.redis_port},
        )
    yield
    if redis_client:
        redis_client.close()
        logger.info("Redis pool closed.")


app = FastAPI(
    title="Fraud Feature Store API",
    description="Low-latency feature serving API for ML inference.",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
)


# --- Middleware ---
@app.middleware("http")
async def observability_middleware(request: Request, call_next):
    """
    Injects a correlation ID and emits a structured access log per request.

    X-Request-ID:
        Generated server-side if not provided by the caller.
        Propagate this header from your API gateway / load balancer to get
        end-to-end traceability across microservices.

    Structured log fields emitted per request:
        method, path, status_code, duration_ms, request_id
        → Filterable in Cloud Logging / Datadog without regex:
          jsonPayload.status_code >= 500 AND jsonPayload.service = "feature_api"
    """
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    start_time = time.perf_counter()

    response = await call_next(request)

    duration_ms = (time.perf_counter() - start_time) * 1000
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Process-Time-Ms"] = f"{duration_ms:.2f}"

    log_level = logger.warning if response.status_code >= 400 else logger.info
    log_level(
        "%s %s → %d",
        request.method,
        request.url.path,
        response.status_code,
        extra={
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "duration_ms": round(duration_ms, 2),
            "request_id": request_id,
        },
    )
    return response


# --- Dependencies ---
def get_api_key(api_key: str = Security(api_key_header)) -> str:
    """
    Validate the incoming API key against the value injected from AppSettings.

    In production, settings.api_key is set via Kubernetes Secret or
    GCP Secret Manager — never hardcoded in source.
    """
    if api_key != settings.api_key:
        raise HTTPException(status_code=403, detail="Invalid API Key. Access denied.")
    return api_key


def get_redis() -> Generator[redis.Redis, None, None]:
    """Dependency injection for the Redis client."""
    if not redis_client:
        raise HTTPException(status_code=500, detail="Redis client not initialized")
    yield redis_client


# --- Models ---
class BatchRequest(BaseModel):
    user_ids: list[str] = Field(..., max_length=100, description="List of max 100 user IDs")


class FeatureResponse(BaseModel):
    user_id: str
    features: dict
    served_at: str


class HealthResponse(BaseModel):
    status: str
    redis: str
    features_age_hours: float | None = None
    last_loaded_at: str | None = None


# --- V1 Router ---
v1_router = APIRouter(prefix="/v1", tags=["Features V1"])


@v1_router.get("/features/user/{user_id}", response_model=FeatureResponse)
def get_user_features(
    user_id: str, api_key: str = Depends(get_api_key), r: redis.Redis = Depends(get_redis)
):
    """Retrieve all precomputed fraud features for a single user."""
    raw = r.get(f"user:features:{user_id}")
    if raw is None:
        raise HTTPException(
            status_code=404,
            detail=f"No features found for user '{user_id}'.",
        )
    return FeatureResponse(
        user_id=user_id,
        features=json.loads(raw),
        served_at=datetime.now(UTC).isoformat(),
    )


@v1_router.post("/features/batch")
def get_batch_features(
    request: BatchRequest, api_key: str = Depends(get_api_key), r: redis.Redis = Depends(get_redis)
):
    """Multi-user feature lookup via a single Redis MGET for high-throughput scoring."""
    keys = [f"user:features:{uid}" for uid in request.user_ids]
    results = r.mget(keys)

    found = []
    missing = []
    for user_id, raw in zip(request.user_ids, results):
        if raw is not None:
            found.append({"user_id": user_id, "features": json.loads(raw)})
        else:
            missing.append(user_id)

    return {
        "results": found,
        "summary": {
            "requested": len(request.user_ids),
            "found": len(found),
            "missing": len(missing),
            "missing_ids": missing,
        },
        "served_at": datetime.now(UTC).isoformat(),
    }


# --- Base Router (Unauthenticated) ---
@app.get("/health", response_model=HealthResponse, tags=["System"])
def health_check(r: redis.Redis = Depends(get_redis)):
    """System health and data freshness monitor. Unauthenticated."""
    try:
        r.ping()
    except redis.ConnectionError:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "redis": "disconnected"},
        )

    meta_raw = r.get("_meta:features:last_loaded")
    if not meta_raw:
        return HealthResponse(status="degraded", redis="connected")

    meta = json.loads(meta_raw)
    last_loaded = datetime.fromisoformat(meta["last_loaded_at"])
    age_hours = (datetime.now(UTC) - last_loaded).total_seconds() / 3600

    return HealthResponse(
        status="healthy" if age_hours < FRESHNESS_LIMIT_HOURS else "degraded",
        redis="connected",
        features_age_hours=round(age_hours, 2),
        last_loaded_at=meta["last_loaded_at"],
    )


# Attach routers
app.include_router(v1_router)
