"""
Mock ML Scoring Service.

Simulates the real-time decision engine (the "Bouncer") that sits in front of the
Feature Store API. It intercepts a dummy transaction, queries the API for the
user's historical features, and runs a mock ML heuristic to ALLOW or BLOCK
the transaction.

12-Factor III compliance: All config sourced from config.settings.
    API URL, API key, and Postgres credentials are injected — not hardcoded.
12-Factor XI compliance: Structured JSON logs via config.logging_config.
    All print() calls replaced with logger.info() / logger.warning() so output
    is captured by Docker logging, Cloud Logging, and Datadog.

Usage:
    make score                    → random user from gold table
    make score USER_ID=user_abc   → specific user
"""

import argparse
import random
import sys
import time

import psycopg2
import requests

from config.logging_config import configure_logging
from config.settings import settings

logger = configure_logging("ml_scorer")


def get_random_user_from_db() -> str:
    """
    Simulate getting a user ID from an active checkout session.

    Connects using AppSettings — identical to every other service in the pipeline.
    In production this would be replaced by reading the user_id from the
    inference request payload (e.g., a Kafka event or REST call from the
    checkout service).
    """
    logger.info(
        "Querying gold table for a random active user",
        extra={"pg_host": settings.pg_host, "pg_port": settings.pg_port},
    )
    try:
        conn = psycopg2.connect(
            host=settings.pg_host,
            port=settings.pg_port,
            database=settings.pg_database,
            user=settings.pg_user,
            password=settings.pg_password,
        )
        cur = conn.cursor()
        cur.execute(
            "SELECT user_id FROM silver_gold.gold_user_fraud_features ORDER BY random() LIMIT 1;"
        )
        res = cur.fetchone()
        conn.close()

        if res:
            return res[0]
        logger.error("No users found in gold table. Run `make demo` first.")
        sys.exit(1)

    except psycopg2.OperationalError:
        logger.exception(
            "Failed to connect to Postgres. Ensure the cluster is running.",
            extra={"pg_host": settings.pg_host, "pg_port": settings.pg_port},
        )
        sys.exit(1)


def score_transaction(user_id: str) -> dict:
    """
    Fetch features from the Feature Store API and run a mock heuristic scorer.

    Returns:
        dict with keys: user_id, risk_score, action, reasons, latency_ms
    """
    logger.info("Scoring transaction", extra={"user_id": user_id})

    api_url = f"http://{settings.api_host}:{settings.api_port}/v1/features/user/{user_id}"
    headers = {"X-API-Key": settings.api_key}

    # ── 1. Fetch Features ─────────────────────────────────────────────────────
    start_time = time.perf_counter()
    try:
        response = requests.get(api_url, headers=headers, timeout=2.0)
    except requests.exceptions.ConnectionError:
        logger.error(
            "Failed to connect to Feature Store API",
            extra={"api_url": api_url},
        )
        sys.exit(1)

    latency_ms = round((time.perf_counter() - start_time) * 1000, 2)
    server_process_time = response.headers.get("X-Process-Time-Ms", "unknown")
    request_id = response.headers.get("X-Request-ID", "unknown")

    if response.status_code == 403:
        logger.error(
            "API returned 403 — check API_KEY in .env.local matches the running API",
            extra={"status_code": 403, "request_id": request_id},
        )
        sys.exit(1)

    if response.status_code != 200:
        logger.error(
            "API returned non-200 status",
            extra={"status_code": response.status_code, "request_id": request_id},
        )
        sys.exit(1)

    features = response.json()["features"]

    logger.info(
        "Features retrieved",
        extra={
            "user_id": user_id,
            "server_process_time_ms": server_process_time,
            "total_latency_ms": latency_ms,
            "request_id": request_id,
        },
    )

    # ── 2. Extract Model Inputs ───────────────────────────────────────────────
    txn_count_24h = features.get("txn_count_24h", 0)
    failure_rate_24h = features.get("failure_rate_24h", 0.0)
    unique_cities_24h = features.get("unique_cities_24h", 1)
    late_night_count = features.get("late_night_txn_count_24h", 0)
    zscore = features.get("latest_amount_zscore", 0.0)

    logger.info(
        "Feature vector extracted",
        extra={
            "user_id": user_id,
            "txn_count_24h": txn_count_24h,
            "failure_rate_24h": failure_rate_24h,
            "unique_cities_24h": unique_cities_24h,
            "late_night_txn_count_24h": late_night_count,
            "latest_amount_zscore": zscore,
        },
    )

    # ── 3. Mock Risk Scoring ──────────────────────────────────────────────────
    # A production model would call a serialised scikit-learn / XGBoost model
    # here. This heuristic mirrors the feature weights a gradient-boosted tree
    # would learn from labelled fraud data.
    risk_score = 0
    reasons: list[str] = []

    if txn_count_24h > 15:
        risk_score += 30
        reasons.append("high_velocity_24h")
    if failure_rate_24h > 0.4:
        risk_score += 40
        reasons.append("extreme_failure_rate_24h")
    if unique_cities_24h > 2:
        risk_score += 30
        reasons.append("impossible_travel_speed")
    if late_night_count > 2:
        risk_score += 20
        reasons.append("suspicious_late_night_activity")
    if float(zscore) > 3.0:
        risk_score += 25
        reasons.append("amount_zscore_anomaly")

    # Baseline noise simulates typical ML score variance (0-15 points).
    risk_score += random.randint(0, 15)
    risk_score = min(risk_score, 100)

    action = "BLOCKED" if risk_score > 75 else "APPROVED"

    logger.warning(
        "Scoring decision",
        extra={
            "user_id": user_id,
            "risk_score": risk_score,
            "action": action,
            "reasons": reasons,
            "latency_ms": latency_ms,
        },
    ) if action == "BLOCKED" else logger.info(
        "Scoring decision",
        extra={
            "user_id": user_id,
            "risk_score": risk_score,
            "action": action,
            "reasons": reasons,
            "latency_ms": latency_ms,
        },
    )

    return {
        "user_id": user_id,
        "risk_score": risk_score,
        "action": action,
        "reasons": reasons,
        "latency_ms": latency_ms,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mock ML transaction scorer")
    parser.add_argument("--user_id", type=str, help="Specific user_id to score")
    args = parser.parse_args()

    user = args.user_id if args.user_id else get_random_user_from_db()
    result = score_transaction(user)

    # Human-readable summary for the terminal (all detail is in the JSON logs above).
    print("\n" + "─" * 52)
    print(f"  User:        {result['user_id']}")
    print(f"  Risk Score:  {result['risk_score']}/100")
    print(f"  Action:      {result['action']}")
    if result["reasons"]:
        print(f"  Signals:     {', '.join(result['reasons'])}")
    print(f"  Latency:     {result['latency_ms']} ms")
    print("─" * 52 + "\n")
