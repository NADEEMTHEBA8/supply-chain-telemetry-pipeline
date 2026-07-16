"""
Seed the Postgres reference tables (public.users, public.merchants).

These tables are the CDC source. The transaction generator (run.py) builds
its in-memory profiles from the same ProfileFactory seed, so the user_id and
merchant_id values it emits resolve against the rows written here. Keep
NUM_USERS / NUM_MERCHANTS / SEED in sync with run.py (run.py imports them).

Run once after the Postgres container is up:
    python -m ingestion.transaction_generator.src.seed_reference
"""

from __future__ import annotations

import random

import psycopg2
from psycopg2.extras import execute_values

from config.settings import settings
from ingestion.transaction_generator.src.profiles import ProfileFactory

# Shared with run.py — the generator regenerates the identical profile set
# from this seed, which is how transaction FKs stay valid.
NUM_USERS = 5000
NUM_MERCHANTS = 500
SEED = 42


def _connect():
    """Connect to Postgres using AppSettings (12-Factor III compliant)."""
    return psycopg2.connect(
        host=settings.pg_host,
        port=settings.pg_port,
        dbname=settings.pg_database,
        user=settings.pg_user,
        password=settings.pg_password,
    )


def main() -> None:
    factory = ProfileFactory(seed=SEED)
    users = factory.make_users(NUM_USERS)
    merchants = factory.make_merchants(NUM_MERCHANTS)

    # Deterministic blocked/inactive flags (not part of ProfileFactory output).
    flag_rng = random.Random(SEED)
    user_rows = [
        (
            u.user_id,
            u.email_hash,
            u.phone_hash,
            u.city,
            u.country,
            u.kyc_status,
            u.risk_score,
            flag_rng.random() < 0.02,
            u.account_created_at,
            u.account_created_at,
        )
        for u in users
    ]
    merchant_rows = [
        (
            m.merchant_id,
            m.merchant_name,
            m.category,
            m.risk_tier,
            m.avg_ticket_size,
            flag_rng.random() < 0.03,
            m.onboarded_at,
            m.onboarded_at,
        )
        for m in merchants
    ]

    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            # Idempotent: truncate so re-seeding is a clean reset.
            cur.execute("TRUNCATE public.users, public.merchants")
            execute_values(
                cur,
                "INSERT INTO public.users "
                "(user_id, email_hash, phone_hash, city, country, kyc_level, "
                "risk_score, is_blocked, created_at, updated_at) VALUES %s",
                user_rows,
            )
            execute_values(
                cur,
                "INSERT INTO public.merchants "
                "(merchant_id, merchant_name, category, risk_tier, "
                "avg_ticket_size, is_active, onboarded_at, updated_at) VALUES %s",
                merchant_rows,
            )
        print(f"Seeded {len(user_rows)} users, {len(merchant_rows)} merchants")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
