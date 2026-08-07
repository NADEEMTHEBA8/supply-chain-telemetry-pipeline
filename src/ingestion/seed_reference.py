"""
Seed PostgreSQL reference tables with machine and supplier master data.

These tables represent the ERP transactional source system. In production,
AWS DMS or Debezium captures CDC events to feed downstream data lake layers.

Usage:
    .venv/bin/python -m ingestion.telemetry_generator.src.seed_reference
"""

from __future__ import annotations

import random

import psycopg2
from psycopg2.extras import execute_values

from config.settings import settings
from ingestion.telemetry_generator.src.profiles import ProfileFactory

NUM_MACHINES = 50
NUM_SUPPLIERS = 20
SEED = 42


def _connect():
    return psycopg2.connect(
        host=settings.pg_host,
        port=settings.pg_port,
        dbname=settings.pg_database,
        user=settings.pg_user,
        password=settings.pg_password,
    )


def main() -> None:
    factory = ProfileFactory(seed=SEED)
    machines = factory.make_machines(NUM_MACHINES)
    suppliers = factory.make_suppliers(NUM_SUPPLIERS)

    machine_rows = [
        (
            m.machine_id,
            m.plant_id,
            m.machine_type,
            m.installed_at,
            m.baseline_temp,
            m.baseline_vibration,
        )
        for m in machines
    ]

    supplier_rows = [
        (
            s.supplier_id,
            s.supplier_name,
            s.part_id,
            s.avg_lead_time_days,
            float(s.unit_cost),
        )
        for s in suppliers
    ]

    rng = random.Random(SEED)
    inventory_rows = []
    warehouses = ["WH_MX_01", "WH_DE_02", "WH_US_03", "WH_CN_04", "WH_IN_05"]
    for s in suppliers:
        inventory_rows.append((
            s.part_id,
            s.supplier_id,
            rng.choice(warehouses),
            rng.randint(50, 2000),
            "AT_WAREHOUSE",
            rng.randint(80, 200),
            rng.randint(40, 100),
        ))

    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("TRUNCATE public.machines, public.suppliers, public.inventory_levels")

            execute_values(
                cur,
                """INSERT INTO public.machines
                   (machine_id, plant_id, machine_type, installed_at,
                    baseline_temp_c, baseline_vibration_hz)
                   VALUES %s""",
                machine_rows,
            )

            execute_values(
                cur,
                """INSERT INTO public.suppliers
                   (supplier_id, supplier_name, part_id,
                    avg_lead_time_days, unit_cost)
                   VALUES %s""",
                supplier_rows,
            )

            execute_values(
                cur,
                """INSERT INTO public.inventory_levels
                   (part_id, supplier_id, warehouse_id, stock_level,
                    transit_status, reorder_point, safety_stock)
                   VALUES %s""",
                inventory_rows,
            )

        print(
            f"Seeded {len(machine_rows)} machines, "
            f"{len(supplier_rows)} suppliers, "
            f"{len(inventory_rows)} inventory records"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
