-- Supply Chain Reference Tables
-- Replaces the fraud pipeline's users/merchants tables.
-- These are the CDC source tables — Debezium tails the WAL and publishes
-- changes to Kafka topics that flow into the Bronze S3 layer.
-- In production, AWS DMS replicates this ERP data directly.

-- Drop old fraud tables if they exist
DROP TABLE IF EXISTS public.merchants CASCADE;
DROP TABLE IF EXISTS public.users CASCADE;

-- Machine master data (maps to Amazon Connect Decisions 'site' entity)
CREATE TABLE IF NOT EXISTS public.machines (
    machine_id              VARCHAR(20)  PRIMARY KEY,
    plant_id                VARCHAR(20)  NOT NULL,
    machine_type            VARCHAR(50)  NOT NULL,
    installed_at            TIMESTAMPTZ  NOT NULL,
    baseline_temp_c         DOUBLE PRECISION NOT NULL,
    baseline_vibration_hz   DOUBLE PRECISION NOT NULL,
    is_active               BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Supplier master data (maps to Amazon Connect Decisions 'tpartner' entity)
CREATE TABLE IF NOT EXISTS public.suppliers (
    supplier_id             VARCHAR(20)  PRIMARY KEY,
    supplier_name           VARCHAR(100) NOT NULL,
    part_id                 VARCHAR(20)  NOT NULL,
    avg_lead_time_days      INTEGER      NOT NULL,
    unit_cost               NUMERIC(10, 2) NOT NULL,
    is_active               BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Inventory levels (maps to Amazon Connect Decisions 'inventory_level' entity)
CREATE TABLE IF NOT EXISTS public.inventory_levels (
    id                      SERIAL       PRIMARY KEY,
    part_id                 VARCHAR(20)  NOT NULL,
    supplier_id             VARCHAR(20)  REFERENCES public.suppliers(supplier_id),
    warehouse_id            VARCHAR(20)  NOT NULL,
    stock_level             INTEGER      NOT NULL DEFAULT 0,
    transit_status          VARCHAR(20)  NOT NULL DEFAULT 'AT_WAREHOUSE',
    reorder_point           INTEGER      NOT NULL DEFAULT 100,
    safety_stock            INTEGER      NOT NULL DEFAULT 50,
    snapshot_date           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Enable logical replication for Debezium CDC
-- (Simulates AWS DMS change data capture from ERP system)
ALTER TABLE public.machines     REPLICA IDENTITY FULL;
ALTER TABLE public.suppliers    REPLICA IDENTITY FULL;
ALTER TABLE public.inventory_levels REPLICA IDENTITY FULL;

-- Indexes for CDC and query performance
CREATE INDEX IF NOT EXISTS idx_machines_plant_id    ON public.machines(plant_id);
CREATE INDEX IF NOT EXISTS idx_inventory_part_id    ON public.inventory_levels(part_id);
CREATE INDEX IF NOT EXISTS idx_inventory_warehouse  ON public.inventory_levels(warehouse_id);
CREATE INDEX IF NOT EXISTS idx_inventory_updated    ON public.inventory_levels(updated_at);
