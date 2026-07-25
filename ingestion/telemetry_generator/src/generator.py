"""
Factory machine telemetry generator.

Produces realistic machine sensor readings that simulate TE Connectivity's
factory floor telemetry streams. In production, these events flow through
Amazon Kinesis Data Streams at ~15,000 events/second.

Key design: occasional fault injection (overheating, vibration spikes,
pressure anomalies) to create realistic supply risk signals that the
downstream Gold model (gold_supply_risk) is designed to detect.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime

from ingestion.telemetry_generator.src.profiles import MachineProfile, SupplierProfile
from ingestion.telemetry_generator.src.schemas import (
    InventoryEvent,
    MachineEvent,
    OperationalStatus,
    TransitStatus,
)

# Probability of an error code being emitted per event
ERROR_CODES = ["E001_OVERHEAT", "E002_VIBRATION", "E003_PRESSURE", "E004_BEARING_WEAR"]
ERROR_PROBABILITY = 0.03  # 3% of events carry an error code

# Probability of operational anomaly
STATUS_WEIGHTS = {
    OperationalStatus.RUNNING: 88,
    OperationalStatus.IDLE: 8,
    OperationalStatus.FAULT: 3,
    OperationalStatus.MAINTENANCE: 1,
}


class TelemetryGenerator:
    """
    Generates realistic machine telemetry events from a pool of machine profiles.

    Temperature and vibration readings follow a lognormal distribution centred
    on each machine's baseline, with injected spikes to simulate fault conditions.
    Generates realistic machine sensor data for the TE Connectivity supply chain platform.

    Usage:
        factory = ProfileFactory(seed=42)
        gen = TelemetryGenerator(
            machines=factory.make_machines(50),
            suppliers=factory.make_suppliers(20),
            seed=42,
        )
        event = gen.generate_machine_event()
    """

    def __init__(
        self,
        machines: list[MachineProfile],
        suppliers: list[SupplierProfile],
        seed: int | None = None,
    ) -> None:
        if not machines:
            raise ValueError("Need at least one machine profile")
        if not suppliers:
            raise ValueError("Need at least one supplier profile")

        self._machines = machines
        self._suppliers = suppliers
        self._rng = random.Random(seed)

    # ── public API ────────────────────────────────────────────────────────────

    def generate_machine_event(self) -> MachineEvent:
        """Generate one machine telemetry reading."""
        machine = self._rng.choice(self._machines)
        status = self._pick_status()
        is_fault = status == OperationalStatus.FAULT

        # Inject anomalous readings on fault events — this is what the
        # gold_supply_risk dbt model is designed to detect.
        temp = self._generate_temperature(machine.baseline_temp, fault=is_fault)
        vibration = self._generate_vibration(machine.baseline_vibration, fault=is_fault)
        pressure = self._generate_pressure(fault=is_fault)
        error_code = self._pick_error_code() if is_fault else None

        return MachineEvent(
            machine_id=machine.machine_id,
            plant_id=machine.plant_id,
            event_timestamp=datetime.now(UTC),
            temperature_celsius=temp,
            vibration_hz=vibration,
            pressure_bar=pressure,
            operational_status=status,
            error_code=error_code,
            rpm=self._generate_rpm(fault=is_fault),
            power_consumption_kw=round(self._rng.uniform(2.5, 45.0), 2),
        )

    def generate_inventory_event(self) -> InventoryEvent:
        """Generate one inventory level snapshot (simulates ERP CDC via Debezium/AWS DMS)."""
        supplier = self._rng.choice(self._suppliers)
        stock = self._rng.randint(0, 2000)
        reorder_point = self._rng.randint(80, 200)
        transit = self._pick_transit_status()

        return InventoryEvent(
            part_id=supplier.part_id,
            supplier_id=supplier.supplier_id,
            warehouse_id=self._rng.choice(
                ["WH_MX_01", "WH_DE_02", "WH_US_03", "WH_CN_04", "WH_IN_05"]
            ),
            stock_level=stock,
            transit_status=transit,
            snapshot_date=datetime.now(UTC),
            reorder_point=reorder_point,
            safety_stock=self._rng.randint(40, 100),
            unit_cost=supplier.unit_cost,
        )

    def generate_batch(self, n: int) -> list[MachineEvent]:
        """Generate n machine telemetry events."""
        return [self.generate_machine_event() for _ in range(n)]

    # ── private helpers ───────────────────────────────────────────────────────

    def _generate_temperature(self, baseline: float, fault: bool) -> float:
        """Lognormal temperature centred on machine baseline; spikes on fault."""
        if fault:
            # Fault events push temperature 20-50% above baseline
            spike = self._rng.uniform(1.20, 1.50)
            return round(baseline * spike + self._rng.gauss(0, 2.0), 2)
        return round(
            self._rng.lognormvariate(0, 0.05) * baseline + self._rng.gauss(0, 1.5),
            2,
        )

    def _generate_vibration(self, baseline: float, fault: bool) -> float:
        if fault:
            spike = self._rng.uniform(1.30, 2.00)
            return round(baseline * spike, 2)
        return round(
            self._rng.lognormvariate(0, 0.04) * baseline + self._rng.gauss(0, 1.0),
            2,
        )

    def _generate_pressure(self, fault: bool) -> float:
        if fault:
            return round(self._rng.uniform(5.0, 8.0), 2)  # Above normal range
        return round(self._rng.uniform(2.8, 4.2), 2)

    def _generate_rpm(self, fault: bool) -> float:
        if fault:
            return round(self._rng.uniform(0, 200), 1)  # Low/erratic RPM
        return round(self._rng.uniform(800, 3600), 1)

    def _pick_status(self) -> OperationalStatus:
        statuses = list(STATUS_WEIGHTS.keys())
        weights = list(STATUS_WEIGHTS.values())
        return self._rng.choices(statuses, weights=weights, k=1)[0]

    def _pick_error_code(self) -> str:
        return self._rng.choice(ERROR_CODES)

    def _pick_transit_status(self) -> TransitStatus:
        return self._rng.choices(
            list(TransitStatus),
            weights=[40, 35, 20, 5],
            k=1,
        )[0]
