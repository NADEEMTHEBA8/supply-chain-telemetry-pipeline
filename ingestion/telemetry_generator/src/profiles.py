"""
Machine and inventory profile factories.

Generates deterministic pools of machines (plants, types) and suppliers
used by the TelemetryGenerator. Keeping the seed fixed means the
inventory_level CDC values in PostgreSQL stay consistent with what
the generator emits — same deterministic seeding pattern used across all pipeline layers.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

PLANT_IDS = [
    "PLANT_MX_01",  # Monterrey, Mexico — connectors
    "PLANT_DE_02",  # Bensheim, Germany — sensors
    "PLANT_CN_03",  # Shenzhen, China — cable assemblies
    "PLANT_IN_04",  # Pune, India — industrial components
    "PLANT_US_05",  # Winston-Salem, US — aerospace
]

MACHINE_TYPES = [
    "INJECTION_MOLDER",
    "CRIMPING_PRESS",
    "ASSEMBLY_ROBOT",
    "QUALITY_SCANNER",
    "CONVEYOR_SYSTEM",
]

SUPPLIER_PREFIXES = ["SUPP_BASF", "SUPP_DOW", "SUPP_3M", "SUPP_HENKEL", "SUPP_DUPONT"]


@dataclass
class MachineProfile:
    machine_id: str
    plant_id: str
    machine_type: str
    installed_at: datetime
    baseline_temp: float      # normal operating temperature
    baseline_vibration: float # normal vibration frequency


@dataclass
class SupplierProfile:
    supplier_id: str
    supplier_name: str
    part_id: str
    avg_lead_time_days: int
    unit_cost: Decimal


class ProfileFactory:
    """
    Deterministic factory — same seed always produces the same machines
    and suppliers so FK references stay consistent across the pipeline.
    """

    def __init__(self, seed: int = 42) -> None:
        self._rng = random.Random(seed)

    def make_machines(self, n: int) -> list[MachineProfile]:
        machines = []
        for i in range(n):
            plant = self._rng.choice(PLANT_IDS)
            mtype = self._rng.choice(MACHINE_TYPES)
            machines.append(
                MachineProfile(
                    machine_id=f"MCH_{i+1:04d}",
                    plant_id=plant,
                    machine_type=mtype,
                    installed_at=datetime.now(UTC) - timedelta(
                        days=self._rng.randint(30, 1825)
                    ),
                    baseline_temp=self._rng.uniform(60.0, 80.0),
                    baseline_vibration=self._rng.uniform(45.0, 65.0),
                )
            )
        return machines

    def make_suppliers(self, n: int) -> list[SupplierProfile]:
        suppliers = []
        for i in range(n):
            prefix = self._rng.choice(SUPPLIER_PREFIXES)
            suppliers.append(
                SupplierProfile(
                    supplier_id=f"SUPP_{i+1:03d}",
                    supplier_name=f"{prefix}_{i+1:03d}",
                    part_id=f"PART_{self._rng.randint(10000, 99999)}",
                    avg_lead_time_days=self._rng.randint(3, 30),
                    unit_cost=Decimal(str(round(self._rng.uniform(0.5, 250.0), 2))),
                )
            )
        return suppliers
