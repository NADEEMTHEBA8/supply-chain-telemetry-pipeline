"""
Telemetry data schemas for the Supply Chain pipeline.

Defines the MachineEvent and InventoryEvent dataclasses that mirror
the data model expected by TE Connectivity's supply chain intelligence platform
and the Amazon Connect Decisions Canonical Data Model (CDM).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum


class OperationalStatus(str, Enum):
    RUNNING = "RUNNING"
    IDLE = "IDLE"
    FAULT = "FAULT"
    MAINTENANCE = "MAINTENANCE"


class TransitStatus(str, Enum):
    IN_TRANSIT = "IN_TRANSIT"
    AT_WAREHOUSE = "AT_WAREHOUSE"
    DELIVERED = "DELIVERED"
    DELAYED = "DELAYED"


@dataclass
class MachineEvent:
    """
    Represents a single telemetry reading from a factory floor machine.
    Maps to the 'site' and 'product' entities in the Amazon Connect Decisions CDM.
    Published to Amazon Kinesis Data Streams at ~15,000 events/second in production.
    """

    machine_id: str
    plant_id: str
    event_timestamp: datetime
    temperature_celsius: float
    vibration_hz: float
    pressure_bar: float
    operational_status: OperationalStatus
    error_code: str | None = None
    rpm: float = 0.0
    power_consumption_kw: float = 0.0

    def to_dict(self) -> dict:
        return {
            "machine_id": self.machine_id,
            "plant_id": self.plant_id,
            "event_timestamp": self.event_timestamp.isoformat(),
            "temperature_celsius": self.temperature_celsius,
            "vibration_hz": self.vibration_hz,
            "pressure_bar": self.pressure_bar,
            "operational_status": self.operational_status.value,
            "error_code": self.error_code,
            "rpm": self.rpm,
            "power_consumption_kw": self.power_consumption_kw,
        }


@dataclass
class InventoryEvent:
    """
    Represents a CDC snapshot of inventory levels from the ERP system.
    Captured via Debezium CDC from PostgreSQL (simulating AWS DMS in production).
    Maps directly to the 'inventory_level' entity in the Amazon Connect Decisions CDM.
    """

    part_id: str
    supplier_id: str
    warehouse_id: str
    stock_level: int
    transit_status: TransitStatus
    snapshot_date: datetime
    reorder_point: int = 100
    safety_stock: int = 50
    unit_cost: Decimal = field(default_factory=lambda: Decimal("0.00"))

    def to_dict(self) -> dict:
        return {
            "part_id": self.part_id,
            "supplier_id": self.supplier_id,
            "warehouse_id": self.warehouse_id,
            "stock_level": self.stock_level,
            "transit_status": self.transit_status.value,
            "snapshot_date": self.snapshot_date.isoformat(),
            "reorder_point": self.reorder_point,
            "safety_stock": self.safety_stock,
            "unit_cost": str(self.unit_cost),
        }
