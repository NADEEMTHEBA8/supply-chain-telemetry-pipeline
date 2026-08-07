from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class OperationalStatus(StrEnum):
    RUNNING = "RUNNING"
    IDLE = "IDLE"
    FAULT = "FAULT"
    MAINTENANCE = "MAINTENANCE"


class TransitStatus(StrEnum):
    IN_TRANSIT = "IN_TRANSIT"
    AT_WAREHOUSE = "AT_WAREHOUSE"
    DELIVERED = "DELIVERED"
    DELAYED = "DELAYED"


class MachineEvent(BaseModel):
    """Domain model for machine sensor telemetry emitted on factory floors.

    Configured with extra='allow' so novel firmware fields (e.g. ambient_humidity)
    pass through serialization to Kinesis/S3, delegating schema evolution to
    downstream Databricks Auto Loader rescue mode.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

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
        data = self.model_dump()
        data["event_timestamp"] = self.event_timestamp.isoformat()
        data["operational_status"] = self.operational_status.value
        return data

    def to_json(self) -> bytes:
        return self.model_dump_json().encode("utf-8")


class InventoryEvent(BaseModel):
    """CDC event payload representing ERP inventory balances."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    part_id: str
    supplier_id: str
    warehouse_id: str
    stock_level: int
    transit_status: TransitStatus
    snapshot_date: datetime
    reorder_point: int = 100
    safety_stock: int = 50
    unit_cost: Decimal = Field(default_factory=lambda: Decimal("0.00"))

    def to_dict(self) -> dict:
        data = self.model_dump()
        data["snapshot_date"] = self.snapshot_date.isoformat()
        data["transit_status"] = self.transit_status.value
        data["unit_cost"] = str(self.unit_cost)
        return data

    def to_json(self) -> bytes:
        return self.model_dump_json().encode("utf-8")
