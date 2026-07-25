from __future__ import annotations

import logging

import boto3
from botocore.exceptions import ClientError

from ingestion.telemetry_generator.src.schemas import MachineEvent

logger = logging.getLogger(__name__)


class KinesisProducer:
    """Publishes machine telemetry payloads to Amazon Kinesis Data Streams."""

    def __init__(self, stream_name: str, region_name: str = "us-east-1") -> None:
        self._stream_name = stream_name
        self._client = boto3.client("kinesis", region_name=region_name)
        self._stats = {"sent": 0, "errors": 0}

    @property
    def stats(self) -> dict[str, int]:
        return self._stats.copy()

    def send(self, telemetry_event: MachineEvent) -> None:
        try:
            self._client.put_record(
                StreamName=self._stream_name,
                Data=telemetry_event.to_json(),
                PartitionKey=telemetry_event.machine_id,
            )
            self._stats["sent"] += 1
        except ClientError as exc:
            logger.error("Kinesis put_record failure: %s", exc)
            self._stats["errors"] += 1

    def send_batch(self, telemetry_batch: list[MachineEvent]) -> None:
        records = [
            {
                "Data": event.to_json(),
                "PartitionKey": event.machine_id,
            }
            for event in telemetry_batch[:500]
        ]
        try:
            response = self._client.put_records(
                StreamName=self._stream_name,
                Records=records,
            )
            failed_count = response.get("FailedRecordCount", 0)
            self._stats["sent"] += len(records) - failed_count
            self._stats["errors"] += failed_count
        except ClientError as exc:
            logger.error("Kinesis batch write failure: %s", exc)
            self._stats["errors"] += len(records)
