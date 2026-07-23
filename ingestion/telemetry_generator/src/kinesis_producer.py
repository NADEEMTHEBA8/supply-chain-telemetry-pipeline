"""
Kinesis producer for machine telemetry events.

Publishes machine telemetry events to Amazon Kinesis Data Streams.
Pushes MachineEvent JSON payloads to Amazon Kinesis Data Streams.

Production equivalence: In TE Connectivity's architecture, this is the
equivalent of factory floor IoT sensors publishing to Amazon MSK.
For this portfolio project we use Kinesis (free tier) to demonstrate
real cloud streaming at zero additional cost.
"""

from __future__ import annotations

import json
import logging

import boto3
from botocore.exceptions import ClientError

from ingestion.telemetry_generator.src.schemas import MachineEvent

logger = logging.getLogger(__name__)


class KinesisProducer:
    """
    Thin wrapper around boto3 Kinesis client.
    Publishes MachineEvent dicts to a named Kinesis Data Stream.

    Partition key = machine_id ensures all readings from the same
    machine land in the same shard (preserving event ordering per machine).
    """

    def __init__(self, stream_name: str, region_name: str = "us-east-1") -> None:
        self._stream_name = stream_name
        self._client = boto3.client("kinesis", region_name=region_name)
        self._stats = {"sent": 0, "errors": 0}

    @property
    def stats(self) -> dict:
        return self._stats.copy()

    def send(self, event: MachineEvent) -> None:
        """Publish a single MachineEvent to Kinesis."""
        try:
            self._client.put_record(
                StreamName=self._stream_name,
                Data=json.dumps(event.to_dict()).encode("utf-8"),
                PartitionKey=event.machine_id,  # Shard affinity by machine
            )
            self._stats["sent"] += 1
        except ClientError as exc:
            logger.error("Kinesis put_record failed: %s", exc)
            self._stats["errors"] += 1

    def send_batch(self, events: list[MachineEvent]) -> None:
        """
        Publish up to 500 events in a single put_records call.
        Kinesis put_records max = 500 records or 5MB per call.
        """
        records = [
            {
                "Data": json.dumps(e.to_dict()).encode("utf-8"),
                "PartitionKey": e.machine_id,
            }
            for e in events[:500]
        ]
        try:
            response = self._client.put_records(
                StreamName=self._stream_name,
                Records=records,
            )
            failed = response.get("FailedRecordCount", 0)
            sent = len(records) - failed
            self._stats["sent"] += sent
            self._stats["errors"] += failed
            if failed:
                logger.warning("%d records failed in batch put", failed)
        except ClientError as exc:
            logger.error("Kinesis put_records failed: %s", exc)
            self._stats["errors"] += len(records)
