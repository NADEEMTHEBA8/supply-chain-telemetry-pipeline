from __future__ import annotations

import logging
from datetime import UTC, datetime
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError

from ingestion.telemetry_generator.src.schemas import MachineEvent

logger = logging.getLogger(__name__)


class S3Producer:
    """Streams machine telemetry JSON payloads directly to AWS S3 object storage."""

    def __init__(self, bucket_name: str, region_name: str = "us-east-1") -> None:
        self._bucket_name = bucket_name
        self._client = boto3.client("s3", region_name=region_name)
        self._stats = {"sent": 0, "errors": 0}

    @property
    def stats(self) -> dict[str, int]:
        return self._stats.copy()

    def send(self, telemetry_event: MachineEvent) -> None:
        key = (
            f"raw/machine-telemetry/"
            f"{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_"
            f"{telemetry_event.machine_id}_{uuid4().hex[:6]}.json"
        )
        try:
            self._client.put_object(
                Bucket=self._bucket_name,
                Key=key,
                Body=telemetry_event.to_json(),
                ContentType="application/json",
            )
            self._stats["sent"] += 1
        except ClientError as exc:
            logger.error("S3 object store write failure: %s", exc)
            self._stats["errors"] += 1

    def send_batch(self, telemetry_batch: list[MachineEvent]) -> None:
        if not telemetry_batch:
            return
        key = (
            f"raw/machine-telemetry/"
            f"batch_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_"
            f"{uuid4().hex[:8]}.json"
        )
        body = b"\n".join([event.to_json() for event in telemetry_batch])
        try:
            self._client.put_object(
                Bucket=self._bucket_name,
                Key=key,
                Body=body,
                ContentType="application/json",
            )
            self._stats["sent"] += len(telemetry_batch)
        except ClientError as exc:
            logger.error("S3 batch write failure: %s", exc)
            self._stats["errors"] += len(telemetry_batch)

