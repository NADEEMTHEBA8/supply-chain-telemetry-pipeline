from __future__ import annotations

import logging
import random
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError

from ingestion.telemetry_generator.src.schemas import MachineEvent

logger = logging.getLogger(__name__)


class KinesisProducer:
    """Publishes machine telemetry payloads to Amazon Kinesis Data Streams.

    Implements deep payload inspection on put_records responses to identify individual
    record failures (e.g. ProvisionedThroughputExceededException) and retries them
    with exponential backoff and randomized jitter to prevent silent data loss.
    """

    def __init__(self, stream_name: str, region_name: str = "us-east-1", max_retries: int = 4) -> None:
        self._stream_name = stream_name
        self._client = boto3.client("kinesis", region_name=region_name)
        self._max_retries = max_retries
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
        if not telemetry_batch:
            return

        records = [
            {
                "Data": event.to_json(),
                "PartitionKey": event.machine_id,
            }
            for event in telemetry_batch[:500]
        ]

        records_to_send = records
        attempt = 0

        while records_to_send and attempt <= self._max_retries:
            try:
                response = self._client.put_records(
                    StreamName=self._stream_name,
                    Records=records_to_send,
                )
                failed_count = response.get("FailedRecordCount", 0)

                if failed_count == 0:
                    self._stats["sent"] += len(records_to_send)
                    break

                # Deep response inspection: extract failed record payloads for retry
                failed_records: list[dict[str, Any]] = []
                for orig_record, res_entry in zip(records_to_send, response.get("Records", [])):
                    if "ErrorCode" in res_entry:
                        failed_records.append(orig_record)

                successful_in_batch = len(records_to_send) - len(failed_records)
                self._stats["sent"] += successful_in_batch

                if failed_records:
                    attempt += 1
                    if attempt <= self._max_retries:
                        backoff = (2 ** attempt) * 0.1 + random.uniform(0.01, 0.05)
                        logger.warning(
                            "Kinesis batch retry attempt %d/%d for %d failed records (backoff %.2fs)",
                            attempt,
                            self._max_retries,
                            len(failed_records),
                            backoff,
                        )
                        time.sleep(backoff)
                        records_to_send = failed_records
                    else:
                        logger.error(
                            "Kinesis batch permanently failed %d records after %d attempts",
                            len(failed_records),
                            self._max_retries,
                        )
                        self._stats["errors"] += len(failed_records)
                        break

            except ClientError as exc:
                logger.error("Kinesis batch write exception on attempt %d: %s", attempt, exc)
                attempt += 1
                if attempt <= self._max_retries:
                    time.sleep((2 ** attempt) * 0.1)
                else:
                    self._stats["errors"] += len(records_to_send)
                    break
