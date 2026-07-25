import json
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from ingestion.telemetry_generator.src.generator import TelemetryGenerator
from ingestion.telemetry_generator.src.profiles import ProfileFactory
from ingestion.telemetry_generator.src.s3_producer import S3Producer
from ingestion.telemetry_generator.src.schemas import MachineEvent, OperationalStatus


@pytest.fixture
def sample_profiles():
    factory = ProfileFactory(seed=42)
    machines = factory.make_machines(5)
    suppliers = factory.make_suppliers(5)
    return machines, suppliers


@pytest.fixture
def telemetry_generator(sample_profiles):
    machines, suppliers = sample_profiles
    return TelemetryGenerator(machines=machines, suppliers=suppliers, seed=42)


def test_profile_factory_reproducibility():
    factory_a = ProfileFactory(seed=42)
    factory_b = ProfileFactory(seed=42)

    machines_a = factory_a.make_machines(10)
    machines_b = factory_b.make_machines(10)

    assert len(machines_a) == 10
    assert machines_a[0].machine_id == machines_b[0].machine_id
    assert machines_a[0].baseline_temp == machines_b[0].baseline_temp


def test_telemetry_event_generation(telemetry_generator):
    event = telemetry_generator.generate_machine_event()

    assert isinstance(event, MachineEvent)
    assert event.machine_id.startswith("MCH_")
    assert event.plant_id.startswith("PLANT_")
    assert event.temperature_celsius > 0
    assert event.vibration_hz > 0
    assert isinstance(event.operational_status, OperationalStatus)


def test_telemetry_schema_serialization(telemetry_generator):
    event = telemetry_generator.generate_machine_event()
    serialized_bytes = event.to_json()
    payload = json.loads(serialized_bytes.decode("utf-8"))

    assert payload["machine_id"] == event.machine_id
    assert payload["plant_id"] == event.plant_id
    assert payload["temperature_celsius"] == event.temperature_celsius
    assert payload["operational_status"] in [s.value for s in OperationalStatus]


@patch("boto3.client")
def test_s3_producer_send_success(mock_boto_client, telemetry_generator):
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    producer = S3Producer(bucket_name="test-lakehouse-bucket")
    event = telemetry_generator.generate_machine_event()

    producer.send(event)

    assert producer.stats["sent"] == 1
    assert producer.stats["errors"] == 0
    mock_s3.put_object.assert_called_once()


@patch("boto3.client")
def test_s3_producer_send_failure(mock_boto_client, telemetry_generator):
    mock_s3 = MagicMock()
    mock_s3.put_object.side_effect = ClientError(
        {"Error": {"Code": "500", "Message": "Internal Error"}}, "PutObject"
    )
    mock_boto_client.return_value = mock_s3

    producer = S3Producer(bucket_name="test-lakehouse-bucket")
    event = telemetry_generator.generate_machine_event()

    producer.send(event)

    assert producer.stats["sent"] == 0
    assert producer.stats["errors"] == 1
