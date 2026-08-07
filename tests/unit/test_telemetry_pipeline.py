from unittest.mock import MagicMock, patch

import pytest

try:
    from src.ingestion.generator import TelemetryGenerator
    from src.ingestion.kinesis_producer import KinesisProducer
    from src.ingestion.profiles import ProfileFactory
    from src.ingestion.s3_producer import S3Producer
    from src.ingestion.schemas import MachineEvent, OperationalStatus
except ImportError:
    from ingestion.telemetry_generator.src.generator import TelemetryGenerator
    from ingestion.telemetry_generator.src.kinesis_producer import KinesisProducer
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


def test_telemetry_schema_drift_extra_fields(telemetry_generator):
    event = telemetry_generator.generate_machine_event()
    event_dict = event.to_dict()
    event_dict["wifi_signal_strength"] = -42.5  # Firmware upgrade novel field

    event_with_drift = MachineEvent.model_validate(event_dict)
    assert event_with_drift.wifi_signal_strength == -42.5

    serialized_json = event_with_drift.to_json().decode("utf-8")
    assert "wifi_signal_strength" in serialized_json


@patch("boto3.client")
def test_kinesis_producer_partial_batch_retry_success(mock_boto_client, telemetry_generator):
    mock_kinesis = MagicMock()
    # First response returns FailedRecordCount = 1
    mock_kinesis.put_records.side_effect = [
        {
            "FailedRecordCount": 1,
            "Records": [
                {"SequenceNumber": "1"},
                {"ErrorCode": "ProvisionedThroughputExceededException", "ErrorMessage": "Rate exceeded"},
            ],
        },
        {
            "FailedRecordCount": 0,
            "Records": [{"SequenceNumber": "2"}],
        },
    ]
    mock_boto_client.return_value = mock_kinesis

    producer = KinesisProducer(stream_name="test-stream", max_retries=2)
    events = telemetry_generator.generate_batch(2)

    producer.send_batch(events)

    assert producer.stats["sent"] == 2
    assert producer.stats["errors"] == 0
    assert mock_kinesis.put_records.call_count == 2


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
