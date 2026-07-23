"""
Telemetry generator runner.

Publishes synthetic factory machine telemetry events to Amazon Kinesis
Data Streams. Simulates the IoT sensor stream that feeds TE Connectivity's
supply chain telemetry platform in production.

12-Factor III compliance: AWS region and stream name are injected via
environment variables. No hardcoded configuration.

Usage:
    python -m ingestion.telemetry_generator.src.run --events 500
    python -m ingestion.telemetry_generator.src.run --rate 50 --firehose
"""

from __future__ import annotations

import argparse
import os
import time

from ingestion.telemetry_generator.src.generator import TelemetryGenerator
from ingestion.telemetry_generator.src.kinesis_producer import KinesisProducer
from ingestion.telemetry_generator.src.profiles import ProfileFactory
from ingestion.telemetry_generator.src.seed_reference import (
    NUM_MACHINES,
    NUM_SUPPLIERS,
    SEED,
)

PRINT_EVERY = 50
KINESIS_STREAM = os.environ.get("KINESIS_STREAM_NAME", "te-machine-telemetry")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")


def main() -> None:
    parser = argparse.ArgumentParser(description="Supply chain telemetry generator.")
    parser.add_argument("--rate", type=int, default=10,
                        help="Target events per second (default: 10)")
    parser.add_argument("--firehose", action="store_true",
                        help="Aggressively batch events without sleep")
    parser.add_argument("--events", type=int, default=0,
                        help="Stop after this many events (0 = unlimited)")
    args = parser.parse_args()

    print(f"Initialising {NUM_MACHINES} machines / {NUM_SUPPLIERS} suppliers (seed={SEED})")
    factory = ProfileFactory(seed=SEED)
    machines = factory.make_machines(NUM_MACHINES)
    suppliers = factory.make_suppliers(NUM_SUPPLIERS)

    gen = TelemetryGenerator(machines=machines, suppliers=suppliers, seed=SEED)
    producer = KinesisProducer(stream_name=KINESIS_STREAM, region_name=AWS_REGION)

    mode = "FIREHOSE" if args.firehose else f"~{args.rate} events/s"
    print(f"Publishing to Kinesis stream '{KINESIS_STREAM}' [{AWS_REGION}] — {mode}")
    print("Press Ctrl+C to stop.\n")

    delay = 1.0 / args.rate if args.rate > 0 else 0
    total_sent = 0
    start_time = time.time()

    try:
        while True:
            event = gen.generate_machine_event()
            producer.send(event)
            total_sent += 1

            if args.events > 0 and total_sent >= args.events:
                break

            if total_sent % PRINT_EVERY == 0:
                elapsed = time.time() - start_time
                actual_rate = total_sent / elapsed if elapsed > 0 else 0
                print(
                    f"  sent={total_sent:>6}  "
                    f"rate={actual_rate:.1f}/s  "
                    f"errors={producer.stats['errors']}"
                )

            if not args.firehose and delay > 0:
                time.sleep(delay)

    except KeyboardInterrupt:
        pass
    finally:
        elapsed = time.time() - start_time
        stats = producer.stats
        actual_rate = stats["sent"] / elapsed if elapsed > 0 else 0
        print(
            f"\nStopped. sent={stats['sent']}  "
            f"errors={stats['errors']}  "
            f"duration={elapsed:.1f}s  "
            f"avg_rate={actual_rate:.1f}/s"
        )


if __name__ == "__main__":
    main()
