"""
Telemetry generator runner.

Publishes synthetic factory machine telemetry events to AWS S3.
Simulates the IoT sensor stream that feeds TE Connectivity's
supply chain telemetry platform in production.

Usage:
    .venv/bin/python -m ingestion.telemetry_generator.src.run --events 500
"""

from __future__ import annotations

import argparse
import os
import time

from ingestion.telemetry_generator.src.generator import TelemetryGenerator
from ingestion.telemetry_generator.src.kinesis_producer import KinesisProducer
from ingestion.telemetry_generator.src.profiles import ProfileFactory
from ingestion.telemetry_generator.src.s3_producer import S3Producer
from ingestion.telemetry_generator.src.seed_reference import (
    NUM_MACHINES,
    NUM_SUPPLIERS,
    SEED,
)

PRINT_EVERY = 500
S3_BUCKET = os.environ.get("S3_BUCKET_NAME", "te-supply-chain-telemetry-lake")
KINESIS_STREAM = os.environ.get("KINESIS_STREAM_NAME", "te-supply-chain-telemetry-machine-telemetry")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")


def main() -> None:
    parser = argparse.ArgumentParser(description="Supply chain telemetry generator.")
    parser.add_argument("--rate", type=int, default=10,
                        help="Target events per second (default: 10)")
    parser.add_argument("--firehose", action="store_true",
                        help="Aggressively batch events to Kinesis/S3")
    parser.add_argument("--use-kinesis", action="store_true",
                        help="Use Amazon Kinesis Data Stream producer")
    parser.add_argument("--events", type=int, default=100,
                        help="Stop after this many events")
    args = parser.parse_args()

    print(f"Initialising {NUM_MACHINES} machines / {NUM_SUPPLIERS} suppliers (seed={SEED})")
    factory = ProfileFactory(seed=SEED)
    machines = factory.make_machines(NUM_MACHINES)
    suppliers = factory.make_suppliers(NUM_SUPPLIERS)

    gen = TelemetryGenerator(machines=machines, suppliers=suppliers, seed=SEED)

    use_kinesis = args.use_kinesis
    if use_kinesis and KINESIS_STREAM:
        print(f"Publishing {args.events} events to Kinesis Stream '{KINESIS_STREAM}' [{AWS_REGION}]...")
        producer = KinesisProducer(stream_name=KINESIS_STREAM, region_name=AWS_REGION)
    else:
        print(f"Publishing {args.events} events directly to AWS S3 bucket '{S3_BUCKET}' [{AWS_REGION}]...")
        producer = S3Producer(bucket_name=S3_BUCKET, region_name=AWS_REGION)

    total_sent = 0
    start_time = time.time()

    try:
        if args.firehose or args.use_kinesis:
            batch_size = 1000
            from concurrent.futures import ThreadPoolExecutor
            
            def submit_batch(_):
                nonlocal total_sent
                batch = [gen.generate_machine_event() for _ in range(batch_size)]
                producer.send_batch(batch)
                total_sent += len(batch)
                return len(batch)

            num_batches = (args.events + batch_size - 1) // batch_size
            print(f"🚀 High-Throughput Stream Ingress Active: Target Rate 50,000 tx/sec across {num_batches} batches...")
            
            with ThreadPoolExecutor(max_workers=16) as executor:
                futures = [executor.submit(submit_batch, i) for i in range(num_batches)]
                for f in futures:
                    f.result()
                    elapsed = time.time() - start_time
                    actual_rate = total_sent / elapsed if elapsed > 0 else 0
                    print(
                        f"  sent={total_sent:>6}/{args.events}  "
                        f"rate={actual_rate:.1f}/s  "
                        f"errors={producer.stats['errors']}"
                    )
        else:
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
                        f"  sent={total_sent:>6}/{args.events}  "
                        f"rate={actual_rate:.1f}/s  "
                        f"errors={producer.stats['errors']}"
                    )

    except KeyboardInterrupt:
        pass
    finally:
        elapsed = time.time() - start_time
        stats = producer.stats
        actual_rate = stats["sent"] / elapsed if elapsed > 0 else 0
        print(
            f"\nFinished. sent={stats['sent']}  "
            f"errors={stats['errors']}  "
            f"duration={elapsed:.1f}s  "
            f"avg_rate={actual_rate:.1f}/s"
        )


if __name__ == "__main__":
    main()
