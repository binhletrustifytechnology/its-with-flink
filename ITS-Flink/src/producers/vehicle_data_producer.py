"""
Reads vehicle_data.csv and publishes each row as a JSON message to Kafka.

Usage:
    python src/producers/vehicle_data_producer.py [--delay SECONDS] [--csv PATH]

Options:
    --delay   Seconds to wait between messages (default: 0.1)
    --csv     Path to CSV file (default: resources/vehicle_data.csv)
"""

import argparse
import csv
import json
import os
import time
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

load_dotenv(Path(__file__).parents[2] / ".env")

# ── NULL handling ─────────────────────────────────────────────────────────────

def _coerce(value: str):
    """Convert CSV string values to appropriate Python types."""
    if value in ("NULL", ""):
        return None
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


def parse_row(row: dict) -> dict:
    return {key: _coerce(val) for key, val in row.items()}


# ── Producer ──────────────────────────────────────────────────────────────────

def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def produce(csv_path: str, delay: float) -> None:
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    topic             = os.environ.get("KAFKA_TOPIC",             "topic-sensors-2")

    print(f"Connecting to Kafka at {bootstrap_servers} ...")
    try:
        producer = build_producer(bootstrap_servers)
    except NoBrokersAvailable:
        print("ERROR: No Kafka brokers available. Check KAFKA_BOOTSTRAP_SERVERS in .env")
        raise

    print(f"Reading {csv_path}")
    print(f"Publishing to topic '{topic}' with {delay}s delay between messages\n")

    sent = 0
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            message = parse_row(row)

            # Use serial_number as the partition key so records from the
            # same sensor always go to the same partition (ordering guarantee)
            key = message.get("serial_number")

            future = producer.send(topic, key=key, value=message)
            future.get(timeout=10)  # block until broker confirms receipt

            sent += 1
            print(f"[{sent:>4}] sent → serial={message.get('serial_number')} "
                  f"ts={message.get('timestamp_seconds')} "
                  f"message_number={message.get('message_number')} "
                  f"class={message.get('vehicle_class')} "
                  f"volume={message.get('vehicle_volume')}")

            if delay > 0:
                time.sleep(delay)

    producer.flush()
    producer.close()
    print(f"\nDone. {sent} messages published to '{topic}'.")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    project_root = Path(__file__).parents[2]

    file_name = 'data-1776848658945.csv'

    parser = argparse.ArgumentParser(description=f"Publish {file_name} to Kafka")
    parser.add_argument(
        "--delay", type=float, default=0.1,
        help="Seconds between messages (default: 0.1)",
    )
    parser.add_argument(
        "--csv", type=str,
        default=str(project_root / "resources" / f"{file_name}"),
        help="Path to CSV file",
    )
    args = parser.parse_args()

    produce(csv_path=args.csv, delay=args.delay)
