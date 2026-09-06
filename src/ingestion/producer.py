"""Publish e-commerce events from a JSON source file to Kafka."""

import argparse
import json
import sys
import time
from pathlib import Path

from kafka import KafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from utils.logging_config import get_logger
except ModuleNotFoundError:
    from src.utils.logging_config import get_logger


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOGGER = get_logger(__name__)


def load_events(path):
    """Read source events before publishing them to the streaming system."""
    with path.open(encoding="utf-8") as file:
        return json.load(file)


def create_producer(broker):
    """Create a producer that converts Python dictionaries into JSON messages."""
    return KafkaProducer(
        bootstrap_servers=broker,
        key_serializer=lambda customer_id: customer_id.encode("utf-8"),
        value_serializer=lambda event: json.dumps(event).encode("utf-8"),
    )


def publish_events(events, broker, topic):
    """Publish events and wait until Kafka acknowledges every message."""
    started_at = time.perf_counter()
    producer = create_producer(broker)
    published_count = 0
    try:
        for event in events:
            # The key keeps one customer's events in one partition and preserves their order.
            record = producer.send(topic, key=event["customer_id"], value=event)
            metadata = record.get(timeout=10)
            published_count += 1
            LOGGER.info(
                "Kafka event published",
                extra={
                    "event": "kafka_publish",
                    "topic": topic,
                    "event_id": event["event_id"],
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                    "status": "success",
                },
            )
        producer.flush()
        LOGGER.info(
            "Publish batch completed",
            extra={
                "event": "publish_batch",
                "topic": topic,
                "count": published_count,
                "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
                "status": "success",
            },
        )
    finally:
        producer.close()


def parse_arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=PROJECT_ROOT / "data" / "raw" / "events.json",
    )
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--topic", default="ecommerce-events")
    return parser.parse_args()


def main():
    arguments = parse_arguments()
    events = load_events(arguments.input)
    publish_events(events, arguments.broker, arguments.topic)
    print(f"Published {len(events)} events to {arguments.topic}")


if __name__ == "__main__":
    main()
