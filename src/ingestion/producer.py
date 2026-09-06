"""Publish e-commerce events from a JSON source file to Kafka."""

import argparse
import json
from pathlib import Path

from kafka import KafkaProducer


PROJECT_ROOT = Path(__file__).resolve().parents[2]


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
    producer = create_producer(broker)
    try:
        for event in events:
            # The key keeps one customer's events in one partition and preserves their order.
            record = producer.send(topic, key=event["customer_id"], value=event)
            metadata = record.get(timeout=10)
            print(
                f"Published {event['event_id']} "
                f"to partition {metadata.partition} at offset {metadata.offset}"
            )
        producer.flush()
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
