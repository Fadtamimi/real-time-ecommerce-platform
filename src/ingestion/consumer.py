"""Read e-commerce events from Kafka for a learning-sized batch."""

import argparse
import json
import sys
import time
from pathlib import Path

from kafka import KafkaConsumer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from utils.logging_config import get_logger
except ModuleNotFoundError:
    from src.utils.logging_config import get_logger


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CURRENCY = "USD"
SUPPORTED_CURRENCIES = {"AUD", "AED", "BRL", "CAD", "EUR", "GBP", "INR", "JPY", "SAR", "USD"}
LOGGER = get_logger(__name__)


def validate_event(event):
    """Reject malformed messages before they enter the Bronze data layer."""
    required_fields = {"event_id", "customer_id", "product_id", "event_type", "timestamp", "quantity"}
    event_types = {"view", "add_to_cart", "purchase"}
    try:
        return (
            required_fields.issubset(event)
            and all(event[field] not in (None, "") for field in required_fields)
            and event["event_type"] in event_types
            and int(event["quantity"]) > 0
            and event.get("currency", DEFAULT_CURRENCY) in SUPPORTED_CURRENCIES
        )
    except (TypeError, ValueError):
        return False


def normalize_event(event):
    """Add defaults so old and new event schema versions share one shape."""
    normalized = dict(event)
    normalized.setdefault("currency", DEFAULT_CURRENCY)
    return normalized


def create_consumer(broker, topic, group_id):
    """Create a consumer that resumes from the group's committed offset."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda message: json.loads(message.decode("utf-8")),
    )


def load_event_ids(path):
    """Read existing Bronze IDs so retries do not create duplicate records."""
    if not path.exists():
        return set()

    event_ids = set()
    with path.open(encoding="utf-8") as file:
        for line in file:
            event_ids.add(json.loads(line)["event_id"])
    return event_ids


def consume_events(broker, topic, group_id, event_count, output_path, rejected_path):
    """Read a fixed number of events so this demonstration can finish cleanly."""
    started_at = time.perf_counter()
    consumer = create_consumer(broker, topic, group_id)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rejected_path.parent.mkdir(parents=True, exist_ok=True)
    processed_event_ids = load_event_ids(output_path)
    consumed_count = 0
    rejected_count = 0
    duplicate_count = 0
    try:
        with (
            output_path.open("a", encoding="utf-8") as output_file,
            rejected_path.open("a", encoding="utf-8") as rejected_file,
        ):
            for message in consumer:
                event = message.value
                if not validate_event(event):
                    rejected_file.write(json.dumps(event) + "\n")
                    rejected_file.flush()
                    rejected_count += 1
                    LOGGER.warning(
                        "Rejected invalid event",
                        extra={
                            "event": "event_rejected",
                            "topic": topic,
                            "partition": message.partition,
                            "offset": message.offset,
                            "status": "rejected",
                        },
                    )
                    event_count -= 1
                    if event_count == 0:
                        break
                    continue
                event = normalize_event(event)
                if event["event_id"] in processed_event_ids:
                    duplicate_count += 1
                    LOGGER.info(
                        "Skipped duplicate event",
                        extra={
                            "event": "event_duplicate",
                            "topic": topic,
                            "event_id": event["event_id"],
                            "partition": message.partition,
                            "offset": message.offset,
                            "status": "skipped",
                        },
                    )
                    event_count -= 1
                    if event_count == 0:
                        break
                    continue
                output_file.write(json.dumps(event) + "\n")
                output_file.flush()
                processed_event_ids.add(event["event_id"])
                consumed_count += 1
                LOGGER.info(
                    "Stored Bronze event",
                    extra={
                        "event": "bronze_write",
                        "topic": topic,
                        "event_id": event["event_id"],
                        "partition": message.partition,
                        "offset": message.offset,
                        "status": "success",
                    },
                )
                event_count -= 1
                if event_count == 0:
                    break
    finally:
        consumer.close()
        LOGGER.info(
            "Consume batch completed",
            extra={
                "event": "consume_batch",
                "topic": topic,
                "count": consumed_count,
                "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
                "status": "success",
            },
        )
        LOGGER.info(
            "Consume quality metrics",
            extra={
                "event": "consume_quality",
                "topic": topic,
                "count": rejected_count + duplicate_count,
                "status": f"rejected={rejected_count},duplicates={duplicate_count}",
            },
        )


def parse_arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--topic", default="ecommerce-events")
    parser.add_argument("--group", default="ecommerce-learning-consumer")
    parser.add_argument("--count", type=int, default=6)
    parser.add_argument(
        "--output",
        type=Path,
        default=PROJECT_ROOT / "data" / "processed" / "bronze_events.jsonl",
    )
    parser.add_argument(
        "--rejected-output",
        type=Path,
        default=PROJECT_ROOT / "data" / "processed" / "dead_letter_events.jsonl",
    )
    return parser.parse_args()


def main():
    arguments = parse_arguments()
    consume_events(
        arguments.broker,
        arguments.topic,
        arguments.group,
        arguments.count,
        arguments.output,
        arguments.rejected_output,
    )


if __name__ == "__main__":
    main()
