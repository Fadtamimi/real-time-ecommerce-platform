"""Clean Bronze events and enrich them with customer and product attributes."""

import argparse
import csv
import json
import sys
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from utils.logging_config import get_logger
except ModuleNotFoundError:
    from src.utils.logging_config import get_logger


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOGGER = get_logger(__name__)


def load_csv_by_id(path, identifier):
    """Load a reference table into a lookup dictionary for efficient joins."""
    with path.open(newline="", encoding="utf-8") as file:
        return {row[identifier]: row for row in csv.DictReader(file)}


def transform_event(event, customers, products):
    """Standardize one event and add the fields needed by downstream analytics."""
    customer = customers[event["customer_id"]]
    product = products[event["product_id"]]
    quantity = int(event["quantity"])
    price = Decimal(product["price"])
    is_purchase = event["event_type"] == "purchase"

    return {
        "event_id": event["event_id"],
        "customer_id": event["customer_id"],
        "customer_country": customer["country"],
        "product_id": event["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "event_type": event["event_type"],
        "currency": event.get("currency", "USD"),
        "event_timestamp": datetime.fromisoformat(
            event["timestamp"].replace("Z", "+00:00")
        ).isoformat(),
        "quantity": quantity,
        "unit_price": float(price),
        "total_amount": float(price * quantity) if is_purchase else 0.0,
    }


def transform_file(bronze_path, customers_path, products_path, silver_path):
    """Read Bronze JSONL and write a fresh, deterministic Silver JSONL file."""
    started_at = time.perf_counter()
    customers = load_csv_by_id(customers_path, "customer_id")
    products = load_csv_by_id(products_path, "product_id")
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0

    with bronze_path.open(encoding="utf-8") as source, silver_path.open(
        "w", encoding="utf-8"
    ) as destination:
        for line in source:
            event = json.loads(line)
            enriched_event = transform_event(event, customers, products)
            destination.write(json.dumps(enriched_event) + "\n")
            row_count += 1
    LOGGER.info(
        "Silver transformation completed",
        extra={
            "event": "silver_transform",
            "count": row_count,
            "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
            "status": "success",
        },
    )


def parse_arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bronze",
        type=Path,
        default=PROJECT_ROOT / "data" / "processed" / "bronze_demo.jsonl",
    )
    parser.add_argument(
        "--customers",
        type=Path,
        default=PROJECT_ROOT / "data" / "raw" / "customers.csv",
    )
    parser.add_argument(
        "--products",
        type=Path,
        default=PROJECT_ROOT / "data" / "raw" / "products.csv",
    )
    parser.add_argument(
        "--silver",
        type=Path,
        default=PROJECT_ROOT / "data" / "processed" / "silver_events.jsonl",
    )
    return parser.parse_args()


def main():
    arguments = parse_arguments()
    transform_file(
        arguments.bronze,
        arguments.customers,
        arguments.products,
        arguments.silver,
    )
    print(f"Wrote Silver data to {arguments.silver}")


if __name__ == "__main__":
    main()
