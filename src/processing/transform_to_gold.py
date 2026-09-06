"""Create business-level summaries from Silver e-commerce events."""

import argparse
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from utils.logging_config import get_logger
except ModuleNotFoundError:
    from src.utils.logging_config import get_logger


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOGGER = get_logger(__name__)


def load_events(path):
    """Read one JSON object per line from the Silver data layer."""
    with path.open(encoding="utf-8") as file:
        return [json.loads(line) for line in file]


def aggregate(events, dimension):
    """Summarize completed purchases by one business dimension."""
    totals = defaultdict(lambda: {"purchase_count": 0, "units_sold": 0, "revenue": 0.0})
    for event in events:
        if event["event_type"] != "purchase":
            continue
        key = event[dimension]
        totals[key]["purchase_count"] += 1
        totals[key]["units_sold"] += event["quantity"]
        totals[key]["revenue"] += event["total_amount"]

    return [
        {
            "dimension": key,
            "purchase_count": values["purchase_count"],
            "units_sold": values["units_sold"],
            "revenue": round(values["revenue"], 2),
        }
        for key, values in sorted(totals.items())
    ]


def write_summary(path, summary):
    """Write a compact JSON summary for dashboards or downstream queries."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2)
        file.write("\n")


def create_gold_outputs(silver_path, output_directory):
    """Create one Gold summary for each useful business dimension."""
    started_at = time.perf_counter()
    events = load_events(silver_path)
    dimensions = {
        "revenue_by_country": "customer_country",
        "revenue_by_category": "category",
        "revenue_by_product": "product_name",
    }
    for filename, dimension in dimensions.items():
        write_summary(output_directory / f"{filename}.json", aggregate(events, dimension))
    LOGGER.info(
        "Gold summaries completed",
        extra={
            "event": "gold_transform",
            "count": len(events),
            "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
            "status": "success",
        },
    )


def parse_arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--silver",
        type=Path,
        default=PROJECT_ROOT / "data" / "processed" / "silver_events.jsonl",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=PROJECT_ROOT / "data" / "processed" / "gold",
    )
    return parser.parse_args()


def main():
    arguments = parse_arguments()
    create_gold_outputs(arguments.silver, arguments.output)
    print(f"Wrote Gold summaries to {arguments.output}")


if __name__ == "__main__":
    main()
