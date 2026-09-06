import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from src.ingestion.consumer import load_event_ids, normalize_event, validate_event
from src.processing.transform_to_gold import aggregate
from src.processing.transform_to_silver import transform_event


class TransformationTests(unittest.TestCase):
    def setUp(self):
        self.customers = {
            "C001": {"customer_id": "C001", "country": "Saudi Arabia"}
        }
        self.products = {
            "P001": {
                "product_id": "P001",
                "product_name": "Wireless Keyboard",
                "category": "Electronics",
                "price": "279.00",
            }
        }

    def test_silver_enriches_purchase_and_calculates_revenue(self):
        event = {
            "event_id": "E001",
            "customer_id": "C001",
            "product_id": "P001",
            "event_type": "purchase",
            "timestamp": "2026-03-10T09:17:32Z",
            "quantity": 2,
        }

        result = transform_event(event, self.customers, self.products)

        self.assertEqual(result["customer_country"], "Saudi Arabia")
        self.assertEqual(result["product_name"], "Wireless Keyboard")
        self.assertEqual(result["quantity"], 2)
        self.assertEqual(result["total_amount"], 558.0)

    def test_gold_aggregates_purchases_only(self):
        events = [
            {"event_type": "view", "category": "Electronics", "quantity": 1, "total_amount": 0.0},
            {"event_type": "purchase", "category": "Electronics", "quantity": 2, "total_amount": 558.0},
        ]

        result = aggregate(events, "category")

        self.assertEqual(result, [{
            "dimension": "Electronics",
            "purchase_count": 1,
            "units_sold": 2,
            "revenue": 558.0,
        }])

    def test_existing_bronze_ids_can_be_used_for_deduplication(self):
        with TemporaryDirectory() as directory:
            bronze_path = Path(directory) / "bronze.jsonl"
            bronze_path.write_text(
                '{"event_id": "E001", "quantity": 1}\n',
                encoding="utf-8",
            )

            self.assertEqual(load_event_ids(bronze_path), {"E001"})

    def test_old_event_schema_gets_default_currency(self):
        old_event = {
            "event_id": "E002",
            "customer_id": "C001",
            "product_id": "P001",
            "event_type": "view",
            "timestamp": "2026-03-10T09:17:32Z",
            "quantity": 1,
        }

        self.assertTrue(validate_event(old_event))
        self.assertEqual(normalize_event(old_event)["currency"], "USD")

    def test_new_event_schema_accepts_sar_currency(self):
        new_event = {
            "event_id": "E003",
            "customer_id": "C001",
            "product_id": "P001",
            "event_type": "purchase",
            "timestamp": "2026-03-10T09:17:32Z",
            "quantity": 1,
            "currency": "SAR",
        }

        self.assertTrue(validate_event(new_event))
        self.assertEqual(normalize_event(new_event)["currency"], "SAR")


if __name__ == "__main__":
    unittest.main()
