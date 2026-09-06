import csv
import json
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA = PROJECT_ROOT / "data" / "raw"


class DataModelIntegrityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Raw files are the source layer: load them exactly as a pipeline would.
        with (RAW_DATA / "customers.csv").open(newline="", encoding="utf-8") as file:
            cls.customers = list(csv.DictReader(file))
        with (RAW_DATA / "products.csv").open(newline="", encoding="utf-8") as file:
            cls.products = list(csv.DictReader(file))
        with (RAW_DATA / "events.json").open(encoding="utf-8") as file:
            cls.events = json.load(file)

    def test_primary_keys_are_unique(self):
        # A primary key identifies one row. Duplicates would make updates and joins ambiguous.
        for rows, key in (
            (self.customers, "customer_id"),
            (self.products, "product_id"),
            (self.events, "event_id"),
        ):
            values = [row[key] for row in rows]
            self.assertEqual(len(values), len(set(values)))
            self.assertTrue(all(values))

    def test_event_foreign_keys_reference_parent_entities(self):
        # Foreign-key checks protect referential integrity between events and their parent tables.
        customer_ids = {row["customer_id"] for row in self.customers}
        product_ids = {row["product_id"] for row in self.products}

        self.assertTrue(all(event["customer_id"] in customer_ids for event in self.events))
        self.assertTrue(all(event["product_id"] in product_ids for event in self.events))

    def test_required_values_and_domains_are_valid(self):
        # Domain checks catch bad records before they reach downstream transformations.
        event_types = {"view", "add_to_cart", "purchase"}

        for customer in self.customers:
            self.assertTrue(all(customer[field] for field in customer))
            date.fromisoformat(customer["signup_date"])

        for product in self.products:
            self.assertTrue(all(product[field] for field in product))
            self.assertGreater(Decimal(product["price"]), 0)

        for event in self.events:
            self.assertTrue(all(event[field] not in (None, "") for field in event))
            self.assertIn(event["event_type"], event_types)
            self.assertGreater(int(event["quantity"]), 0)
            datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))


if __name__ == "__main__":
    unittest.main()
