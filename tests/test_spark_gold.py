import unittest
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession

from src.processing.spark_gold import create_category_summary


class SparkGoldTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("spark-gold-tests")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_category_summary_counts_purchases(self):
        # Keep the Spark test self-contained: generated data is intentionally
        # ignored by Git and is not available on a clean CI runner.
        with TemporaryDirectory() as directory:
            silver_path = f"{directory}/silver_events.jsonl"
            with open(silver_path, "w", encoding="utf-8") as handle:
                handle.write(
                    '{"event_id":"E001","event_type":"purchase",'
                    '"category":"Electronics","quantity":2,'
                    '"total_amount":298.0}\n'
                )
                handle.write(
                    '{"event_id":"E002","event_type":"view",'
                    '"category":"Electronics","quantity":1,'
                    '"total_amount":0.0}\n'
                )
            summary = create_category_summary(self.spark, silver_path)
            electronics = summary.where("category = 'Electronics'").first()

        self.assertEqual(electronics.purchase_count, 1)
        self.assertEqual(electronics.units_sold, 2)
        self.assertEqual(electronics.revenue, 298.0)


if __name__ == "__main__":
    unittest.main()
