import unittest
from pathlib import Path

from pyspark.sql import SparkSession

from src.processing.spark_gold import create_category_summary


PROJECT_ROOT = Path(__file__).resolve().parents[1]


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
        summary = create_category_summary(
            self.spark,
            PROJECT_ROOT / "data" / "processed" / "silver_events.jsonl",
        )
        electronics = summary.where("category = 'Electronics'").first()

        self.assertEqual(electronics.purchase_count, 1)
        self.assertEqual(electronics.units_sold, 2)
        self.assertEqual(electronics.revenue, 298.0)


if __name__ == "__main__":
    unittest.main()
