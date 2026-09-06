"""Airflow orchestration for the local e-commerce batch pipeline."""

from datetime import datetime, timedelta
import os
from pathlib import Path
import subprocess
import sys

from airflow.decorators import dag, task

from src.processing.transform_to_gold import create_gold_outputs
from src.processing.transform_to_silver import transform_file


PROJECT_ROOT = Path(
    os.environ.get("ECOMMERCE_PROJECT_ROOT", Path(__file__).resolve().parents[1])
)


@dag(
    dag_id="ecommerce_batch_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ecommerce", "learning"],
)
def ecommerce_batch_pipeline():
    """Define dependencies while keeping transformations in application code."""

    @task
    def validate_source_data():
        """Run the repository's existing source-data integrity tests."""
        subprocess.run(
            [sys.executable, "-m", "unittest", "tests.test_data_model"],
            cwd=PROJECT_ROOT,
            check=True,
        )

    @task
    def run_silver_transformation():
        """Build Silver data from the Bronze input."""
        transform_file(
            PROJECT_ROOT / "data" / "processed" / "bronze_demo.jsonl",
            PROJECT_ROOT / "data" / "raw" / "customers.csv",
            PROJECT_ROOT / "data" / "raw" / "products.csv",
            PROJECT_ROOT / "data" / "processed" / "silver_events.jsonl",
        )

    @task
    def run_gold_transformation():
        """Build Gold summaries from the Silver output."""
        create_gold_outputs(
            PROJECT_ROOT / "data" / "processed" / "silver_events.jsonl",
            PROJECT_ROOT / "data" / "processed" / "gold",
        )

    source_valid = validate_source_data()
    silver_ready = run_silver_transformation()
    gold_ready = run_gold_transformation()

    source_valid >> silver_ready >> gold_ready


ecommerce_batch_pipeline()
