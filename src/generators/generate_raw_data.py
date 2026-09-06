"""Generate small, reproducible raw e-commerce datasets for local learning."""

import argparse
import csv
import json
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


FIRST_NAMES = ["Amira", "Omar", "Sofia", "Daniel", "Aisha", "Liam", "Maya", "Yuki", "Chloe", "Ravi", "Fatima", "Lucas"]
LAST_NAMES = ["Al-Tamimi", "Khan", "Martinez", "Smith", "Chen", "Okafor", "Dubois", "Silva", "Andersson", "Patel", "Sato", "Haddad"]
COUNTRIES = [
    "Saudi Arabia",
    "United Arab Emirates",
    "United States",
    "United Kingdom",
    "Canada",
    "Germany",
    "India",
    "Japan",
    "Brazil",
    "Australia",
    "Nigeria",
    "South Africa",
]
PRODUCTS = [
    ("Wireless Keyboard", "Electronics", 279.00),
    ("USB-C Laptop Hub", "Electronics", 149.00),
    ("Data Engineering Handbook", "Books", 159.00),
    ("Insulated Water Bottle", "Home", 85.00),
    ("Noise Cancelling Headphones", "Electronics", 599.00),
    ("Running Shoes", "Sports", 349.00),
    ("Cotton Overshirt", "Clothing", 179.00),
    ("Smart LED Desk Lamp", "Home", 129.00),
]
EVENT_TYPES = ["view", "add_to_cart", "purchase"]


def generate_customers(count, generator):
    """Create dimension-style customer records with stable IDs."""
    customers = []
    for number in range(1, count + 1):
        name = f"{generator.choice(FIRST_NAMES)} {generator.choice(LAST_NAMES)}"
        customers.append(
            {
                "customer_id": f"C{number:03d}",
                "name": name,
                "country": generator.choice(COUNTRIES),
                "signup_date": date(2026, 1, 1) + timedelta(days=generator.randrange(90)),
            }
        )
    return customers


def generate_products(count):
    """Create product records; repeat the catalog only when a larger test is requested."""
    products = []
    for number in range(1, count + 1):
        name, category, price = PRODUCTS[(number - 1) % len(PRODUCTS)]
        products.append(
            {
                "product_id": f"P{number:03d}",
                "product_name": name if number <= len(PRODUCTS) else f"{name} {number}",
                "category": category,
                "price": price,
            }
        )
    return products


def generate_events(count, customers, products, generator):
    """Create fact-style activity while keeping every foreign key valid."""
    start = datetime(2026, 3, 1, tzinfo=timezone.utc)
    events = []
    for number in range(1, count + 1):
        event_time = start + timedelta(seconds=generator.randrange(14 * 24 * 60 * 60))
        events.append(
            {
                "event_id": f"E{number:04d}",
                "customer_id": generator.choice(customers)["customer_id"],
                "product_id": generator.choice(products)["product_id"],
                "event_type": generator.choice(EVENT_TYPES),
                "timestamp": event_time.isoformat().replace("+00:00", "Z"),
                "quantity": generator.randint(1, 3),
            }
        )
    return events


def write_csv(path, rows, fieldnames):
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def generate_data(output_directory, customer_count, product_count, event_count, seed):
    """Generate all three related datasets using one seed for reproducibility."""
    output_directory.mkdir(parents=True, exist_ok=True)
    generator = random.Random(seed)
    customers = generate_customers(customer_count, generator)
    products = generate_products(product_count)
    events = generate_events(event_count, customers, products, generator)

    # Separate files mirror common source systems with different export formats.
    write_csv(output_directory / "customers.csv", customers, ["customer_id", "name", "country", "signup_date"])
    write_csv(output_directory / "products.csv", products, ["product_id", "product_name", "category", "price"])
    with (output_directory / "events.json").open("w", encoding="utf-8") as file:
        json.dump(events, file, indent=2)
        file.write("\n")


def parse_arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/generated"))
    parser.add_argument("--customers", type=int, default=10)
    parser.add_argument("--products", type=int, default=10)
    parser.add_argument("--events", type=int, default=50)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main():
    arguments = parse_arguments()
    generate_data(
        arguments.output,
        arguments.customers,
        arguments.products,
        arguments.events,
        arguments.seed,
    )
    print(f"Generated data in {arguments.output}")


if __name__ == "__main__":
    main()
2