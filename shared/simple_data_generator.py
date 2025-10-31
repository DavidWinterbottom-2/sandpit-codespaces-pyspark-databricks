#!/usr/bin/env python3
"""
Simple data generator for PySpark examples that avoids py4j issues.
Creates small JSON files instead of CSV to reduce complexity.
"""

import json
import os
import random
from datetime import datetime, timedelta


def create_directories():
    """Create necessary directories."""
    directories = [
        "./data/input",
        "./data/output",
        "./data/delta",
        "./data/warehouse"
    ]

    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"✅ Created directory: {directory}")


def generate_sample_data():
    """Generate sample data as Python dictionaries."""
    # Generate user data
    users = []
    for i in range(100):
        users.append({
            "user_id": i + 1,
            "name": f"User{i+1}",
            "email": f"user{i+1}@example.com",
            "age": random.randint(18, 80),
            "city": random.choice(["New York", "London", "Tokyo", "Sydney", "Toronto"])
        })

    # Generate product data
    products = []
    product_names = ["Laptop", "Phone", "Tablet", "Watch",
                     "Headphones", "Speaker", "Camera", "Monitor"]
    for i in range(50):
        products.append({
            "product_id": i + 1,
            "name": random.choice(product_names),
            "category": random.choice(["Electronics", "Accessories", "Computers"]),
            "price": round(random.uniform(50, 2000), 2),
            "rating": round(random.uniform(3.0, 5.0), 1)
        })

    # Generate sales data
    sales = []
    start_date = datetime.now() - timedelta(days=30)
    for i in range(500):
        sale_date = start_date + timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        sales.append({
            "sale_id": i + 1,
            "user_id": random.randint(1, 100),
            "product_id": random.randint(1, 50),
            "quantity": random.randint(1, 5),
            "sale_date": sale_date.isoformat(),
            "total_amount": round(random.uniform(50, 1000), 2)
        })

    return users, products, sales


def save_data_to_json(data, filename):
    """Save data to JSON Lines format (one JSON object per line) for PySpark."""
    with open(filename, 'w', encoding='utf-8') as f:
        for record in data:
            json.dump(record, f)
            f.write('\n')
    print(f"✅ Saved {len(data)} records to {filename}")


def main():
    """Generate and save sample data."""
    print("🚀 Starting simple data generation...")

    # Create directories
    create_directories()

    # Generate data
    print("📊 Generating sample data...")
    users, products, sales = generate_sample_data()

    # Save to JSON files
    save_data_to_json(users, "./data/input/users.json")
    save_data_to_json(products, "./data/input/products.json")
    save_data_to_json(sales, "./data/input/sales.json")

    print("✅ Data generation completed successfully!")
    print("\nGenerated files:")
    print("  - ./data/input/users.json (100 users)")
    print("  - ./data/input/products.json (50 products)")
    print("  - ./data/input/sales.json (500 sales)")

    print("\nYou can now use these files with PySpark:")
    print("  spark.read.json('./data/input/users.json')")


if __name__ == "__main__":
    main()
