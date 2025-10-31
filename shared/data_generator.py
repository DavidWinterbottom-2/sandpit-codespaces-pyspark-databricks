"""
Data generation utilities for PySpark examples
"""

import json
import random
from datetime import datetime, timedelta
from typing import Any, Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (BooleanType, DateType, DoubleType, IntegerType,
                               StringType, StructField, StructType,
                               TimestampType)

# Avoiding wildcard import to prevent overriding built-in functions like round()
# from pyspark.sql.functions import *


class DataGenerator:
    """Generate sample data for various PySpark examples"""

    @staticmethod
    def generate_customer_data(spark: SparkSession, num_records: int = 1000) -> DataFrame:
        """Generate sample customer data"""

        # Define schema
        schema = StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("signup_date", DateType(), True),
            StructField("is_premium", BooleanType(), True),
        ])

        # Sample data
        cities = ["New York", "Los Angeles", "Chicago",
                  "Houston", "Phoenix", "Philadelphia"]
        states = ["NY", "CA", "IL", "TX", "AZ", "PA"]

        # Generate data
        data = []
        for i in range(num_records):
            city_state = random.choice(list(zip(cities, states)))
            data.append((
                i + 1,
                f"Customer_{i+1}",
                f"customer{i+1}@email.com",
                random.randint(18, 80),
                city_state[0],
                city_state[1],
                (datetime.now() - timedelta(days=random.randint(0, 365))).date(),
                random.choice([True, False])
            ))

        return spark.createDataFrame(data, schema)

    @staticmethod
    def generate_transaction_data(spark: SparkSession, num_records: int = 5000) -> DataFrame:
        """Generate sample transaction data"""

        schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", IntegerType(), True),
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("payment_method", StringType(), True),
        ])

        # Sample data
        products = [
            ("PROD_001", "Laptop", "Electronics"),
            ("PROD_002", "Smartphone", "Electronics"),
            ("PROD_003", "Coffee Maker", "Appliances"),
            ("PROD_004", "Running Shoes", "Sports"),
            ("PROD_005", "Book", "Education"),
            ("PROD_006", "Headphones", "Electronics"),
        ]

        payment_methods = ["Credit Card", "Debit Card",
                           "PayPal", "Cash", "Digital Wallet"]

        # Generate data
        data = []
        for i in range(num_records):
            product = random.choice(products)
            quantity = random.randint(1, 5)
            base_price = random.uniform(10, 1000)

            data.append((
                f"TXN_{i+1:06d}",
                random.randint(1, 1000),  # customer_id
                product[0],
                product[1],
                product[2],
                round(base_price * quantity, 2),
                quantity,
                datetime.now() - timedelta(minutes=random.randint(0, 525600)),  # Last year
                random.choice(payment_methods)
            ))

        return spark.createDataFrame(data, schema)

    @staticmethod
    def generate_streaming_data(num_events: int = 100) -> List[str]:
        """Generate streaming data as JSON strings"""

        events = []
        event_types = ["page_view", "click", "purchase", "login", "logout"]

        for i in range(num_events):
            event = {
                "event_id": f"evt_{i+1:06d}",
                "user_id": f"user_{random.randint(1, 100)}",
                "event_type": random.choice(event_types),
                "page": f"/page/{random.randint(1, 20)}",
                "timestamp": (datetime.now() - timedelta(seconds=random.randint(0, 3600))).isoformat(),
                "session_id": f"session_{random.randint(1, 1000)}",
                "ip_address": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }

            if event["event_type"] == "purchase":
                event["amount"] = round(random.uniform(10, 500), 2)
                event["product_id"] = f"prod_{random.randint(1, 100)}"

            events.append(json.dumps(event))

        return events

    @staticmethod
    def generate_sensor_data(spark: SparkSession, num_records: int = 10000) -> DataFrame:
        """Generate IoT sensor data"""

        schema = StructType([
            StructField("sensor_id", StringType(), False),
            StructField("device_type", StringType(), True),
            StructField("location", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("pressure", DoubleType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("battery_level", IntegerType(), True),
            StructField("status", StringType(), True),
        ])

        # Sample data
        device_types = ["temperature_sensor",
                        "humidity_sensor", "pressure_sensor", "multi_sensor"]
        locations = ["Building_A_Floor_1", "Building_A_Floor_2",
                     "Building_B_Floor_1", "Warehouse", "Parking_Lot"]
        statuses = ["active", "maintenance", "error", "offline"]

        # Generate data
        data = []
        for i in range(num_records):
            data.append((
                f"SENSOR_{i+1:04d}",
                random.choice(device_types),
                random.choice(locations),
                round(random.uniform(-10, 40), 2),  # Temperature in Celsius
                round(random.uniform(30, 90), 2),   # Humidity percentage
                round(random.uniform(980, 1020), 2),  # Pressure in hPa
                datetime.now() - timedelta(minutes=random.randint(0, 10080)),  # Last week
                random.randint(10, 100),  # Battery percentage
                random.choice(statuses)
            ))

        return spark.createDataFrame(data, schema)


def save_sample_data_to_files(spark: SparkSession):
    """Generate and save sample data files"""

    # Generate datasets
    customers = DataGenerator.generate_customer_data(spark, 1000)
    transactions = DataGenerator.generate_transaction_data(spark, 5000)
    sensors = DataGenerator.generate_sensor_data(spark, 10000)

    # Save as CSV files
    customers.coalesce(1).write.mode("overwrite").option(
        "header", "true").csv("./data/input/customers")
    transactions.coalesce(1).write.mode("overwrite").option(
        "header", "true").csv("./data/input/transactions")
    sensors.coalesce(1).write.mode("overwrite").option(
        "header", "true").csv("./data/input/sensors")

    # Save as Parquet files
    customers.write.mode("overwrite").parquet("./data/input/customers_parquet")
    transactions.write.mode("overwrite").parquet(
        "./data/input/transactions_parquet")
    sensors.write.mode("overwrite").parquet("./data/input/sensors_parquet")

    # Generate streaming data files
    streaming_events = DataGenerator.generate_streaming_data(1000)

    # Save streaming data as text files
    with open("./data/input/streaming_events.jsonl", "w") as f:
        for event in streaming_events:
            f.write(event + "\n")

    print("Sample data files generated successfully!")
    print("Files created:")
    print("- ./data/input/customers/ (CSV)")
    print("- ./data/input/transactions/ (CSV)")
    print("- ./data/input/sensors/ (CSV)")
    print("- ./data/input/customers_parquet/ (Parquet)")
    print("- ./data/input/transactions_parquet/ (Parquet)")
    print("- ./data/input/sensors_parquet/ (Parquet)")
    print("- ./data/input/streaming_events.jsonl (JSON Lines)")


if __name__ == "__main__":
    from spark_utils import SparkSessionManager

    spark = SparkSessionManager.get_session("base", "DataGenerator")
    save_sample_data_to_files(spark)
    SparkSessionManager.stop_session()
