#!/usr/bin/env python3
"""
PySpark Delta Lake Examples

This module demonstrates Delta Lake capabilities including:
1. ACID transactions
2. Time travel (data versioning)
3. Schema evolution
4. Upserts (MERGE operations)
5. Data quality constraints
6. Concurrent reads and writes

Note: This version uses delayed imports to ensure proper path setup.
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path


def setup_imports():
    """Setup module imports after ensuring proper path configuration."""
    # Add shared modules to path BEFORE importing custom modules
    current_dir = os.path.dirname(os.path.abspath(__file__))
    shared_dir = os.path.join(current_dir, '..', '..', 'shared')
    sys.path.insert(0, shared_dir)

    # Import PySpark modules
    global SparkSession, StructType, StructField, StringType, DoubleType, IntegerType
    global TimestampType, DateType, BooleanType, DeltaTable
    global col, lit, when, current_timestamp, current_date, to_date, year, month, desc, asc
    global count, spark_sum, avg, spark_max, spark_min
    global SparkSessionManager, create_sample_data_dir, setup_logging, DataGenerator

    from data_generator import DataGenerator
    from delta.tables import DeltaTable
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (asc, avg, col, count, current_date,
                                       current_timestamp, desc, lit)
    from pyspark.sql.functions import max as spark_max
    from pyspark.sql.functions import min as spark_min
    from pyspark.sql.functions import month
    from pyspark.sql.functions import sum as spark_sum
    from pyspark.sql.functions import to_date, when, year
    from pyspark.sql.types import (BooleanType, DateType, DoubleType,
                                   IntegerType, StringType, StructField,
                                   StructType, TimestampType)
    # Import custom modules AFTER path setup
    from spark_utils import (SparkSessionManager, create_sample_data_dir,
                             setup_logging)

    return setup_logging()


class DeltaLakeBasicsExample:
    """Basic Delta Lake operations"""

    def __init__(self):
        self.spark = SparkSessionManager.get_session(
            "delta", "DeltaLakeBasics")
        create_sample_data_dir()
        self.delta_path = "./data/delta/customers"

    def create_delta_table(self):
        """Create a Delta table with initial data"""
        print("Creating Delta table with initial data...")

        # Generate sample customer data
        customers_df = DataGenerator.generate_customer_data(self.spark, 1000)

        # Write as Delta table
        customers_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(self.delta_path)

        print(f"Delta table created at: {self.delta_path}")
        return customers_df

    def read_delta_table(self):
        """Read from Delta table"""
        print("Reading from Delta table...")

        df = self.spark.read.format("delta").load(self.delta_path)
        print(f"Total records: {df.count()}")
        df.show(10)
        return df

    def append_data(self):
        """Append new data to Delta table"""
        print("Appending new data to Delta table...")

        # Generate new customer data
        new_customers = DataGenerator.generate_customer_data(self.spark, 200)

        # Modify customer IDs to avoid conflicts
        new_customers = new_customers.withColumn(
            "customer_id",
            col("customer_id") + 1000
        )

        # Append to Delta table
        new_customers.write \
            .format("delta") \
            .mode("append") \
            .save(self.delta_path)

        print("Data appended successfully!")

        # Verify the append
        df = self.spark.read.format("delta").load(self.delta_path)
        print(f"Total records after append: {df.count()}")

        return new_customers

    def update_data(self):
        """Update existing data in Delta table"""
        print("Updating data in Delta table...")

        # Load Delta table
        delta_table = DeltaTable.forPath(self.spark, self.delta_path)

        # Update premium status for customers from specific cities
        delta_table.update(
            condition=col("city").isin(["New York", "Los Angeles"]),
            set={
                "is_premium": lit(True),
                "signup_date": current_date()
            }
        )

        print("Data updated successfully!")

        # Show updated records
        updated_df = self.spark.read.format("delta").load(self.delta_path)
        updated_df.filter(col("city").isin(
            ["New York", "Los Angeles"])).show(10)

    def delete_data(self):
        """Delete data from Delta table"""
        print("Deleting data from Delta table...")

        # Load Delta table
        delta_table = DeltaTable.forPath(self.spark, self.delta_path)

        # Count before deletion
        before_count = self.spark.read.format(
            "delta").load(self.delta_path).count()
        print(f"Records before deletion: {before_count}")

        # Delete customers older than 65
        delta_table.delete(col("age") > 65)

        print("Data deleted successfully!")

        # Count after deletion
        after_count = self.spark.read.format(
            "delta").load(self.delta_path).count()
        print(f"Records after deletion: {after_count}")
        print(f"Deleted records: {before_count - after_count}")


class DeltaLakeTimeTravel:
    """Delta Lake time travel and versioning examples"""

    def __init__(self):
        self.spark = SparkSessionManager.get_session(
            "delta", "DeltaTimeTravel")
        self.delta_path = "./data/delta/customers"

    def show_table_history(self):
        """Show Delta table history"""
        print("Delta table history:")

        # Get table history
        delta_table = DeltaTable.forPath(self.spark, self.delta_path)
        history_df = delta_table.history()

        history_df.select(
            "version",
            "timestamp",
            "operation",
            "operationParameters",
            "readVersion",
            "isBlindAppend"
        ).show(truncate=False)

        return history_df

    def time_travel_by_version(self):
        """Read specific version of Delta table"""
        print("Time travel by version...")

        # Read version 0 (initial data)
        version_0 = self.spark.read \
            .format("delta") \
            .option("versionAsOf", 0) \
            .load(self.delta_path)

        print(f"Version 0 record count: {version_0.count()}")

        # Read latest version
        latest_version = self.spark.read \
            .format("delta") \
            .load(self.delta_path)

        print(f"Latest version record count: {latest_version.count()}")

        # Compare data between versions
        print("Comparing data between versions...")
        version_0_cities = version_0.select("city").distinct().collect()
        latest_cities = latest_version.select("city").distinct().collect()

        print(f"Cities in version 0: {[row.city for row in version_0_cities]}")
        print(f"Cities in latest: {[row.city for row in latest_cities]}")

    def time_travel_by_timestamp(self):
        """Read Delta table at specific timestamp"""
        print("Time travel by timestamp...")

        # Get a timestamp from history
        delta_table = DeltaTable.forPath(self.spark, self.delta_path)
        history_df = delta_table.history()

        # Get timestamp of first operation
        first_timestamp = history_df.select("timestamp").collect()[0].timestamp

        # Read data at that timestamp
        historical_data = self.spark.read \
            .format("delta") \
            .option("timestampAsOf", first_timestamp) \
            .load(self.delta_path)

        print(f"Data at {first_timestamp}: {historical_data.count()} records")
        historical_data.show(5)

    def restore_table(self):
        """Restore table to previous version"""
        print("Demonstrating table restore...")

        # Create a backup before restoration
        current_data = self.spark.read.format("delta").load(self.delta_path)
        current_count = current_data.count()
        print(f"Current record count: {current_count}")

        # Restore to version 1 (after first append)
        delta_table = DeltaTable.forPath(self.spark, self.delta_path)

        try:
            # This would restore the table to version 1
            # delta_table.restoreToVersion(1)
            print("Table restore simulation (actual restore commented out)")
            print("To restore: delta_table.restoreToVersion(1)")

            # Show what version 1 looked like
            version_1_data = self.spark.read \
                .format("delta") \
                .option("versionAsOf", 1) \
                .load(self.delta_path)

            print(f"Version 1 would have: {version_1_data.count()} records")

        except Exception as e:
            print(f"Restore simulation: {e}")


class DeltaLakeSchemaEvolution:
    """Schema evolution examples"""

    def __init__(self):
        self.spark = SparkSessionManager.get_session(
            "delta", "DeltaSchemaEvolution")
        self.delta_path = "./data/delta/products"
        create_sample_data_dir()

    def create_initial_schema(self):
        """Create Delta table with initial schema"""
        print("Creating Delta table with initial schema...")

        # Initial product schema
        initial_data = [
            (1, "Laptop", "Electronics", 999.99),
            (2, "Smartphone", "Electronics", 599.99),
            (3, "Coffee Maker", "Appliances", 149.99),
            (4, "Running Shoes", "Sports", 89.99),
            (5, "Book", "Education", 24.99)
        ]

        schema = StructType([
            StructField("product_id", IntegerType(), False),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True)
        ])

        df = self.spark.createDataFrame(initial_data, schema)

        df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(self.delta_path)

        print("Initial schema created:")
        df.printSchema()
        df.show()

    def add_new_columns(self):
        """Add new columns to existing Delta table"""
        print("Adding new columns through schema evolution...")

        # New data with additional columns
        evolved_data = [
            (6, "Tablet", "Electronics", 399.99, "2024-01-15", 5, True),
            (7, "Headphones", "Electronics", 199.99, "2024-01-16", 4, False),
            (8, "Desk Chair", "Furniture", 299.99, "2024-01-17", 4, True)
        ]

        evolved_schema = StructType([
            StructField("product_id", IntegerType(), False),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("launch_date", StringType(), True),  # New column
            StructField("rating", IntegerType(), True),      # New column
            StructField("in_stock", BooleanType(), True)     # New column
        ])

        evolved_df = self.spark.createDataFrame(evolved_data, evolved_schema)

        # Enable schema evolution and append
        evolved_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(self.delta_path)

        print("Schema evolved successfully!")

        # Read and show the evolved table
        result_df = self.spark.read.format("delta").load(self.delta_path)
        print("New schema:")
        result_df.printSchema()
        result_df.show()

    def handle_schema_changes(self):
        """Demonstrate handling of schema changes"""
        print("Handling schema changes and data types...")

        # Read current data
        current_df = self.spark.read.format("delta").load(self.delta_path)

        # Transform data with type changes
        transformed_df = current_df \
            .withColumn("launch_date",
                        when(col("launch_date").isNotNull(),
                             to_date(col("launch_date"), "yyyy-MM-dd"))
                        .otherwise(lit(None).cast(DateType()))) \
            .withColumn("price_category",
                        when(col("price") < 100, "Budget")
                        .when(col("price") < 500, "Mid-range")
                        .otherwise("Premium"))

        # Show transformed data
        print("Data with schema transformations:")
        transformed_df.printSchema()
        transformed_df.show()

        # Save transformed data as a new version
        transformed_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(self.delta_path + "_transformed")

        print("Transformed data saved to new location")


class DeltaLakeUpsertExample:
    """UPSERT (MERGE) operations example"""

    def __init__(self):
        self.spark = SparkSessionManager.get_session("delta", "DeltaUpsert")
        self.delta_path = "./data/delta/inventory"
        create_sample_data_dir()

    def create_inventory_table(self):
        """Create initial inventory Delta table"""
        print("Creating inventory Delta table...")

        inventory_data = [
            (1, "Laptop", 50, 999.99, "2024-01-01"),
            (2, "Smartphone", 100, 599.99, "2024-01-01"),
            (3, "Coffee Maker", 25, 149.99, "2024-01-01"),
            (4, "Running Shoes", 75, 89.99, "2024-01-01"),
            (5, "Book", 200, 24.99, "2024-01-01")
        ]

        schema = StructType([
            StructField("product_id", IntegerType(), False),
            StructField("product_name", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True),
            StructField("last_updated", StringType(), True)
        ])

        df = self.spark.createDataFrame(inventory_data, schema)

        df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(self.delta_path)

        print("Inventory table created:")
        df.show()

        return df

    def perform_upsert(self):
        """Perform UPSERT operation using MERGE"""
        print("Performing UPSERT operation...")

        # Create updates data (mix of updates and inserts)
        updates_data = [
            (2, "Smartphone Pro", 80, 699.99, "2024-01-15"),  # Update existing
            (4, "Running Shoes", 60, 94.99, "2024-01-15"),    # Update existing
            (6, "Tablet", 30, 399.99, "2024-01-15"),          # New product
            (7, "Wireless Mouse", 150, 49.99, "2024-01-15")   # New product
        ]

        schema = StructType([
            StructField("product_id", IntegerType(), False),
            StructField("product_name", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True),
            StructField("last_updated", StringType(), True)
        ])

        updates_df = self.spark.createDataFrame(updates_data, schema)

        print("Updates data:")
        updates_df.show()

        # Load Delta table
        delta_table = DeltaTable.forPath(self.spark, self.delta_path)

        # Perform MERGE operation
        delta_table.alias("inventory") \
            .merge(
                updates_df.alias("updates"),
                "inventory.product_id = updates.product_id"
        ) \
            .whenMatchedUpdate(set={
                "product_name": col("updates.product_name"),
                "quantity": col("updates.quantity"),
                "price": col("updates.price"),
                "last_updated": col("updates.last_updated")
            }) \
            .whenNotMatchedInsert(values={
                "product_id": col("updates.product_id"),
                "product_name": col("updates.product_name"),
                "quantity": col("updates.quantity"),
                "price": col("updates.price"),
                "last_updated": col("updates.last_updated")
            }) \
            .execute()

        print("UPSERT completed!")

        # Show final result
        result_df = self.spark.read.format("delta").load(self.delta_path)
        print("Final inventory:")
        result_df.orderBy("product_id").show()

    def conditional_upsert(self):
        """Perform conditional UPSERT with business logic"""
        print("Performing conditional UPSERT...")

        # Create conditional updates
        conditional_updates = [
            (1, "Laptop Pro", 45, 1099.99, "2024-01-20"),  # Higher price
            (3, "Coffee Maker", 30, 139.99, "2024-01-20")  # Lower price
        ]

        schema = StructType([
            StructField("product_id", IntegerType(), False),
            StructField("product_name", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True),
            StructField("last_updated", StringType(), True)
        ])

        updates_df = self.spark.createDataFrame(conditional_updates, schema)

        # Load Delta table
        delta_table = DeltaTable.forPath(self.spark, self.delta_path)

        # Conditional merge - only update if new price is higher
        delta_table.alias("inventory") \
            .merge(
                updates_df.alias("updates"),
                "inventory.product_id = updates.product_id"
        ) \
            .whenMatchedUpdate(
                condition=col("updates.price") > col("inventory.price"),
                set={
                    "product_name": col("updates.product_name"),
                    "quantity": col("updates.quantity"),
                    "price": col("updates.price"),
                    "last_updated": col("updates.last_updated")
                }
        ) \
            .execute()

        print("Conditional UPSERT completed!")

        # Show result
        result_df = self.spark.read.format("delta").load(self.delta_path)
        print("Updated inventory (only higher prices):")
        result_df.orderBy("product_id").show()


def main():
    """Run all Delta Lake examples"""
    logger = setup_imports()

    print("=== PySpark Delta Lake Examples ===\n")

    try:
        # 1. Basic Delta Lake Operations
        print("1. Delta Lake Basics")
        basics_example = DeltaLakeBasicsExample()
        basics_example.create_delta_table()
        basics_example.read_delta_table()
        basics_example.append_data()
        basics_example.update_data()
        basics_example.delete_data()

        print("\n" + "="*50 + "\n")

        # 2. Time Travel
        print("2. Delta Lake Time Travel")
        time_travel_example = DeltaLakeTimeTravel()
        time_travel_example.show_table_history()
        time_travel_example.time_travel_by_version()
        time_travel_example.time_travel_by_timestamp()
        time_travel_example.restore_table()

        print("\n" + "="*50 + "\n")

        # 3. Schema Evolution
        print("3. Schema Evolution")
        schema_example = DeltaLakeSchemaEvolution()
        schema_example.create_initial_schema()
        schema_example.add_new_columns()
        schema_example.handle_schema_changes()

        print("\n" + "="*50 + "\n")

        # 4. UPSERT Operations
        print("4. UPSERT Operations")
        upsert_example = DeltaLakeUpsertExample()
        upsert_example.create_inventory_table()
        upsert_example.perform_upsert()
        upsert_example.conditional_upsert()

    except Exception as e:
        logger.error(f"Error in Delta Lake examples: {e}")
        raise

    finally:
        SparkSessionManager.stop_session()

    print("\n=== All Delta Lake Examples Completed ===")


if __name__ == "__main__":
    main()
