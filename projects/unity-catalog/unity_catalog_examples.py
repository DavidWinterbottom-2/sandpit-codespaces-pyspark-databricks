"""
PySpark Unity Catalog Examples

This module demonstrates Unity Catalog capabilities including:
1. Catalog and schema management
2. Table governance and access control
3. Data lineage tracking
4. Data discovery and classification
5. Fine-grained permissions
6. Integration with Delta Lake

Note: Some features require a Databricks environment with Unity Catalog enabled.
This module provides examples that can run locally with simulated Unity Catalog functionality.
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path


def setup_imports():
    """Setup import paths for shared modules"""
    # Get the current script's directory
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Add shared directory to Python path
    shared_dir = os.path.join(current_dir, '..', '..', 'shared')
    shared_dir = os.path.abspath(shared_dir)

    if shared_dir not in sys.path:
        sys.path.insert(0, shared_dir)


# Call setup_imports at module level
setup_imports()

# Now import all modules at module level
try:
    from data_generator import DataGenerator
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from spark_utils import (SparkSessionManager, create_sample_data_dir,
                             setup_logging)
except ImportError as e:
    print(f"Error importing modules: {e}")
    print(f"Current Python path: {sys.path}")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    shared_dir = os.path.join(current_dir, '..', '..', 'shared')
    shared_dir = os.path.abspath(shared_dir)
    print(f"Shared directory: {shared_dir}")
    print(f"Shared directory exists: {os.path.exists(shared_dir)}")
    raise

logger = setup_logging()


class UnityCatalogSetup:
    """Unity Catalog setup and configuration"""

    def __init__(self, spark_session=None):
        # Use provided session or create new one
        # Use delta configuration for local Unity Catalog simulation
        self.spark = spark_session if spark_session else SparkSessionManager.get_session(
            "delta", "UnityCatalog")
        create_sample_data_dir()
        self.catalog_name = "pyspark_examples"
        self.schema_name = "sample_data"
        self.warehouse_path = "./data/warehouse"

        # Create warehouse directory
        Path(self.warehouse_path).mkdir(parents=True, exist_ok=True)

    def setup_local_catalog(self):
        """Setup local catalog structure (simulating Unity Catalog)"""
        print("Setting up local catalog structure...")

        try:
            # Create catalog (in local mode, this is simulated)
            print(f"Creating catalog: {self.catalog_name}")

            # Create schema
            self.spark.sql(
                f"CREATE SCHEMA IF NOT EXISTS {self.catalog_name}.{self.schema_name}")
            print(f"Created schema: {self.catalog_name}.{self.schema_name}")

            # Show available catalogs and schemas
            print("Available schemas:")
            self.spark.sql("SHOW SCHEMAS").show()

        except Exception as e:
            print(
                f"Note: Running in local mode - Unity Catalog features simulated: {e}")

            # Create database instead of schema for local mode
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.schema_name}")
            print(f"Created database: {self.schema_name}")

    def create_managed_tables(self):
        """Create managed tables with metadata"""
        print("Creating managed tables...")

        # Customer table
        customers_df = DataGenerator.generate_customer_data(self.spark, 1000)

        table_name = f"{self.schema_name}.customers"

        # Write as managed table
        customers_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(table_name)

        print(f"Created managed table: {table_name}")

        # Add table properties (metadata)
        try:
            self.spark.sql(f"""
                ALTER TABLE {table_name} 
                SET TBLPROPERTIES (
                    'description' = 'Customer master data',
                    'data_classification' = 'PII',
                    'data_owner' = 'data_team',
                    'created_by' = 'pyspark_example',
                    'created_date' = '{datetime.now().isoformat()}'
                )
            """)
            print("Added table properties")
        except Exception as e:
            print(f"Table properties note: {e}")

        return table_name

    def create_external_tables(self):
        """Create external tables pointing to external data"""
        print("Creating external tables...")

        # Generate transaction data and save to external location
        transactions_df = DataGenerator.generate_transaction_data(
            self.spark, 5000)
        external_path = f"{self.warehouse_path}/external/transactions"

        # Save to external location
        transactions_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(external_path)

        # Create external table
        table_name = f"{self.schema_name}.transactions_external"

        try:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_name}
                USING DELTA
                LOCATION '{external_path}'
            """)
            print(f"Created external table: {table_name}")
        except Exception as e:
            print(f"External table creation note: {e}")
            # Alternative for local mode - create as managed table instead
            print("Creating as managed table instead...")
            transactions_df.write \
                .format("delta") \
                .mode("overwrite") \
                .saveAsTable(table_name)
            print(f"Created managed table: {table_name}")

        return table_name


class DataGovernanceExample:
    """Data governance and access control examples"""

    def __init__(self, spark_session=None):
        # Use provided session or create new one
        self.spark = spark_session if spark_session else SparkSessionManager.get_session(
            "delta", "DataGovernance")
        self.schema_name = "sample_data"

    def demonstrate_column_masking(self):
        """Demonstrate column-level security (simulated)"""
        print("Demonstrating column-level security...")

        # Read customer data
        customers_df = self.spark.table(f"{self.schema_name}.customers")

        # Simulate column masking for PII data
        masked_df = customers_df.select(
            col("customer_id"),
            col("name"),
            # Mask email domain
            regexp_replace(col("email"), "@.*",
                           "@*****.com").alias("email_masked"),
            col("age"),
            col("city"),
            col("state"),
            col("signup_date"),
            col("is_premium")
        )

        print("Original data (first 5 rows):")
        customers_df.select("customer_id", "name", "email").show(5)

        print("Masked data (simulated column-level security):")
        masked_df.select("customer_id", "name", "email_masked").show(5)

        return masked_df

    def demonstrate_row_level_security(self):
        """Demonstrate row-level security (simulated)"""
        print("Demonstrating row-level security...")

        # Simulate different user access levels
        user_roles = {
            "analyst": ["NY", "CA"],  # Can only see NY and CA data
            "manager": ["NY", "CA", "TX", "IL"],  # Can see more states
            "admin": "all"  # Can see all data
        }

        customers_df = self.spark.table(f"{self.schema_name}.customers")

        for role, allowed_states in user_roles.items():
            print(f"\nData access for role '{role}':")

            if allowed_states == "all":
                filtered_df = customers_df
            else:
                filtered_df = customers_df.filter(
                    col("state").isin(allowed_states))

            print(f"Accessible records: {filtered_df.count()}")
            filtered_df.groupBy("state").count().show()

    def create_data_classification_tags(self):
        """Create and apply data classification tags"""
        print("Creating data classification tags...")

        # Simulate data classification
        classification_rules = {
            "customer_id": "identifier",
            "name": "pii",
            "email": "pii",
            "age": "demographic",
            "city": "location",
            "state": "location",
            "signup_date": "temporal",
            "is_premium": "business"
        }

        customers_df = self.spark.table(f"{self.schema_name}.customers")

        # Create metadata about data classification
        classification_metadata = []
        for column in customers_df.columns:
            classification = classification_rules.get(column, "unclassified")
            classification_metadata.append({
                "column_name": column,
                "data_type": str(customers_df.schema[column].dataType),
                "classification": classification,
                "is_sensitive": classification in ["pii", "identifier"]
            })

        # Save classification metadata
        metadata_path = "./data/metadata/data_classification.json"
        os.makedirs(os.path.dirname(metadata_path), exist_ok=True)

        with open(metadata_path, 'w') as f:
            json.dump(classification_metadata, f, indent=2)

        print(f"Data classification metadata saved to: {metadata_path}")

        # Display classification summary
        print("Data Classification Summary:")
        for item in classification_metadata:
            print(
                f"  {item['column_name']}: {item['classification']} {'(SENSITIVE)' if item['is_sensitive'] else ''}")


class DataLineageExample:
    """Data lineage tracking examples"""

    def __init__(self, spark_session=None):
        # Use provided session or create new one
        self.spark = spark_session if spark_session else SparkSessionManager.get_session(
            "delta", "DataLineage")
        self.schema_name = "sample_data"
        self.lineage_path = "./data/lineage"
        Path(self.lineage_path).mkdir(parents=True, exist_ok=True)

    def create_data_pipeline_with_lineage(self):
        """Create a data pipeline and track lineage"""
        print("Creating data pipeline with lineage tracking...")

        # Step 1: Source data
        customers_df = self.spark.table(f"{self.schema_name}.customers")
        transactions_df = self.spark.table(
            f"{self.schema_name}.transactions_external")

        # Track lineage metadata
        lineage_info = {
            "pipeline_id": "customer_analytics_001",
            "created_at": datetime.now().isoformat(),
            "steps": []
        }

        # Step 2: Data transformation
        print("Step 1: Customer segmentation...")
        customer_segments = customers_df.withColumn(
            "segment",
            when(col("age") < 30, "Young")
            .when(col("age") < 50, "Middle")
            .otherwise("Senior")
        ).withColumn(
            "premium_segment",
            when(col("is_premium"), "Premium")
            .otherwise("Standard")
        )

        # Record lineage step
        lineage_info["steps"].append({
            "step": 1,
            "operation": "customer_segmentation",
            "input_tables": [f"{self.schema_name}.customers"],
            "output": "customer_segments",
            "transformations": ["age_segment", "premium_segment"],
            "timestamp": datetime.now().isoformat()
        })

        # Step 3: Aggregation
        print("Step 2: Transaction aggregation...")
        transaction_summary = transactions_df.groupBy("customer_id").agg(
            count("*").alias("total_transactions"),
            sum("amount").alias("total_spent"),
            avg("amount").alias("avg_transaction_amount"),
            max("timestamp").alias("last_transaction_date")
        )

        lineage_info["steps"].append({
            "step": 2,
            "operation": "transaction_aggregation",
            "input_tables": [f"{self.schema_name}.transactions_external"],
            "output": "transaction_summary",
            "transformations": ["count", "sum", "avg", "max"],
            "timestamp": datetime.now().isoformat()
        })

        # Step 4: Join and create final dataset
        print("Step 3: Creating customer analytics...")
        customer_analytics = customer_segments.join(
            transaction_summary,
            "customer_id",
            "left"
        ).fillna(0, ["total_transactions", "total_spent", "avg_transaction_amount"])

        lineage_info["steps"].append({
            "step": 3,
            "operation": "customer_analytics_join",
            "input_tables": ["customer_segments", "transaction_summary"],
            "output": "customer_analytics",
            "transformations": ["left_join", "fillna"],
            "timestamp": datetime.now().isoformat()
        })

        # Save final dataset
        output_table = f"{self.schema_name}.customer_analytics"
        customer_analytics.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(output_table)

        # Save lineage information
        lineage_file = f"{self.lineage_path}/customer_analytics_lineage.json"
        with open(lineage_file, 'w') as f:
            json.dump(lineage_info, f, indent=2)

        print(f"Data lineage saved to: {lineage_file}")
        print(f"Output table created: {output_table}")

        # Show sample results
        print("Sample customer analytics:")
        customer_analytics.show(10)

        return customer_analytics, lineage_info

    def visualize_lineage(self, lineage_info):
        """Create a simple lineage visualization"""
        print("Data Lineage Visualization:")
        print("=" * 50)

        for step in lineage_info["steps"]:
            print(f"Step {step['step']}: {step['operation']}")
            print(f"  Inputs: {', '.join(step['input_tables'])}")
            print(f"  Output: {step['output']}")
            print(f"  Transformations: {', '.join(step['transformations'])}")
            print(f"  Timestamp: {step['timestamp']}")
            print()


class DataDiscoveryExample:
    """Data discovery and cataloging examples"""

    def __init__(self, spark_session=None):
        # Use provided session or create new one
        self.spark = spark_session if spark_session else SparkSessionManager.get_session(
            "delta", "DataDiscovery")
        self.schema_name = "sample_data"

    def discover_table_metadata(self):
        """Discover and catalog table metadata"""
        print("Discovering table metadata...")

        # Get list of tables
        tables = self.spark.sql(f"SHOW TABLES IN {self.schema_name}").collect()

        metadata_catalog = {
            "catalog_created": datetime.now().isoformat(),
            "tables": []
        }

        for table_row in tables:
            table_name = table_row.tableName
            full_table_name = f"{self.schema_name}.{table_name}"

            print(f"\nAnalyzing table: {full_table_name}")

            # Get table details
            try:
                df = self.spark.table(full_table_name)

                # Basic statistics
                row_count = df.count()
                column_count = len(df.columns)

                # Column analysis
                columns_info = []
                for col_name in df.columns:
                    try:
                        # Get column type safely
                        field = next(
                            field for field in df.schema.fields if field.name == col_name)
                        col_type = str(field.dataType)

                        # Get column statistics
                        non_null_count = df.filter(
                            df[col_name].isNotNull()).count()
                        null_percentage = (
                            (row_count - non_null_count) / row_count) * 100 if row_count > 0 else 0.0

                        columns_info.append({
                            "name": col_name,
                            "type": col_type,
                            "null_percentage": round(null_percentage, 2),
                            "sample_values": [str(row[col_name]) for row in df.select(col_name).limit(3).collect()]
                        })
                    except Exception as e:
                        print(f"    Error analyzing column {col_name}: {e}")
                        columns_info.append({
                            "name": col_name,
                            "type": "unknown",
                            "null_percentage": 0.0,
                            "sample_values": ["error"]
                        })

                # Create table metadata
                table_metadata = {
                    "table_name": full_table_name,
                    "row_count": row_count,
                    "column_count": column_count,
                    "columns": columns_info,
                    "discovered_at": datetime.now().isoformat()
                }

                metadata_catalog["tables"].append(table_metadata)

                print(f"  Rows: {row_count:,}")
                print(f"  Columns: {column_count}")
                print(
                    f"  Column details: {[c['name'] + ' (' + c['type'] + ')' for c in columns_info]}")

            except Exception as e:
                print(f"  Error analyzing table: {e}")

        # Save metadata catalog
        catalog_path = "./data/metadata/table_catalog.json"
        os.makedirs(os.path.dirname(catalog_path), exist_ok=True)

        with open(catalog_path, 'w') as f:
            json.dump(metadata_catalog, f, indent=2)

        print(f"\nTable catalog saved to: {catalog_path}")
        return metadata_catalog

    def create_data_quality_report(self):
        """Create a data quality assessment report"""
        print("Creating data quality report...")

        tables_to_check = [
            f"{self.schema_name}.customers",
            f"{self.schema_name}.customer_analytics"
        ]

        quality_report = {
            "report_generated": datetime.now().isoformat(),
            "tables": []
        }

        for table_name in tables_to_check:
            try:
                df = self.spark.table(table_name)

                # Data quality checks
                total_rows = df.count()
                duplicate_rows = df.count() - df.dropDuplicates().count()

                # Column-level quality checks
                column_quality = []
                for col_name in df.columns:
                    try:
                        null_count = df.filter(df[col_name].isNull()).count()
                        null_percentage = (null_count / total_rows) * \
                            100 if total_rows > 0 else 0.0

                        # Additional checks based on column type
                        quality_issues = []
                        if null_percentage > 20:
                            quality_issues.append("high_null_percentage")

                        # For string columns, check for empty strings
                        field = next(
                            field for field in df.schema.fields if field.name == col_name)
                        if "string" in str(field.dataType).lower():
                            empty_count = df.filter(
                                (df[col_name] == "") | (df[col_name] == " ")).count()
                            if empty_count > 0:
                                quality_issues.append("empty_strings")
                    except Exception as col_error:
                        print(
                            f"    Error checking column {col_name}: {col_error}")
                        quality_issues = ["analysis_error"]
                        null_percentage = 0.0

                    column_quality.append({
                        "column": col_name,
                        "null_percentage": round(null_percentage, 2),
                        "quality_issues": quality_issues
                    })

                table_quality = {
                    "table_name": table_name,
                    "total_rows": total_rows,
                    "duplicate_rows": duplicate_rows,
                    "data_quality_score": max(0, 100 - (duplicate_rows / total_rows * 100 if total_rows > 0 else 0)),
                    "column_quality": column_quality
                }

                quality_report["tables"].append(table_quality)

                print(f"\nTable: {table_name}")
                print(f"  Total rows: {total_rows:,}")
                print(f"  Duplicate rows: {duplicate_rows:,}")
                print(
                    f"  Quality score: {table_quality['data_quality_score']:.1f}%")

            except Exception as e:
                print(f"Error checking table {table_name}: {e}")

        # Save quality report
        report_path = "./data/metadata/data_quality_report.json"
        with open(report_path, 'w') as f:
            json.dump(quality_report, f, indent=2)

        print(f"\nData quality report saved to: {report_path}")
        return quality_report


def main():
    """Run all Unity Catalog examples"""
    print("=== PySpark Unity Catalog Examples ===\n")

    # Get shared Spark session
    spark = SparkSessionManager.get_session("delta", "UnityCatalogExamples")

    try:
        # 1. Setup Unity Catalog
        print("1. Unity Catalog Setup")
        setup_example = UnityCatalogSetup(spark_session=spark)
        setup_example.setup_local_catalog()
        customer_table = setup_example.create_managed_tables()
        transaction_table = setup_example.create_external_tables()

        print("\n" + "="*50 + "\n")

        # 2. Data Governance
        print("2. Data Governance Examples")
        governance_example = DataGovernanceExample(spark_session=spark)
        governance_example.demonstrate_column_masking()
        governance_example.demonstrate_row_level_security()
        governance_example.create_data_classification_tags()

        print("\n" + "="*50 + "\n")

        # 3. Data Lineage
        print("3. Data Lineage Tracking")
        lineage_example = DataLineageExample(spark_session=spark)
        analytics_df, lineage_info = lineage_example.create_data_pipeline_with_lineage()
        lineage_example.visualize_lineage(lineage_info)

        print("\n" + "="*50 + "\n")

        # 4. Data Discovery
        print("4. Data Discovery and Cataloging")
        discovery_example = DataDiscoveryExample(spark_session=spark)
        table_catalog = discovery_example.discover_table_metadata()
        quality_report = discovery_example.create_data_quality_report()

    except Exception as e:
        logger.error(f"Error in Unity Catalog examples: {e}")
        raise

    finally:
        SparkSessionManager.stop_session()

    print("\n=== All Unity Catalog Examples Completed ===")
    print("\nGenerated files:")
    print("- ./data/metadata/data_classification.json")
    print("- ./data/metadata/table_catalog.json")
    print("- ./data/metadata/data_quality_report.json")
    print("- ./data/lineage/customer_analytics_lineage.json")


if __name__ == "__main__":
    main()
