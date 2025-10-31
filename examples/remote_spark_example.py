#!/usr/bin/env python3
"""
Example script showing how to connect to a remote Databricks cluster
"""

import os

from dotenv import load_dotenv

from shared.spark_utils import SparkSessionManager


def main():
    """Main function to demonstrate remote Spark connection"""

    # Load environment variables from .env file
    load_dotenv()

    # Check if required environment variables are set
    databricks_host = os.getenv("DATABRICKS_HOST")
    databricks_token = os.getenv("DATABRICKS_TOKEN")

    if not databricks_host or not databricks_token:
        print("❌ Error: DATABRICKS_HOST and DATABRICKS_TOKEN must be set!")
        print("Please create a .env file with your Databricks credentials:")
        print("  DATABRICKS_HOST=https://your-workspace.databricks.com")
        print("  DATABRICKS_TOKEN=your-personal-access-token")
        print("  DATABRICKS_CLUSTER_ID=your-cluster-id  # Optional")
        return

    print("🚀 Connecting to remote Databricks cluster...")
    print(f"   Host: {databricks_host}")
    print(
        f"   Cluster ID: {os.getenv('DATABRICKS_CLUSTER_ID', 'Not specified')}")

    try:
        # Get remote Spark session
        spark = SparkSessionManager.get_session("remote", "RemoteSparkExample")

        print("✅ Successfully connected to remote Spark cluster!")
        print(f"   Spark Version: {spark.version}")
        print(f"   Application ID: {spark.sparkContext.applicationId}")

        # Test the connection with a simple query
        print("\n📊 Running test query...")
        df = spark.sql(
            "SELECT 'Hello from remote Spark!' as message, current_timestamp() as timestamp")
        df.show()

        # Show available databases (if using Unity Catalog)
        print("\n🗄️  Available databases:")
        databases = spark.sql("SHOW DATABASES")
        databases.show()

    except Exception as e:
        print(f"❌ Error connecting to remote Spark: {e}")
        print("\nTroubleshooting tips:")
        print("1. Verify your DATABRICKS_HOST and DATABRICKS_TOKEN are correct")
        print("2. Ensure your Databricks cluster is running")
        print("3. Check that your token has the necessary permissions")
        print("4. If using a specific cluster, verify DATABRICKS_CLUSTER_ID is correct")

    finally:
        # Clean up
        SparkSessionManager.stop_session()
        print("\n🔄 Spark session stopped.")


if __name__ == "__main__":
    main()
