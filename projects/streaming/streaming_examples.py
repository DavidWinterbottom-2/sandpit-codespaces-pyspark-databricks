#!/usr/bin/env python3
"""
PySpark Streaming Examples

This module demonstrates various PySpark streaming capabilities including:
1. Socket-based streaming
2. File-based streaming  
3. Kafka streaming (if available)
4. Window operations and aggregations
5. Stateful operations

Note: This version uses delayed imports to ensure proper path setup.
"""

import os
import random
import socket
import sys
import threading
import time
from pathlib import Path


def setup_imports():
    """Setup module imports after ensuring proper path configuration."""
    # Add shared modules to path BEFORE importing custom modules
    current_dir = os.path.dirname(os.path.abspath(__file__))
    shared_dir = os.path.join(current_dir, '..', '..', 'shared')
    sys.path.insert(0, shared_dir)

    # Import PySpark modules
    global SparkSession, StructType, StructField, StringType, DoubleType, TimestampType
    global col, window, count, spark_sum, avg, spark_max, spark_min
    global from_json, to_json, split, regexp_extract, approx_count_distinct
    global SparkSessionManager, create_sample_data_dir, setup_logging, DataGenerator

    from data_generator import DataGenerator
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (approx_count_distinct, avg, col, count,
                                       from_json)
    from pyspark.sql.functions import max as spark_max
    from pyspark.sql.functions import min as spark_min
    from pyspark.sql.functions import regexp_extract, split
    from pyspark.sql.functions import sum as spark_sum
    from pyspark.sql.functions import to_json, window
    from pyspark.sql.types import (DoubleType, StringType, StructField,
                                   StructType, TimestampType)
    # Import custom modules AFTER path setup
    from spark_utils import (SparkSessionManager, create_sample_data_dir,
                             setup_logging)

    return setup_logging()


class StreamingSocketServer:
    """Simple socket server that sends test data for streaming examples."""

    def __init__(self, host='localhost', port=9999):
        self.host = host
        self.port = port
        self.running = False
        self.server_thread = None

    def start(self):
        """Start the socket server in a separate thread."""
        if self.running:
            return

        self.running = True
        self.server_thread = threading.Thread(target=self._run_server)
        self.server_thread.daemon = True
        self.server_thread.start()
        time.sleep(1)  # Give server time to start

    def stop(self):
        """Stop the socket server."""
        self.running = False
        if self.server_thread:
            self.server_thread.join(timeout=2)

    def _run_server(self):
        """Run the socket server loop."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
                server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server.bind((self.host, self.port))
                server.listen(1)
                server.settimeout(1.0)  # Timeout to check running flag

                print(f"🔌 Socket server started on {self.host}:{self.port}")

                test_data = [
                    "user1,click,homepage,2024-01-01 10:00:00",
                    "user2,view,product,2024-01-01 10:01:00",
                    "user1,purchase,checkout,2024-01-01 10:02:00",
                    "user3,click,homepage,2024-01-01 10:03:00",
                    "user2,purchase,checkout,2024-01-01 10:04:00"
                ]

                while self.running:
                    try:
                        conn, addr = server.accept()
                        print(f"📡 Client connected from {addr}")

                        with conn:
                            for data in test_data:
                                if not self.running:
                                    break
                                conn.sendall((data + '\n').encode())
                                time.sleep(1)

                    except socket.timeout:
                        continue  # Check running flag
                    except Exception as e:
                        if self.running:  # Only log if we're supposed to be running
                            print(f"⚠️ Server error: {e}")
                        break

        except Exception as e:
            print(f"❌ Failed to start socket server: {e}")


def socket_streaming_example(spark, logger):
    """Demonstrate socket-based streaming."""
    print("\n" + "="*50)
    print("🔌 SOCKET STREAMING EXAMPLE")
    print("="*50)

    # Start socket server
    server = StreamingSocketServer()
    server.start()

    try:

        # Define schema for streaming data
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("action", StringType(), True),
            StructField("page", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])

        # Create streaming DataFrame
        lines = spark \
            .readStream \
            .format("socket") \
            .option("host", "localhost") \
            .option("port", 9999) \
            .load()

        # Parse the data
        parsed_data = lines.select(
            split(col("value"), ",").getItem(0).alias("user_id"),
            split(col("value"), ",").getItem(1).alias("action"),
            split(col("value"), ",").getItem(2).alias("page"),
            split(col("value"), ",").getItem(3).alias("timestamp")
        )

        # Simple aggregation
        user_actions = parsed_data \
            .groupBy("user_id", "action") \
            .count()

        # Start the streaming query
        query = user_actions \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime='2 seconds') \
            .start()

        print("📊 Processing socket stream for 10 seconds...")
        query.awaitTermination(timeout=10)
        query.stop()

    finally:
        server.stop()
        print("🔌 Socket server stopped")


def file_streaming_example(spark, logger):
    """Demonstrate file-based streaming."""
    print("\n" + "="*50)
    print("📁 FILE STREAMING EXAMPLE")
    print("="*50)

    # Create sample data directory
    create_sample_data_dir()
    data_dir = "./data"
    input_path = os.path.join(data_dir, "streaming_input")
    output_path = os.path.join(data_dir, "streaming_output")

    # Create input directory
    os.makedirs(input_path, exist_ok=True)

    # Generate sample data files
    generator = DataGenerator()
    sample_data = generator.generate_streaming_data(100)

    # Write sample files
    for i in range(3):
        file_path = os.path.join(input_path, f"data_{i}.json")
        batch_data = sample_data[i*30:(i+1)*30]  # 30 records per file

        with open(file_path, 'w', encoding='utf-8') as f:
            for record in batch_data:
                f.write(f"{record}\n")

        print(f"📄 Created {file_path} with {len(batch_data)} records")

    # Define schema
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("page", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("product_id", StringType(), True)
    ])

    # Create streaming DataFrame
    df = spark \
        .readStream \
        .format("json") \
        .schema(schema) \
        .option("path", input_path) \
        .option("maxFilesPerTrigger", 1) \
        .load()

    # Perform aggregations
    aggregations = df \
        .groupBy("event_type") \
        .agg(
            count("*").alias("event_count"),
            approx_count_distinct("user_id").alias("unique_users"),
            avg("amount").alias("avg_amount")
        )

    # Start streaming query
    query = aggregations \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='3 seconds') \
        .start()

    print("📊 Processing file stream for 12 seconds...")
    query.awaitTermination(timeout=12)
    query.stop()

    print(f"📁 Input files processed from: {input_path}")


def windowed_aggregation_example(spark, logger):
    """Demonstrate windowed aggregations with streaming."""
    print("\n" + "="*50)
    print("🕐 WINDOWED AGGREGATION EXAMPLE")
    print("="*50)

    # Create sample data directory
    create_sample_data_dir()
    data_dir = "./data"
    input_path = os.path.join(data_dir, "windowed_input")
    os.makedirs(input_path, exist_ok=True)

    # Generate time-series data
    import json
    from datetime import datetime, timedelta
    sample_data = []
    event_types = ["login", "click", "purchase", "logout"]

    for i in range(200):
        event = {
            "timestamp": (datetime.now() - timedelta(minutes=random.randint(-60, 60))).isoformat(),
            "user_id": f"user_{random.randint(1, 50)}",
            "event_type": random.choice(event_types),
            "value": round(random.uniform(1.0, 100.0), 2)
        }
        sample_data.append(json.dumps(event))

    # Write data with timestamps
    file_path = os.path.join(input_path, "timeseries_data.json")
    with open(file_path, 'w', encoding='utf-8') as f:
        for record in sample_data:
            f.write(f"{record}\n")

    print(f"📄 Created {file_path} with {len(sample_data)} records")

    # Schema for time-series data
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("value", DoubleType(), True)
    ])

    # Create streaming DataFrame
    df = spark \
        .readStream \
        .format("json") \
        .schema(schema) \
        .option("path", input_path) \
        .load()

    # Convert timestamp and perform windowed aggregation
    windowed_df = df \
        .withColumn("timestamp", col("timestamp").cast(TimestampType())) \
        .groupBy(
            window(col("timestamp"), "1 minute"),
            "event_type"
        ) \
        .agg(
            count("*").alias("event_count"),
            avg("value").alias("avg_value"),
            spark_max("value").alias("max_value"),
            spark_min("value").alias("min_value")
        )

    # Start windowed streaming query
    query = windowed_df \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='5 seconds') \
        .start()

    print("📊 Processing windowed aggregations for 15 seconds...")
    query.awaitTermination(timeout=15)
    query.stop()


def main():
    """Main function to run all streaming examples."""
    logger = setup_imports()

    print("🚀 PySpark Streaming Examples")
    print("=" * 50)

    try:
        print("⚡ Initializing Spark session...")
        spark = SparkSessionManager.get_session(
            config_type="streaming", app_name="StreamingExamples")
        print(f"✅ Spark session created: {spark.version}")

        # Run examples
        socket_streaming_example(spark, logger)
        file_streaming_example(spark, logger)
        windowed_aggregation_example(spark, logger)

    except Exception as e:
        logger.error(f"❌ Error in streaming examples: {e}")
        print(f"❌ Error in streaming examples: {e}")
    finally:
        SparkSessionManager.stop_session()
        print("\n🏁 All streaming examples completed!")


if __name__ == "__main__":
    main()
