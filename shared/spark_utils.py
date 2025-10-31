"""
Shared configuration and utility functions for PySpark projects
"""

import os
from typing import Any, Dict, Optional

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


class SparkConfig:
    """Centralized Spark configuration management"""

    @staticmethod
    def get_base_config() -> Dict[str, Any]:
        """Get base Spark configuration"""
        return {
            "spark.app.name": "PySpark-Example",
            "spark.master": "local[*]",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.repl.eagerEval.enabled": "true",
            "spark.sql.repl.eagerEval.maxNumRows": "20",
        }

    @staticmethod
    def get_streaming_config() -> Dict[str, Any]:
        """Get Spark Streaming specific configuration"""
        base_config = SparkConfig.get_base_config()
        streaming_config = {
            "spark.app.name": "PySpark-Streaming",
            "spark.sql.streaming.checkpointLocation": "./data/checkpoints",
            "spark.sql.streaming.forceDeleteTempCheckpointLocation": "true",
        }
        return {**base_config, **streaming_config}

    @staticmethod
    def get_delta_config() -> Dict[str, Any]:
        """Get Delta Lake specific configuration"""
        base_config = SparkConfig.get_base_config()
        delta_config = {
            "spark.app.name": "PySpark-Delta",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.warehouse.dir": "./data/warehouse",
        }
        return {**base_config, **delta_config}

    @staticmethod
    def get_unity_catalog_config() -> Dict[str, Any]:
        """Get Unity Catalog specific configuration"""
        base_config = SparkConfig.get_base_config()
        unity_config = {
            "spark.app.name": "PySpark-Unity-Catalog",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.databricks.service.address": os.getenv("DATABRICKS_HOST", ""),
            "spark.databricks.service.token": os.getenv("DATABRICKS_TOKEN", ""),
        }
        return {**base_config, **unity_config}

    @staticmethod
    def get_remote_config() -> Dict[str, Any]:
        """Get remote Databricks cluster configuration"""
        databricks_host = os.getenv("DATABRICKS_HOST", "")
        databricks_token = os.getenv("DATABRICKS_TOKEN", "")
        cluster_id = os.getenv("DATABRICKS_CLUSTER_ID", "")

        if not databricks_host or not databricks_token:
            raise ValueError(
                "DATABRICKS_HOST and DATABRICKS_TOKEN environment variables must be set for remote connections"
            )

        remote_config = {
            "spark.app.name": "PySpark-Remote",
            "spark.databricks.service.address": databricks_host,
            "spark.databricks.service.token": databricks_token,
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        }

        if cluster_id:
            remote_config["spark.databricks.service.clusterId"] = cluster_id

        return remote_config


class SparkSessionManager:
    """Manage Spark session lifecycle"""

    _instance: Optional[SparkSession] = None

    @classmethod
    def get_session(cls, config_type: str = "base", app_name: Optional[str] = None) -> SparkSession:
        """
        Get or create Spark session with specified configuration

        Args:
            config_type: Type of configuration ('base', 'streaming', 'delta', 'unity', 'remote')
            app_name: Optional custom app name

        Returns:
            SparkSession instance
        """
        if cls._instance is not None:
            return cls._instance

        # Get configuration based on type (using lazy evaluation)
        if config_type == "base":
            config = SparkConfig.get_base_config()
        elif config_type == "streaming":
            config = SparkConfig.get_streaming_config()
        elif config_type == "delta":
            config = SparkConfig.get_delta_config()
        elif config_type == "unity":
            config = SparkConfig.get_unity_catalog_config()
        elif config_type == "remote":
            config = SparkConfig.get_remote_config()
        else:
            config = SparkConfig.get_base_config()

        if app_name:
            config["spark.app.name"] = app_name

        # Create Spark configuration
        spark_conf = SparkConf()
        for key, value in config.items():
            spark_conf.set(key, value)

        # Create Spark session
        builder = SparkSession.builder.config(conf=spark_conf)

        # Add additional packages based on config type
        if config_type in ["delta", "unity"]:
            builder = builder.config(
                "spark.jars.packages",
                "io.delta:delta-core_2.12:2.4.0"
            )

        cls._instance = builder.getOrCreate()
        cls._instance.sparkContext.setLogLevel("WARN")

        return cls._instance

    @classmethod
    def stop_session(cls):
        """Stop the current Spark session"""
        if cls._instance is not None:
            cls._instance.stop()
            cls._instance = None


def create_sample_data_dir():
    """Create sample data directories"""
    directories = [
        "./data/input",
        "./data/output",
        "./data/checkpoints",
        "./data/delta",
        "./data/warehouse",
        "./logs"
    ]

    for directory in directories:
        os.makedirs(directory, exist_ok=True)


def get_project_root() -> str:
    """Get the project root directory"""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def setup_logging():
    """Setup basic logging configuration"""
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('./logs/pyspark-examples.log'),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger(__name__)
