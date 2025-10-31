# Logs directory for PySpark applications
# This directory will contain application logs

# Example log files that will be created:
# - pyspark-examples.log: General application logs
# - spark-driver.log: Spark driver logs  
# - spark-executor.log: Spark executor logs
# - streaming-app.log: Streaming application logs
# - delta-operations.log: Delta Lake operation logs
# - unity-catalog.log: Unity Catalog operation logs

# Logs are automatically rotated and managed by the logging configuration
# in shared/spark_utils.py

*.log