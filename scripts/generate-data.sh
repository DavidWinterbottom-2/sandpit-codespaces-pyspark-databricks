#!/bin/bash

# Generate sample data for all projects
echo "🎲 Generating sample data for PySpark examples..."

# Create data directories
mkdir -p data/{input,output,checkpoints,delta,warehouse,metadata,lineage}
mkdir -p logs

# Activate base environment to generate data
echo "📊 Activating base-pyspark environment..."
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate base-pyspark

# Generate sample data using the data generator
echo "📈 Generating sample datasets..."
cd shared
python -c "
from data_generator import save_sample_data_to_files
from spark_utils import SparkSessionManager

spark = SparkSessionManager.get_session('base', 'DataGenerator')
save_sample_data_to_files(spark)
SparkSessionManager.stop_session()
print('✅ Sample data generation completed!')
"

cd ..

echo ""
echo "📁 Generated data files:"
echo "   CSV files: ./data/input/customers/, ./data/input/transactions/, ./data/input/sensors/"
echo "   Parquet files: ./data/input/*_parquet/"
echo "   Streaming data: ./data/input/streaming_events.jsonl"
echo ""
echo "🚀 Data is ready for use with all examples!"