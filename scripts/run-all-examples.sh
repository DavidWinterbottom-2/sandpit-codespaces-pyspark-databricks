#!/bin/bash

# Run all PySpark examples
echo "🚀 Running all PySpark examples..."

# Check if environments exist
environments=("base-pyspark" "streaming" "delta-lake" "unity-catalog")
for env in "${environments[@]}"; do
    if ! conda env list | grep -q "$env"; then
        echo "❌ Environment '$env' not found. Run setup-environments.sh first."
        exit 1
    fi
done

# Source conda
source "$(conda info --base)/etc/profile.d/conda.sh"

# Generate sample data first
echo "1. Generating sample data..."
conda activate base-pyspark
cd shared
python -c "
from data_generator import save_sample_data_to_files
from spark_utils import SparkSessionManager
spark = SparkSessionManager.get_session('base', 'DataGenerator')
save_sample_data_to_files(spark)
SparkSessionManager.stop_session()
"
cd ..

echo ""
echo "2. Running Streaming examples..."
conda activate streaming
cd projects/streaming
timeout 120 python streaming_examples.py || echo "Streaming examples completed (or timed out)"
cd ../..

echo ""
echo "3. Running Delta Lake examples..."
conda activate delta-lake
cd projects/delta-lake
python delta_examples.py
cd ../..

echo ""
echo "4. Running Unity Catalog examples..."
conda activate unity-catalog
cd projects/unity-catalog
python unity_catalog_examples.py
cd ../..

echo ""
echo "✅ All examples completed!"
echo ""
echo "📊 Generated outputs:"
echo "   - Data files: ./data/"
echo "   - Delta tables: ./data/delta/"
echo "   - Metadata: ./data/metadata/"
echo "   - Lineage: ./data/lineage/"
echo "   - Logs: ./logs/"
echo ""
echo "🌐 Spark UI was available at: http://localhost:4040"