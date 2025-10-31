#!/bin/bash

echo "🚀 Setting up PySpark Development Environment..."

# Update system packages
sudo apt-get update

# Install additional system dependencies
sudo apt-get install -y wget curl unzip lsof

# Initialize conda for bash shell
echo "🔧 Initializing conda..."
/opt/conda/bin/conda init bash
source ~/.bashrc

# Verify conda is working
echo "📋 Conda version:"
/opt/conda/bin/conda --version

# Set up conda environments for each project
echo "📦 Creating conda environments..."

# Base PySpark environment
echo "Creating base PySpark environment..."
/opt/conda/bin/conda env create -f environments/base-pyspark.yml

# Streaming environment
echo "Creating streaming environment..."
/opt/conda/bin/conda env create -f environments/streaming.yml

# Delta Lake environment
echo "Creating Delta Lake environment..."
/opt/conda/bin/conda env create -f environments/delta-lake.yml

# Unity Catalog environment
echo "Creating Unity Catalog environment..."
/opt/conda/bin/conda env create -f environments/unity-catalog.yml

# Set permissions for scripts
chmod +x scripts/*.sh

# Create necessary directories
mkdir -p data/{input,output,checkpoints}
mkdir -p logs

# Generate initial sample data
echo "🎲 Generating sample data..."
/opt/conda/bin/conda run -n base-pyspark python shared/data_generator.py || echo "Sample data generation will be available after first run"

echo "✅ PySpark Development Environment setup complete!"
echo ""
echo "Available conda environments:"
/opt/conda/bin/conda env list
echo ""
echo "To activate an environment, use:"
echo "  conda activate base-pyspark      # Base PySpark functionality"
echo "  conda activate streaming         # Streaming examples"
echo "  conda activate delta-lake        # Delta Lake examples"
echo "  conda activate unity-catalog     # Unity Catalog examples"
echo ""
echo "🚀 Quick start:"
echo "  1. Open a terminal"
echo "  2. Run: conda activate base-pyspark"
echo "  3. Run: jupyter notebook"
echo "  4. Navigate to notebooks/02-streaming-demo.ipynb"
echo ""
echo "Check the README.md for detailed usage instructions."