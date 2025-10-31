#!/bin/bash

# Setup all conda environments for PySpark projects
echo "🔧 Setting up PySpark conda environments..."

# Check if conda is available
if ! command -v conda &> /dev/null; then
    echo "❌ Conda is not available. Please install conda first."
    exit 1
fi

# Setup base PySpark environment
echo "📦 Creating base-pyspark environment..."
if conda env list | grep -q "base-pyspark"; then
    echo "   Environment 'base-pyspark' already exists, removing and recreating..."
    conda env remove -n base-pyspark -y 2>/dev/null || true
fi
conda env create -f environments/base-pyspark.yml

# Setup streaming environment
echo "📦 Creating streaming environment..."
if conda env list | grep -q "streaming"; then
    echo "   Environment 'streaming' already exists, removing and recreating..."
    conda env remove -n streaming -y 2>/dev/null || true
fi
conda env create -f environments/streaming.yml

# Setup Delta Lake environment
echo "📦 Creating delta-lake environment..."
if conda env list | grep -q "delta-lake"; then
    echo "   Environment 'delta-lake' already exists, removing and recreating..."
    conda env remove -n delta-lake -y 2>/dev/null || true
fi
conda env create -f environments/delta-lake.yml

# Setup Unity Catalog environment
echo "📦 Creating unity-catalog environment..."
if conda env list | grep -q "unity-catalog"; then
    echo "   Environment 'unity-catalog' already exists, removing and recreating..."
    conda env remove -n unity-catalog -y 2>/dev/null || true
fi
conda env create -f environments/unity-catalog.yml

echo "✅ All environments setup complete!"
echo ""
echo "Available environments:"
conda env list | grep -E "(base-pyspark|streaming|delta-lake|unity-catalog)"
echo ""
echo "To activate an environment:"
echo "   conda activate base-pyspark"
echo "   conda activate streaming"
echo "   conda activate delta-lake"
echo "   conda activate unity-catalog"