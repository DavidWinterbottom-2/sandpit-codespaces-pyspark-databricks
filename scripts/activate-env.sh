#!/bin/bash

# Activate specific conda environment
ENV_NAME=$1

if [ -z "$ENV_NAME" ]; then
    echo "Usage: $0 <environment_name>"
    echo ""
    echo "Available environments:"
    echo "  base-pyspark    - Base PySpark functionality"
    echo "  streaming       - Streaming examples"
    echo "  delta-lake      - Delta Lake examples"
    echo "  unity-catalog   - Unity Catalog examples"
    exit 1
fi

# Check if environment exists
if ! conda env list | grep -q "$ENV_NAME"; then
    echo "❌ Environment '$ENV_NAME' does not exist."
    echo "Available environments:"
    conda env list | grep -E "(base-pyspark|streaming|delta-lake|unity-catalog)"
    echo ""
    echo "Run './scripts/setup-environments.sh' to create environments."
    exit 1
fi

echo "🔄 Activating conda environment: $ENV_NAME"
conda activate "$ENV_NAME"

echo "✅ Environment '$ENV_NAME' activated!"
echo ""
echo "Python location: $(which python)"
echo "Python version: $(python --version)"
echo ""
echo "Available packages:"
pip list | grep -i spark