#!/bin/bash

# Clean up generated data and logs
echo "🧹 Cleaning up PySpark project data and logs..."

# Confirm before deletion
read -p "This will delete all generated data, logs, and checkpoints. Continue? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Cleanup cancelled."
    exit 1
fi

echo "🗑️  Removing data directories..."
rm -rf data/input/*
rm -rf data/output/*
rm -rf data/checkpoints/*
rm -rf data/delta/*
rm -rf data/warehouse/*
rm -rf data/metadata/*
rm -rf data/lineage/*

echo "🗑️  Removing log files..."
rm -rf logs/*

echo "🗑️  Removing temporary files..."
find . -name "*.pyc" -delete
find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null
find . -name ".ipynb_checkpoints" -type d -exec rm -rf {} + 2>/dev/null

echo "🗑️  Removing Spark temporary directories..."
rm -rf spark-warehouse
rm -rf metastore_db
rm -rf derby.log

echo "✅ Cleanup completed!"
echo ""
echo "📁 Preserved directories:"
echo "   - data/ (empty subdirectories)"
echo "   - logs/ (empty)"
echo "   - environments/ (conda environment files)"
echo "   - projects/ (example code)"
echo "   - shared/ (utilities)"
echo "   - scripts/ (helper scripts)"
echo ""
echo "To regenerate sample data, run:"
echo "   ./scripts/generate-data.sh"