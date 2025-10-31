#!/bin/bash

# Script to build and run the devcontainer locally with Docker Desktop

echo "🐳 Building and running PySpark devcontainer locally..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker Desktop."
    exit 1
fi

echo "✅ Docker is running"

# Check if we're in the right directory
if [ ! -f ".devcontainer/devcontainer.json" ]; then
    echo "❌ Please run this script from the project root directory"
    exit 1
fi

# Build the devcontainer
echo "🔨 Building devcontainer..."
docker build -f .devcontainer/Dockerfile -t pyspark-devcontainer .

# Run the container
echo "🚀 Starting devcontainer..."
docker run -it \
    --name pyspark-dev \
    -p 4040:4040 \
    -p 8080:8080 \
    -p 8888:8888 \
    -p 9092:9092 \
    -v "$(pwd)":/workspace \
    -w /workspace \
    pyspark-devcontainer bash

echo "🎉 Devcontainer is running!"
echo "You can now run:"
echo "  conda activate base-pyspark"
echo "  jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root"