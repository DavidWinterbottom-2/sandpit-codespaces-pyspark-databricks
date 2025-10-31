#!/bin/bash

# Local setup script for running PySpark examples without Codespaces
echo "🔧 Setting up PySpark environment locally..."

# Function to install Miniconda
install_miniconda() {
    echo "📦 Installing Miniconda..."
    
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Linux
        wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
        bash miniconda.sh -b -p $HOME/miniconda3
        export PATH="$HOME/miniconda3/bin:$PATH"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        curl -O https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.sh
        bash Miniconda3-latest-MacOSX-x86_64.sh -b -p $HOME/miniconda3
        export PATH="$HOME/miniconda3/bin:$PATH"
    elif [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "cygwin" ]]; then
        # Windows (Git Bash or Cygwin)
        echo "For Windows, please download and install Miniconda from:"
        echo "https://docs.conda.io/en/latest/miniconda.html"
        echo "Then run this script again."
        exit 1
    fi
    
    # Initialize conda
    $HOME/miniconda3/bin/conda init bash
    eval "$($HOME/miniconda3/bin/conda shell.bash hook)"
    
    echo "✅ Miniconda installed successfully!"
}

# Check if conda is available
if ! command -v conda &> /dev/null; then
    echo "⚠️  Conda not found. Installing Miniconda..."
    install_miniconda
else
    echo "✅ Conda is already available: $(conda --version)"
    # Initialize conda for this shell session
    eval "$(conda shell.bash hook)"
fi

# Check if Java is available
if ! command -v java &> /dev/null; then
    echo "⚠️  Java not found. Please install Java 11 or later for Spark."
    echo "You can install it using:"
    echo "  - Ubuntu/Debian: sudo apt-get install openjdk-11-jdk"
    echo "  - macOS: brew install openjdk@11"
    echo "  - Windows: Download from https://adoptium.net/"
else
    java_version=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2)
    echo "✅ Java is available: $java_version"
fi

# Create conda environments
echo "📦 Creating conda environments..."

# Set up conda environments
./scripts/setup-environments.sh

echo "🎲 Generating sample data..."
# Use simple data generator (no Spark required)
python shared/simple_data_generator.py

echo "✅ Local setup complete!"
echo ""
echo "🚀 To get started:"
echo "  1. Run: conda activate base-pyspark-local"
echo "  2. Run: jupyter notebook"
echo "  3. Open notebooks/02-streaming-demo.ipynb"
echo ""
echo "Or run all examples with:"
echo "  ./scripts/run-all-examples.sh"