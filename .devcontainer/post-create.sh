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
# Set up SSH keys if they exist
echo "🔑 Setting up SSH keys..."
if [ -d "/home/vscode/.ssh" ] && [ -f "/home/vscode/.ssh/id_rsa" ]; then
    # Set proper permissions for SSH keys
    chmod 700 /home/vscode/.ssh
    chmod 600 /home/vscode/.ssh/*
    chmod 644 /home/vscode/.ssh/*.pub 2>/dev/null || true
    
    # Start SSH agent and add keys if not already running
    if [ -z "$SSH_AUTH_SOCK" ]; then
        eval "$(ssh-agent -s)"
        ssh-add /home/vscode/.ssh/id_rsa 2>/dev/null || true
    fi
    
    echo "✅ SSH keys configured successfully"
else
    echo "ℹ️  No SSH keys found in ~/.ssh directory"
    echo "   To add SSH keys, copy them to ~/.ssh/ in your local machine"
fi

echo ""
echo "🚀 Quick start:"
echo "  1. Open a terminal"
echo "  2. Run: conda activate base-pyspark"
echo "  3. Run: jupyter notebook"
echo "  4. Navigate to notebooks/02-streaming-demo.ipynb"
echo ""
echo "🔑 SSH Configuration:"
echo "  - SSH keys are mounted from your local ~/.ssh directory"
echo "  - Use 'ssh-add -l' to list loaded keys"
echo "  - Use 'git config --global user.name \"Your Name\"' to set Git config"
echo ""
echo "Check the README.md for detailed usage instructions."