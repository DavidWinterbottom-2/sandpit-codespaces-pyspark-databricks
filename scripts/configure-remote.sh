#!/bin/bash

# Configuration script for remote Databricks connection
echo "🔧 Setting up remote Databricks connection..."

# Check if .env file exists
if [ -f ".env" ]; then
    echo "⚠️  .env file already exists. Backing up to .env.backup"
    cp .env .env.backup
fi

# Prompt for Databricks credentials
echo ""
echo "Please provide your Databricks connection details:"
echo ""

read -p "Databricks Host (e.g., https://your-workspace.databricks.com): " DATABRICKS_HOST
read -s -p "Databricks Token (personal access token): " DATABRICKS_TOKEN
echo ""
read -p "Databricks Cluster ID (optional, press Enter to skip): " DATABRICKS_CLUSTER_ID

# Create .env file
cat > .env << EOF
# Databricks Connection Configuration
DATABRICKS_HOST=$DATABRICKS_HOST
DATABRICKS_TOKEN=$DATABRICKS_TOKEN
EOF

if [ ! -z "$DATABRICKS_CLUSTER_ID" ]; then
    echo "DATABRICKS_CLUSTER_ID=$DATABRICKS_CLUSTER_ID" >> .env
fi

echo ""
echo "✅ Configuration saved to .env file"
echo ""
echo "🚀 To test your remote connection, you can now:"
echo "   1. Activate the appropriate conda environment:"
echo "      conda activate delta-lake"
echo "      # or"
echo "      conda activate unity-catalog"
echo ""
echo "   2. Run the remote connection example:"
echo "      python examples/remote_spark_example.py"
echo ""
echo "   3. Or use in your own scripts:"
echo "      from shared.spark_utils import SparkSessionManager"
echo "      spark = SparkSessionManager.get_session('remote')"
echo ""

# Set proper permissions
chmod 600 .env
echo "🔒 Set secure permissions on .env file (600)"
echo ""
echo "⚠️  Important: Never commit the .env file to version control!"
echo "   The .env file contains sensitive credentials."