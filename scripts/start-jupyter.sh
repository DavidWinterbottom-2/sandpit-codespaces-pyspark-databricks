#!/bin/bash

# Start Jupyter notebook server
echo "🚀 Starting Jupyter Notebook server..."

# Initialize conda if not already done
if [ -f "/opt/conda/etc/profile.d/conda.sh" ]; then
    source "/opt/conda/etc/profile.d/conda.sh"
elif [ -f "$HOME/miniconda3/etc/profile.d/conda.sh" ]; then
    source "$HOME/miniconda3/etc/profile.d/conda.sh"
elif [ -f "$HOME/anaconda3/etc/profile.d/conda.sh" ]; then
    source "$HOME/anaconda3/etc/profile.d/conda.sh"
fi

# Check if base-pyspark environment exists
if ! conda env list | grep -q "base-pyspark"; then
    echo "❌ base-pyspark environment not found. Creating it now..."
    conda env create -f environments/base-pyspark.yml
fi

# Activate base environment
conda activate base-pyspark

# Create notebooks directory if it doesn't exist
mkdir -p notebooks

# Create a sample notebook if none exist
if [ ! -f "notebooks/01-pyspark-basics.ipynb" ]; then
    echo "📓 Creating sample notebooks..."
    
    cat > notebooks/01-pyspark-basics.ipynb << 'EOF'
{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# PySpark Basics\n",
    "\n",
    "This notebook demonstrates basic PySpark operations."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "import sys\n",
    "import os\n",
    "sys.path.append('../shared')\n",
    "\n",
    "from spark_utils import SparkSessionManager\n",
    "from data_generator import DataGenerator"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Create Spark session\n",
    "spark = SparkSessionManager.get_session('base', 'JupyterExample')\n",
    "print(f\"Spark version: {spark.version}\")\n",
    "print(f\"Spark UI: http://localhost:4040\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Generate sample data\n",
    "df = DataGenerator.generate_customer_data(spark, 100)\n",
    "df.show(10)\n",
    "df.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Basic analytics\n",
    "print(f\"Total customers: {df.count()}\")\n",
    "df.groupBy('city').count().orderBy('count', ascending=False).show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Stop Spark session\n",
    "SparkSessionManager.stop_session()"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.0"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
EOF
fi

echo "📂 Starting Jupyter in the notebooks directory..."
cd notebooks

# Start Jupyter notebook server
echo "🌐 Jupyter will be available at: http://localhost:8888"
echo "📝 Sample notebooks are available in the notebooks/ directory"
echo ""
echo "Press Ctrl+C to stop the server"

jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root