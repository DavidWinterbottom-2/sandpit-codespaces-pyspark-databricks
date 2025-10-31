# PySpark Examples with Codespaces

A comprehensive collection of PySpark examples showcasing streaming, Delta Lake, and Unity Catalog capabilities, all set up for GitHub Codespaces with conda environment management.

## 🚀 Quick Start

### Option 1: GitHub Codespaces (Recommended)
1. **Open in Codespaces**: Click the green "Code" button and select "Open with Codespaces"
2. **Wait for setup**: The devcontainer will automatically install all dependencies
3. **SSH Keys** (Optional): For Git operations, see [SSH Setup Guide](.devcontainer/SSH_SETUP.md)
4. **Activate environment**: Choose your project-specific conda environment
5. **Run examples**: Navigate to any project directory and run the examples

### Option 2: Local Development
1. **Clone the repository**: `git clone <repo-url>`
2. **Run local setup**: `./scripts/local-setup.sh` (installs conda if needed)
3. **Activate environment**: `conda activate base-pyspark`
4. **Start Jupyter**: `./scripts/start-jupyter.sh`

### Option 3: Using pip (Alternative)
```bash
# Create virtual environment
python -m venv pyspark-env
source pyspark-env/bin/activate  # On Windows: pyspark-env\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Start Jupyter
jupyter notebook
```

## 📁 Project Structure

```
├── .devcontainer/          # GitHub Codespaces configuration
│   ├── devcontainer.json   # Container and VS Code settings
│   └── post-create.sh      # Automatic setup script
├── environments/           # Conda environment files
│   ├── base-pyspark.yml    # Base PySpark environment
│   ├── streaming.yml       # Streaming-specific packages
│   ├── delta-lake.yml      # Delta Lake packages
│   └── unity-catalog.yml   # Unity Catalog packages
├── projects/               # Example projects
│   ├── streaming/          # PySpark Streaming examples
│   ├── delta-lake/         # Delta Lake examples
│   └── unity-catalog/      # Unity Catalog examples
├── shared/                 # Shared utilities and configuration
│   ├── spark_utils.py      # Spark session management
│   └── data_generator.py   # Sample data generation
├── scripts/                # Helper scripts
├── data/                   # Generated data and output
└── notebooks/              # Jupyter notebooks
```

## 🔧 Environment Setup

### Conda Environments

This project uses separate conda environments for different PySpark components:

| Environment | Purpose | Key Packages |
|-------------|---------|--------------|
| `base-pyspark` | General PySpark development | PySpark 3.5.0, Pandas, NumPy |
| `streaming` | Streaming examples | Kafka, WebSocket clients |
| `delta-lake` | Delta Lake examples | Delta Lake, MLflow |
| `unity-catalog` | Unity Catalog examples | Databricks SDK, Unity Catalog |

### Activating Environments

```bash
# Base PySpark environment
conda activate base-pyspark

# Streaming examples
conda activate streaming

# Delta Lake examples
conda activate delta-lake

# Unity Catalog examples
conda activate unity-catalog
```

## 📊 Project Examples

### 1. PySpark Streaming (`projects/streaming/`)

Real-time data processing with Structured Streaming:

- **Socket Streaming**: Real-time event processing from socket connections
- **File Streaming**: Processing files as they arrive
- **Stateful Operations**: Session tracking and windowed aggregations
- **Multiple Sources**: Socket, file, and Kafka sources

**Run the examples**:
```bash
conda activate streaming
cd projects/streaming
python streaming_examples.py
```

**Features**:
- Window-based aggregations
- Watermarking for late data
- Checkpointing for fault tolerance
- Real-time analytics dashboards

### 2. Delta Lake (`projects/delta-lake/`)

ACID transactions and data versioning:

- **CRUD Operations**: Create, read, update, delete with ACID guarantees
- **Time Travel**: Access historical versions of data
- **Schema Evolution**: Safe schema changes and migrations
- **UPSERT Operations**: Efficient merge operations

**Run the examples**:
```bash
conda activate delta-lake
cd projects/delta-lake
python delta_examples.py
```

**Features**:
- Version control for data
- Schema enforcement and evolution
- Concurrent read/write operations
- Automatic file compaction

### 3. Unity Catalog (`projects/unity-catalog/`)

Data governance and cataloging:

- **Data Governance**: Column and row-level security
- **Data Lineage**: Track data transformations
- **Data Discovery**: Automatic metadata cataloging
- **Access Control**: Fine-grained permissions

**Run the examples**:
```bash
conda activate unity-catalog
cd projects/unity-catalog
python unity_catalog_examples.py
```

**Features**:
- Three-level namespace (Catalog.Schema.Table)
- Automated data classification
- Lineage tracking and visualization
- Data quality monitoring

### 4. Remote Spark Connection

Connect to remote Databricks clusters using Databricks Connect:

- **Remote Execution**: Run code on Databricks clusters from local environment
- **Interactive Development**: Use Jupyter notebooks with remote compute
- **Production Integration**: Connect to production Databricks workspaces
- **Unity Catalog Access**: Full access to Unity Catalog from local development

**Setup remote connection**:
```bash
# Configure your Databricks credentials
./scripts/configure-remote.sh

# Or manually create .env file with:
# DATABRICKS_HOST=https://your-workspace.databricks.com
# DATABRICKS_TOKEN=your-personal-access-token
# DATABRICKS_CLUSTER_ID=your-cluster-id  # Optional
```

**Use in Python**:
```python
from shared.spark_utils import SparkSessionManager

# Connect to remote Databricks cluster
spark = SparkSessionManager.get_session("remote", "MyRemoteApp")

# Now you can use Spark normally
df = spark.sql("SELECT * FROM my_table")
df.show()
```

**Run the examples**:
```bash
conda activate delta-lake  # or unity-catalog
python examples/remote_spark_example.py

# Or use the Jupyter notebook
jupyter notebook notebooks/03-remote-spark-connection.ipynb
```

**Features**:
- Seamless remote cluster connectivity
- Support for Delta Lake and Unity Catalog
- Interactive Jupyter notebook development
- Production data access from local environment

## 🛠️ Utility Scripts

### Environment Management

```bash
# Setup all environments
./scripts/setup-environments.sh

# Activate specific environment
./scripts/activate-env.sh streaming

# Generate sample data
./scripts/generate-data.sh

# Clean up data and logs
./scripts/cleanup.sh
```

### Development Tools

```bash
# Start Jupyter notebook server
./scripts/start-jupyter.sh

# Run all examples
./scripts/run-all-examples.sh

# Check environment status
./scripts/check-environments.sh
```

## 📓 Jupyter Notebooks

Interactive notebooks are available in the `notebooks/` directory:

- `01-pyspark-basics.ipynb` - PySpark fundamentals
- `02-streaming-demo.ipynb` - Interactive streaming examples
- `03-delta-lake-tutorial.ipynb` - Delta Lake hands-on tutorial
- `04-unity-catalog-demo.ipynb` - Unity Catalog walkthrough

**Start Jupyter**:
```bash
conda activate base-pyspark
jupyter notebook
```

## 🌐 Monitoring and UI

When running examples, several UIs are available:

| Service | URL | Purpose |
|---------|-----|---------|
| Spark UI | http://localhost:4040 | Job monitoring and performance |
| Spark Master | http://localhost:8080 | Cluster status (if applicable) |
| Jupyter | http://localhost:8888 | Interactive development |
| Kafka | localhost:9092 | Streaming message broker |

## 🔍 Key Features

### GitHub Codespaces Integration
- **One-click setup**: Fully configured development environment
- **VS Code extensions**: Python, Jupyter, Databricks extensions pre-installed
- **Port forwarding**: Automatic forwarding of Spark UI and Jupyter ports
- **Persistent storage**: Data and configurations persist across sessions

### Conda Environment Management
- **Isolated environments**: Separate environments for different use cases
- **Dependency management**: Precise version control for all packages
- **Easy switching**: Simple commands to switch between environments
- **Reproducible builds**: Consistent environments across different machines

### Comprehensive Examples
- **Real-world scenarios**: Practical examples with realistic data
- **Best practices**: Industry-standard patterns and configurations
- **Performance optimization**: Tuned Spark configurations for each use case
- **Error handling**: Robust error handling and logging

## 📝 Configuration

### Spark Configuration

Each environment includes optimized Spark configurations:

```python
# Streaming configuration
spark.conf.set("spark.sql.streaming.checkpointLocation", "./data/checkpoints")
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Delta Lake configuration
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Unity Catalog configuration
spark.conf.set("spark.databricks.service.address", databricks_host)
spark.conf.set("spark.databricks.service.token", databricks_token)
```

### Environment Variables

For production use with Databricks:

```bash
export DATABRICKS_HOST="https://your-workspace.databricks.com"
export DATABRICKS_TOKEN="your-access-token"
export SPARK_LOCAL_IP="127.0.0.1"
```

## 🚨 Troubleshooting

### Common Issues

1. **Java Version Issues**:
   ```bash
   # Check Java version
   java -version
   # Should be Java 11
   ```

2. **Memory Issues**:
   ```bash
   # Increase driver memory
   export SPARK_DRIVER_MEMORY=4g
   ```

3. **Port Conflicts**:
   ```bash
   # Check port usage
   netstat -tlnp | grep :4040
   ```

4. **Environment Activation**:
   ```bash
   # Verify conda environments
   conda env list
   # Recreate if needed
   conda env create -f environments/streaming.yml --force
   ```

### Performance Tuning

```python
# Optimize for local development
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.maxBatchSize", "128MB")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add your examples or improvements
4. Test in all relevant environments
5. Submit a pull request

### Development Guidelines

- Follow PEP 8 for Python code
- Include comprehensive docstrings
- Add unit tests for new functionality
- Update documentation for new features
- Test with multiple Spark versions

## 📚 Learning Resources

### Official Documentation
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Databricks Unity Catalog](https://docs.databricks.com/unity-catalog/)

### Tutorials and Guides
- [PySpark Tutorial](https://spark.apache.org/docs/latest/api/python/)
- [Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Delta Lake Quickstart](https://docs.delta.io/latest/quick-start.html)

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Apache Spark community
- Delta Lake contributors
- Databricks team
- GitHub Codespaces team

---

**Happy Sparking! 🎉**

For questions or issues, please open a GitHub issue or start a discussion.