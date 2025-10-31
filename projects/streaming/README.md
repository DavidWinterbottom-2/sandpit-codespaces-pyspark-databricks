# PySpark Streaming Examples

This directory contains comprehensive examples of PySpark Structured Streaming capabilities.

## Environment Setup

### Local Streaming Environment (`streaming-local`)
- **Purpose**: Local development and testing of streaming applications
- **Spark Mode**: Local Spark session (no remote dependencies)
- **Use Case**: Development, debugging, and learning streaming concepts
- **Dependencies**: PySpark 3.5.0, Kafka clients, Jupyter, pandas, numpy
- **Activation**: `source ../../scripts/activate-env.sh streaming-local`

### Remote Streaming Environment (`streaming`)
- **Purpose**: Production streaming with Databricks Connect
- **Spark Mode**: Remote Databricks cluster connection
- **Use Case**: Production workloads, enterprise streaming applications
- **Dependencies**: PySpark 3.5.0, Databricks Connect, Kafka clients
- **Activation**: `source ../../scripts/activate-env.sh streaming`
- **Requirements**: Valid Databricks workspace, cluster, and access token

## Examples Included

### 1. Socket Streaming (`streaming_examples.py`)
- **Socket-based streaming**: Real-time data processing from socket connections
- **Real-time aggregations**: Window-based operations on streaming data  
- **Event processing**: JSON parsing and transformation of streaming events

### 2. File Streaming
- **File-based streaming**: Processing files as they arrive in a directory
- **Batch processing**: Configurable batch size and trigger intervals
- **Output to multiple sinks**: Console and file outputs

### 3. Stateful Streaming
- **Session tracking**: Maintaining state across streaming batches
- **Watermarking**: Handling late-arriving data
- **Complex aggregations**: Multi-level grouping and calculations

## Key Features Demonstrated

- **Structured Streaming API**: Modern streaming framework
- **Window Operations**: Time-based aggregations
- **Watermarking**: Handling late data and state cleanup
- **Multiple Sources**: Socket, file, and custom sources
- **Multiple Sinks**: Console, file, and custom outputs
- **Error Handling**: Robust streaming application patterns
- **Approximate Aggregations**: Using `approx_count_distinct()` for streaming compatibility

## Usage

### For Local Development
1. **Activate the local streaming environment**:
   ```bash
   source ../../scripts/activate-env.sh streaming-local
   ```

2. **Run the examples**:
   ```bash
   cd projects/streaming
   python streaming_examples.py
   ```

3. **Monitor the output**: Watch real-time processing in the console

### For Remote/Production
1. **Setup Databricks connection** (see main README for configuration)
2. **Activate the remote streaming environment**:
   ```bash
   source ../../scripts/activate-env.sh streaming
   ```

3. **Configure environment variables**:
   ```bash
   export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
   export DATABRICKS_TOKEN="your-access-token"
   export DATABRICKS_CLUSTER_ID="your-cluster-id"
   ```

4. **Run examples on remote cluster**

## Environment Comparison

| Feature | `streaming-local` | `streaming` |
|---------|-------------------|-------------|
| **Spark Session** | Local | Remote (Databricks) |
| **Development** | ✅ Ideal | ⚠️ Slower feedback |
| **Production** | ❌ Limited scale | ✅ Enterprise ready |
| **Cost** | 💰 Free | 💰💰 Cluster costs |
| **Setup** | 🟢 Simple | 🟡 Requires credentials |
| **Debugging** | 🟢 Easy | 🟡 More complex |
| **Performance** | 🟡 Single machine | 🟢 Distributed |

## Configuration

### Local Environment Packages:
- PySpark 3.5.0 (standalone)
- Kafka Python clients (2.0.2)
- Confluent Kafka (2.3.0)
- Jupyter notebook support
- Standard data science libraries

### Remote Environment Packages:
- PySpark 3.5.0 
- Databricks Connect 13.3.2
- Kafka Python clients
- Unity Catalog support
- MLflow integration

## Monitoring

- **Spark UI**: Available at http://localhost:4040 (local) or Databricks UI (remote)
- **Streaming Tab**: Real-time metrics and batch processing info
- **Console Output**: Live streaming results
- **Databricks UI**: Advanced monitoring for remote execution

## Advanced Features

- **Checkpointing**: Fault-tolerance and state recovery
- **Trigger Modes**: Processing time and continuous triggers
- **Output Modes**: Append, update, and complete modes
- **Schema Evolution**: Handling changing data schemas
- **Watermarking**: Late data handling with event-time processing

## Troubleshooting

### Common Issues:
1. **Import errors**: Ensure you're using the correct environment (`streaming-local` for local development)
2. **Databricks connection**: Verify credentials and cluster status for remote execution
3. **Port conflicts**: Socket examples use port 9999 - ensure it's available
4. **Memory issues**: Adjust Spark configuration for local resource constraints

### Quick Fixes:
```bash
# Check active environment
conda info --envs

# Switch to local development
source ../../scripts/activate-env.sh streaming-local

# Verify packages
pip list | grep -E "(pyspark|kafka)"
```