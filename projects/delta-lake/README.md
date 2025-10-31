# PySpark Delta Lake Examples

This directory contains comprehensive examples of Delta Lake capabilities with PySpark.

## Examples Included

### 1. Delta Lake Basics (`delta_examples.py`)
- **ACID Transactions**: Create, read, update, delete operations
- **Data Versioning**: Automatic versioning of all changes
- **Concurrent Access**: Safe concurrent reads and writes

### 2. Time Travel
- **Version-based Time Travel**: Access specific versions of data
- **Timestamp-based Time Travel**: Access data at specific timestamps
- **Table History**: View complete change history
- **Table Restoration**: Restore to previous versions

### 3. Schema Evolution
- **Adding Columns**: Safely add new columns to existing tables
- **Data Type Changes**: Handle schema changes gracefully
- **Backward Compatibility**: Maintain compatibility with existing data

### 4. UPSERT Operations
- **MERGE Command**: Efficient insert/update operations
- **Conditional Logic**: Business rule-based merges
- **Bulk Operations**: Handle large-scale data changes

## Key Features Demonstrated

- **ACID Properties**: Atomicity, Consistency, Isolation, Durability
- **Schema Enforcement**: Automatic schema validation
- **Time Travel**: Access historical versions of data
- **Efficient Storage**: Optimized Parquet format with transaction logs
- **Concurrent Operations**: Safe multi-user access patterns

## Usage

### Local Development (✅ Recommended - Now Working!)
1. **Activate the local Delta Lake environment**:
   ```bash
   source ../../scripts/activate-env.sh delta-lake-local
   ```

2. **Run the examples**:
   ```bash
   python delta_examples.py
   ```

🎉 **All Delta Lake features working**: ACID transactions, time travel, schema evolution, and UPSERT operations!

### Remote Development (Databricks)
1. **Set up environment variables**:
   ```bash
   export DATABRICKS_HOST="your-databricks-workspace-url"
   export DATABRICKS_TOKEN="your-access-token"
   export DATABRICKS_CLUSTER_ID="your-cluster-id"  # optional
   ```

2. **Activate the remote Delta Lake environment**:
   ```bash
   source ../../scripts/activate-env.sh delta-lake
   ```

3. **Run the examples**:
   ```bash
   python delta_examples.py
   ```

## ✅ Status: Local Environment Now Working!

**Solution Applied**: Successfully resolved version compatibility by using compatible versions:
- **PySpark 3.4.1** with **Delta Lake 2.4.0** - fully compatible and tested
- **Java 11** instead of Java 17 for better stability
- All Delta Lake features now working in local environment

## Environment Comparison

| Feature | delta-lake-local | delta-lake |
|---------|------------------|------------|
| **Execution Mode** | Local Spark | Remote Databricks |
| **PySpark Version** | ✅ 3.4.1 | ✅ 3.5.0 |
| **Delta Lake** | ✅ 2.4.0 (Working!) | ✅ 3.0.0 |
| **Java Version** | ✅ 11 (stable) | ✅ 17 |
| **databricks-connect** | ❌ Not included | ✅ Included (13.3.2) |
| **Best for** | ✅ Local development & testing | ✅ Production, Databricks integration |
| **Requirements** | None | Databricks credentials |
| **Performance** | Local resources | Databricks cluster resources |

3. **Explore the generated data**: Check the `./data/delta/` directory

## Data Storage

Delta tables are stored in the following locations:
- `./data/delta/customers/` - Customer data with CRUD operations
- `./data/delta/products/` - Product schema evolution examples
- `./data/delta/inventory/` - Inventory UPSERT examples

## Advanced Operations

### Time Travel Queries
```python
# Read specific version
df = spark.read.format("delta").option("versionAsOf", 0).load(path)

# Read at timestamp
df = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load(path)
```

### MERGE Operations
```python
delta_table.merge(updates_df, "target.id = source.id") \
    .whenMatchedUpdate(set={...}) \
    .whenNotMatchedInsert(values={...}) \
    .execute()
```

### Schema Evolution
```python
df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
```

## Monitoring and Optimization

- **Transaction Logs**: Stored in `_delta_log/` directories
- **Optimization**: Use `OPTIMIZE` and `VACUUM` commands
- **Performance**: Monitor table statistics and file sizes
- **History Retention**: Configure data retention policies

## Troubleshooting

### Common Issues

**`ClassNotFoundException` or `NoClassDefFoundError`**
- These errors indicate version compatibility issues between PySpark and Delta Lake
- **Solution**: Use the remote `delta-lake` environment instead of `delta-lake-local`

**`DATA_SOURCE_NOT_FOUND: Failed to find the data source: delta`**
- Delta Lake extensions are not properly loaded
- **Solution**: Verify environment activation and use remote environment

**Maven dependency resolution failures**
- Network or version compatibility issues downloading JAR files
- **Solution**: Use pre-configured remote environment with proper dependencies

### Local Development Success!

✅ **Working Configuration**:
1. Use `source ../../scripts/activate-env.sh delta-lake-local` for local Delta Lake development
2. Compatible versions: PySpark 3.4.1 + Delta Lake 2.4.0 + Java 11
3. All Delta Lake features working: ACID transactions, time travel, schema evolution, UPSERTS

### Version Notes
- **Local**: PySpark 3.4.1 + Delta Lake 2.4.0 (stable, tested)
- **Remote**: PySpark 3.5.0 + Delta Lake 3.0.0 (latest features)