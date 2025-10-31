# PySpark Unity Catalog Examples

This directory contains comprehensive examples of Unity Catalog capabilities with PySpark for data governance, lineage, and cataloging.

## Examples Included

### 1. Unity Catalog Setup (`unity_catalog_examples.py`)
- **Catalog Management**: Create and manage catalogs and schemas
- **Managed Tables**: Tables managed by Unity Catalog
- **External Tables**: Tables pointing to external data sources
- **Table Properties**: Metadata and governance attributes

### 2. Data Governance
- **Column-Level Security**: Data masking and PII protection
- **Row-Level Security**: User and role-based data access
- **Data Classification**: Automatic classification of sensitive data
- **Access Control**: Fine-grained permissions and policies

### 3. Data Lineage
- **Pipeline Tracking**: End-to-end data transformation lineage
- **Impact Analysis**: Understanding downstream dependencies
- **Metadata Capture**: Automatic lineage metadata collection
- **Visualization**: Lineage graph representation

### 4. Data Discovery
- **Table Cataloging**: Automatic discovery of table metadata
- **Schema Analysis**: Column types and statistics
- **Data Quality**: Automated quality assessment
- **Search and Discovery**: Metadata-driven data discovery

## Key Features Demonstrated

- **Three-Level Namespace**: Catalog.Schema.Table hierarchy
- **Governance Policies**: Automated policy enforcement
- **Metadata Management**: Rich metadata and annotations
- **Data Classification**: Automatic sensitive data detection
- **Lineage Tracking**: Complete data transformation history
- **Quality Monitoring**: Continuous data quality assessment

## Usage

1. **Activate the Unity Catalog environment**:
   ```bash
   source ../../scripts/activate-env.sh unity-catalog
   ```

2. **Run the examples**:
   ```bash
   python unity_catalog_examples.py
   ```

3. **Explore generated metadata**: Check the `./data/metadata/` directory

## Local vs Databricks Mode

### Local Mode (Development)
- **Simulated Features**: Unity Catalog features are simulated
- **Local Storage**: Data stored in local file system
- **Basic Governance**: Simplified governance examples
- **Metadata Files**: Governance metadata stored as JSON files

### Databricks Mode (Production)
To use with real Unity Catalog in Databricks:

1. **Set environment variables**:
   ```bash
   export DATABRICKS_HOST="https://your-workspace.databricks.com"
   export DATABRICKS_TOKEN="your-access-token"
   ```

2. **Configure connection**:
   ```python
   spark.conf.set("spark.databricks.service.address", databricks_host)
   spark.conf.set("spark.databricks.service.token", databricks_token)
   ```

## Generated Artifacts

### Metadata Files
- `./data/metadata/data_classification.json` - Data classification tags
- `./data/metadata/table_catalog.json` - Complete table catalog
- `./data/metadata/data_quality_report.json` - Data quality assessment

### Lineage Files
- `./data/lineage/customer_analytics_lineage.json` - Pipeline lineage tracking

### Data Files
- `./data/warehouse/` - Managed table storage
- `./data/warehouse/external/` - External table data

## Advanced Features

### Data Classification
```python
# Automatic PII detection and classification
classification_rules = {
    "email": "pii",
    "customer_id": "identifier",
    "age": "demographic"
}
```

### Row-Level Security
```python
# Role-based data filtering
filtered_df = df.filter(col("region").isin(user_allowed_regions))
```

### Column Masking
```python
# Automatic data masking
masked_df = df.select(
    regexp_replace(col("email"), "@.*", "@*****.com").alias("email_masked")
)
```

### Lineage Tracking
```python
# Automatic lineage capture
lineage_info = {
    "input_tables": ["catalog.schema.source_table"],
    "transformations": ["join", "aggregate", "filter"],
    "output_table": "catalog.schema.target_table"
}
```

## Integration Points

- **Delta Lake**: Full integration with Delta Lake features
- **MLflow**: Model lineage and experiment tracking
- **Databricks**: Native Databricks Unity Catalog support
- **External Systems**: Integration with external data catalogs

## Monitoring and Compliance

- **Access Auditing**: Track all data access patterns
- **Compliance Reporting**: Generate compliance reports
- **Data Quality Metrics**: Continuous quality monitoring
- **Governance Dashboard**: Visual governance insights