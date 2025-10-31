#!/bin/bash

# Check status of all conda environments
echo "🔍 Checking PySpark environment status..."

# Check if conda is available
if ! command -v conda &> /dev/null; then
    echo "❌ Conda is not available."
    exit 1
fi

echo "📦 Conda version:"
conda --version
echo ""

echo "🌍 Available conda environments:"
conda env list
echo ""

# Check each required environment
environments=("base-pyspark" "streaming" "delta-lake" "unity-catalog")
echo "🔧 PySpark environment status:"

for env in "${environments[@]}"; do
    echo -n "   $env: "
    if conda env list | grep -q "$env"; then
        echo "✅ EXISTS"
        
        # Check Python and key packages
        source "$(conda info --base)/etc/profile.d/conda.sh"
        conda activate "$env" 2>/dev/null
        
        if [[ $? -eq 0 ]]; then
            python_version=$(python --version 2>&1)
            spark_version=$(python -c "import pyspark; print(pyspark.__version__)" 2>/dev/null || echo "Not installed")
            echo "      Python: $python_version"
            echo "      PySpark: $spark_version"
            
            # Check environment-specific packages
            case $env in
                "streaming")
                    kafka_version=$(python -c "import kafka; print(kafka.__version__)" 2>/dev/null || echo "Not installed")
                    echo "      Kafka: $kafka_version"
                    ;;
                "delta-lake")
                    delta_version=$(python -c "import delta; print('Installed')" 2>/dev/null || echo "Not installed")
                    echo "      Delta Lake: $delta_version"
                    ;;
                "unity-catalog")
                    databricks_version=$(python -c "import databricks; print('Installed')" 2>/dev/null || echo "Not installed")
                    echo "      Databricks SDK: $databricks_version"
                    ;;
            esac
        else
            echo "      ❌ Failed to activate environment"
        fi
        echo ""
    else
        echo "❌ MISSING"
    fi
done

# Check Java (required for Spark)
echo "☕ Java status:"
if command -v java &> /dev/null; then
    java_version=$(java -version 2>&1 | head -n 1)
    echo "   $java_version"
    
    if java -version 2>&1 | grep -q "11\|17\|21"; then
        echo "   ✅ Compatible Java version"
    else
        echo "   ⚠️  May not be compatible with Spark (recommended: Java 11, 17, or 21)"
    fi
else
    echo "   ❌ Java not found"
fi
echo ""

# Check port availability
echo "🌐 Port availability:"
ports=(4040 8080 8888 9092)
for port in "${ports[@]}"; do
    if lsof -i :$port &> /dev/null; then
        echo "   Port $port: ❌ IN USE"
    else
        echo "   Port $port: ✅ AVAILABLE"
    fi
done
echo ""

# Check disk space
echo "💾 Disk space:"
df -h . | tail -1 | awk '{print "   Available: " $4 " (" $5 " used)"}'
echo ""

# Summary
echo "📋 Summary:"
missing_envs=0
for env in "${environments[@]}"; do
    if ! conda env list | grep -q "$env"; then
        ((missing_envs++))
    fi
done

if [[ $missing_envs -eq 0 ]]; then
    echo "   ✅ All PySpark environments are ready!"
    echo "   🚀 Run './scripts/run-all-examples.sh' to test everything"
else
    echo "   ❌ $missing_envs environment(s) missing"
    echo "   🔧 Run './scripts/setup-environments.sh' to create them"
fi