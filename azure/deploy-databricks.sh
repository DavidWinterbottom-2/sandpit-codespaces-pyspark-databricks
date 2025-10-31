# Deploy Databricks Workspace using ARM Template

# Create resource group
az group create --name "rg-databricks-pyspark" --location "East US"

# Deploy the template
az deployment group create \
  --resource-group "rg-databricks-pyspark" \
  --template-file "databricks-workspace-template.json" \
  --parameters workspaceName="pyspark-databricks-workspace" \
               location="East US" \
               pricingTier="standard"

# Get the workspace URL
az deployment group show \
  --resource-group "rg-databricks-pyspark" \
  --name "databricks-workspace-template" \
  --query "properties.outputs.workspaceUrl.value" -o tsv