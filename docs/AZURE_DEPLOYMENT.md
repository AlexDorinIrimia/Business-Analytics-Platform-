# Azure Deployment Guide

## Architecture

```
Azure Data Lake Storage Gen2 (Bronze/Silver/Gold layers)
    ↓
Azure Databricks (Compute & Processing)
    ↓
Power BI (Dashboards & Reports)
```

## Prerequisites

- Azure subscription with appropriate permissions
- Azure CLI installed locally
- Databricks CLI installed
- Service Principal for authentication

---

## Deployment Steps

### Step 1: Create Resource Group

```bash
# Login to Azure
az login

# Create resource group
az group create \
  --name rg-business-analytics-prod \
  --location eastus

# Verify creation
az group show --name rg-business-analytics-prod
```

---

### Step 2: Deploy Azure Data Lake Storage Gen2

```bash
# Create storage account
az storage account create \
  --name sadataplatform001 \
  --resource-group rg-business-analytics-prod \
  --location eastus \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true

# Create containers for Bronze/Silver/Gold
az storage container create \
  --name bronze \
  --account-name sadataplatform001

az storage container create \
  --name silver \
  --account-name sadataplatform001

az storage container create \
  --name gold \
  --account-name sadataplatform001
```

---

### Step 3: Deploy Azure Databricks

```bash
# Create Databricks workspace
az databricks workspace create \
  --resource-group rg-business-analytics-prod \
  --name dbw-business-analytics \
  --location eastus \
  --sku premium

# Get workspace URL
az databricks workspace show \
  --name dbw-business-analytics \
  --resource-group rg-business-analytics-prod \
  --query workspaceUrl
```

---

### Step 4: Configure Service Principal

```bash
# Create service principal
az ad sp create-for-rbac \
  --name sp-databricks-data-access \
  --role "Storage Blob Data Contributor" \
  --scopes /subscriptions/{subscription-id}/resourceGroups/rg-business-analytics-prod

# Save the output (needed for Databricks configuration):
# {
#   "appId": "xxx",
#   "password": "xxx",
#   "tenant": "xxx"
# }
```

---

### Step 5: Mount ADLS to Databricks

Create a Databricks notebook with this code:

```python
# Configure OAuth for ADLS Gen2 access
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "<service-principal-app-id>",
  "fs.azure.account.oauth2.client.secret": "<service-principal-password>",
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<tenant-id>/oauth2/token"
}

# Mount Bronze layer
dbutils.fs.mount(
  source = "abfss://bronze@sadataplatform001.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs
)

# Mount Silver layer
dbutils.fs.mount(
  source = "abfss://silver@sadataplatform001.dfs.core.windows.net/",
  mount_point = "/mnt/silver",
  extra_configs = configs
)

# Mount Gold layer
dbutils.fs.mount(
  source = "abfss://gold@sadataplatform001.dfs.core.windows.net/",
  mount_point = "/mnt/gold",
  extra_configs = configs
)

# Verify mounts
display(dbutils.fs.ls("/mnt/bronze"))
```

**Best Practice:** Store credentials in Azure Key Vault:

```python
# Reference secrets from Key Vault
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="kv-secrets", key="sp-client-id"),
  "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="kv-secrets", key="sp-client-secret"),
  "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='kv-secrets', key='tenant-id')}/oauth2/token"
}
```

---

### Step 6: Configure Databricks Cluster

**Cluster Configuration (cluster-config.json):**

```json
{
  "cluster_name": "analytics-cluster",
  "spark_version": "14.3.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "driver_node_type_id": "Standard_DS3_v2",
  "autoscale": {
    "min_workers": 2,
    "max_workers": 8
  },
  "autotermination_minutes": 30,
  "spark_conf": {
    "spark.sql.adaptive.enabled": "true",
    "spark.databricks.delta.preview.enabled": "true",
    "spark.sql.shuffle.partitions": "200"
  },
  "azure_attributes": {
    "availability": "ON_DEMAND_AZURE",
    "first_on_demand": 1,
    "spot_bid_max_price": -1
  },
  "enable_elastic_disk": true
}
```

**Create cluster via CLI:**

```bash
databricks clusters create --json-file cluster-config.json
```

---

### Step 7: Upload Code and Notebooks

```bash
# Configure Databricks CLI
databricks configure --token

# Upload notebooks
databricks workspace import_dir \
  ./notebooks \
  /Workspace/business-analytics/notebooks \
  --overwrite

# Upload libraries
databricks fs cp ./requirements.txt dbfs:/FileStore/business-analytics/requirements.txt
```

---

### Step 8: Create Databricks Workflows

**Workflow Definition (workflow-etl-pipeline.json):**

```json
{
  "name": "Business Analytics ETL Pipeline",
  "tasks": [
    {
      "task_key": "bronze_ingestion",
      "description": "Ingest raw data to Bronze layer",
      "notebook_task": {
        "notebook_path": "/Workspace/business-analytics/notebooks/01_data_exploration",
        "base_parameters": {}
      },
      "job_cluster_key": "analytics_cluster"
    },
    {
      "task_key": "etl_pipeline",
      "description": "Process Bronze to Silver to Gold",
      "depends_on": [{"task_key": "bronze_ingestion"}],
      "notebook_task": {
        "notebook_path": "/Workspace/business-analytics/notebooks/02_etl_pipeline",
        "base_parameters": {}
      },
      "job_cluster_key": "analytics_cluster"
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "analytics_cluster",
      "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "Standard_DS3_v2",
        "num_workers": 2,
        "autoscale": {
          "min_workers": 2,
          "max_workers": 8
        }
      }
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 6 * * ?",
    "timezone_id": "America/New_York",
    "pause_status": "UNPAUSED"
  },
  "email_notifications": {
    "on_failure": ["data-team@company.com"]
  },
  "max_concurrent_runs": 1
}
```

**Create workflow:**

```bash
databricks jobs create --json-file workflow-etl-pipeline.json
```

---

### Step 9: Configure Databricks SQL Endpoint

```bash
# Create SQL endpoint for Power BI connectivity
databricks sql endpoints create \
  --name "business-analytics-sql" \
  --cluster-size "Small" \
  --min-num-clusters 1 \
  --max-num-clusters 3 \
  --auto-stop-mins 30
```

---

### Step 10: Connect Power BI

1. **In Power BI Desktop:**
   - Get Data → Azure → Azure Databricks
   - Server hostname: `<your-workspace-url>`
   - HTTP path: Get from SQL Endpoint settings
   - Authentication: Azure Active Directory

2. **Import Gold layer tables:**
   ```sql
   SELECT * FROM gold.customer_analytics
   SELECT * FROM gold.product_performance
   SELECT * FROM gold.monthly_revenue_trends
   ```

3. **Schedule refresh:**
   - Publish to Power BI Service
   - Configure scheduled refresh (daily at 7 AM)

---

## Cost Optimization

### Compute Optimization

- Use cluster pools for faster startup
- Scale to zero when idle (min_workers = 0)
- Terminate clusters after 15-30 minutes of inactivity
- Use spot instances for 70% cost savings

### Storage Optimization

- **Bronze layer:** Move to Cool tier after 90 days
- **Silver layer:** Keep in Hot tier
- **Gold layer:** Keep in Hot tier (frequently accessed)

### Estimated Monthly Cost

- **Databricks:** $500-1500 (2-8 workers, 8 hours/day)
- **ADLS Gen2:** $50-200 (1TB storage)
- **Power BI Pro:** $10/user/month
- **Total:** ~$600-1800/month

---

## Security Configuration

### Network Security

```bash
# Create Virtual Network
az network vnet create \
  --resource-group rg-business-analytics-prod \
  --name vnet-databricks \
  --address-prefix 10.0.0.0/16

# Create subnets for Databricks
az network vnet subnet create \
  --resource-group rg-business-analytics-prod \
  --vnet-name vnet-databricks \
  --name snet-databricks-public \
  --address-prefix 10.0.1.0/24
```

### Data Encryption

- **At Rest:** Azure Storage Service Encryption (enabled by default)
- **In Transit:** TLS 1.2+ for all connections
- **Key Management:** Azure Key Vault for secrets

### Access Control

```python
# Row-level security example
spark.sql("""
  CREATE OR REPLACE VIEW sales_filtered AS
  SELECT * FROM sales
  WHERE region = current_user_region()
""")

# Column masking for PII
spark.sql("""
  CREATE OR REPLACE VIEW customers_masked AS
  SELECT 
    customer_id,
    CASE 
      WHEN is_member('pii_access') THEN email
      ELSE CONCAT('***@', SPLIT(email, '@')[1])
    END as email,
    first_name,
    last_name
  FROM customers
""")
```

---

## Monitoring & Alerts

### Key Metrics to Monitor

- **Job success rate:** Target >99%
- **Data quality score:** Target >95%
- **Query performance:** P95 latency <5 seconds
- **Cost per processed GB:** Track trend
- **Cluster utilization:** Target 70-85%

---

## Deployment Checklist

- [ ] Azure resource group created
- [ ] ADLS Gen2 deployed with containers
- [ ] Databricks workspace provisioned
- [ ] Service principal configured
- [ ] Storage mounted to Databricks
- [ ] Cluster created and configured
- [ ] Code and notebooks uploaded
- [ ] Workflows scheduled
- [ ] SQL endpoint created
- [ ] Power BI connected
- [ ] Monitoring configured
- [ ] Security policies applied