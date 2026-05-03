# Databricks notebook source
# MAGIC %md
# MAGIC # Data Exploration - Business Analytics Platform
# MAGIC 
# MAGIC This notebook provides exploratory data analysis on the business datasets.
# MAGIC 
# MAGIC **Objectives:**
# MAGIC - Understand data distributions
# MAGIC - Identify data quality issues
# MAGIC - Generate summary statistics
# MAGIC - Visualize key patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import seaborn as sns

# Configure display options
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 20)

print("PySpark version:", spark.version)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Sample Data

# COMMAND ----------

# Read from Bronze layer (or generate sample data)
try:
    sales_df = spark.read.format("delta").load("/dbfs/FileStore/business-analytics/bronze/sales")
    customer_df = spark.read.format("delta").load("/dbfs/FileStore/business-analytics/bronze/customers")
    product_df = spark.read.format("delta").load("/dbfs/FileStore/business-analytics/bronze/products")
    
    print("✓ Data loaded from Delta tables")
except:
    print("⚠ Delta tables not found. Run ETL pipeline notebook first.")
    dbutils.notebook.exit("Run 02_etl_pipeline notebook first")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Overview

# COMMAND ----------

print("=" * 60)
print("DATASET SUMMARY")
print("=" * 60)

datasets = {
    "Sales Transactions": sales_df,
    "Customers": customer_df,
    "Products": product_df
}

for name, df in datasets.items():
    record_count = df.count()
    column_count = len(df.columns)
    print(f"\n{name}:")
    print(f"  Records: {record_count:,}")
    print(f"  Columns: {column_count}")
    print(f"  Schema:")
    for field in df.schema.fields[:5]:  # Show first 5 columns
        print(f"    - {field.name}: {field.dataType}")
    if len(df.schema.fields) > 5:
        print(f"    ... and {len(df.schema.fields) - 5} more")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales Data Analysis

# COMMAND ----------

# Display sample records
display(sales_df.limit(10))

# COMMAND ----------

# Summary statistics for numerical columns
sales_df.select(
    "quantity",
    "unit_price",
    "discount_percentage",
    "total_amount"
).summary().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales by Region

# COMMAND ----------

sales_by_region = sales_df.groupBy("region").agg(
    F.sum("total_amount").alias("total_revenue"),
    F.count("transaction_id").alias("transaction_count"),
    F.avg("total_amount").alias("avg_transaction_value")
).orderBy(F.desc("total_revenue"))

display(sales_by_region)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales by Channel

# COMMAND ----------

sales_by_channel = sales_df.groupBy("sales_channel").agg(
    F.sum("total_amount").alias("total_revenue"),
    F.count("transaction_id").alias("transaction_count")
).orderBy(F.desc("total_revenue"))

display(sales_by_channel)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daily Sales Trend

# COMMAND ----------

daily_sales = sales_df.withColumn(
    "sale_date",
    F.to_date("transaction_date")
).groupBy("sale_date").agg(
    F.sum("total_amount").alias("daily_revenue"),
    F.count("transaction_id").alias("transaction_count")
).orderBy("sale_date")

display(daily_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Analysis

# COMMAND ----------

# Customer distribution by region
customer_by_region = customer_df.groupBy("region").agg(
    F.count("customer_id").alias("customer_count")
).orderBy(F.desc("customer_count"))

display(customer_by_region)

# COMMAND ----------

# Active vs Inactive customers
customer_status = customer_df.groupBy("is_active").agg(
    F.count("customer_id").alias("count")
)

display(customer_status)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Analysis

# COMMAND ----------

# Products by category
product_by_category = product_df.groupBy("category").agg(
    F.count("product_id").alias("product_count"),
    F.avg("unit_price").alias("avg_price")
).orderBy(F.desc("product_count"))

display(product_by_category)

# COMMAND ----------

# Price distribution
product_df.select(
    F.min("unit_price").alias("min_price"),
    F.max("unit_price").alias("max_price"),
    F.avg("unit_price").alias("avg_price"),
    F.percentile_approx("unit_price", 0.5).alias("median_price")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# Check for null values in sales data
null_counts = sales_df.select([
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in sales_df.columns
]).collect()[0].asDict()

print("Null Value Counts:")
for col, count in null_counts.items():
    if count > 0:
        pct = (count / sales_df.count()) * 100
        print(f"  {col}: {count} ({pct:.2f}%)")

# COMMAND ----------

# Check for duplicates
total_records = sales_df.count()
distinct_records = sales_df.dropDuplicates(["transaction_id"]).count()
duplicates = total_records - distinct_records

print(f"Total Records: {total_records:,}")
print(f"Distinct Records: {distinct_records:,}")
print(f"Duplicates: {duplicates:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Customers by Revenue

# COMMAND ----------

top_customers = sales_df.groupBy("customer_id").agg(
    F.sum("total_amount").alias("total_spent"),
    F.count("transaction_id").alias("purchase_count")
).orderBy(F.desc("total_spent")).limit(20)

# Join with customer data for names
top_customers_with_info = top_customers.join(
    customer_df.select("customer_id", "first_name", "last_name", "region"),
    "customer_id",
    "left"
)

display(top_customers_with_info)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Products by Revenue

# COMMAND ----------

top_products = sales_df.groupBy("product_id").agg(
    F.sum("total_amount").alias("total_revenue"),
    F.sum("quantity").alias("total_quantity_sold")
).orderBy(F.desc("total_revenue")).limit(20)

# Join with product data
top_products_with_info = top_products.join(
    product_df.select("product_id", "product_name", "category"),
    "product_id",
    "left"
)

display(top_products_with_info)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Insights Summary

# COMMAND ----------

# Calculate key metrics
total_revenue = sales_df.agg(F.sum("total_amount")).collect()[0][0]
total_transactions = sales_df.count()
unique_customers = sales_df.select("customer_id").distinct().count()
avg_transaction = total_revenue / total_transactions
revenue_per_customer = total_revenue / unique_customers

print("=" * 60)
print("KEY BUSINESS METRICS")
print("=" * 60)
print(f"\nTotal Revenue: ${total_revenue:,.2f}")
print(f"Total Transactions: {total_transactions:,}")
print(f"Unique Customers: {unique_customers:,}")
print(f"Average Transaction Value: ${avg_transaction:.2f}")
print(f"Revenue per Customer: ${revenue_per_customer:.2f}")
print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Based on this exploration, proceed to:
# MAGIC 1. **ETL Pipeline** (notebook 02) - Process and transform the data
# MAGIC 2. **Analytics Dashboard** (notebook 03) - Build visualizations and insights
