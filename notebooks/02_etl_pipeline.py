# Databricks notebook source
# MAGIC %md
# MAGIC # Business Analytics ETL Pipeline
# MAGIC 
# MAGIC This notebook demonstrates a production-ready ETL pipeline for business analytics.
# MAGIC 
# MAGIC **Pipeline Flow:**
# MAGIC 1. Data Ingestion (Bronze Layer)
# MAGIC 2. Data Validation & Cleaning (Silver Layer)
# MAGIC 3. Business Transformations (Gold Layer)
# MAGIC 4. Analytics & Reporting
# MAGIC 
# MAGIC **Author:** Your Name  
# MAGIC **Date:** 2026-05-02

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# Import required libraries
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import yaml
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("PySpark version:", spark.version)
print("Databricks Runtime:", spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Pipeline configuration
config = {
    'paths': {
        'bronze': '/dbfs/FileStore/business-analytics/bronze/',
        'silver': '/dbfs/FileStore/business-analytics/silver/',
        'gold': '/dbfs/FileStore/business-analytics/gold/',
        'sample_data': '/dbfs/FileStore/business-analytics/sample/'
    },
    'spark': {
        'shuffle_partitions': 200,
        'broadcast_threshold': 10485760  # 10MB
    },
    'business_rules': {
        'high_value_threshold': 10000,
        'churn_risk_days': 180,
        'low_stock_threshold': 50
    }
}

# Apply Spark configurations
spark.conf.set("spark.sql.shuffle.partitions", config['spark']['shuffle_partitions'])
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", config['spark']['broadcast_threshold'])

print("Configuration loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Sample Data
# MAGIC 
# MAGIC For demonstration, we'll generate synthetic data that simulates a real business scenario.

# COMMAND ----------

from datetime import datetime, timedelta
import random

# Set seed for reproducibility
random.seed(42)

def generate_sample_sales_data(num_records=10000):
    """Generate synthetic sales transaction data"""
    
    # Date range: last 2 years
    end_date = datetime.now()
    start_date = end_date - timedelta(days=730)
    
    # Generate date range
    date_range = [start_date + timedelta(days=x) for x in range((end_date - start_date).days)]
    
    sales_data = []
    
    for i in range(num_records):
        transaction_id = f"TXN{str(i).zfill(8)}"
        transaction_date = random.choice(date_range)
        customer_id = f"CUST{random.randint(1, 2000):04d}"
        product_id = f"PROD{random.randint(1, 500):04d}"
        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(10, 500), 2)
        discount_pct = random.choice([0, 0.05, 0.10, 0.15, 0.20])
        total_amount = round(quantity * unit_price * (1 - discount_pct), 2)
        payment_method = random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Cash'])
        sales_channel = random.choice(['Online', 'Store', 'Mobile App'])
        region = random.choice(['North', 'South', 'East', 'West', 'Central'])
        store_id = f"STORE{random.randint(1, 50):03d}"
        
        sales_data.append((
            transaction_id, transaction_date, customer_id, product_id,
            quantity, unit_price, discount_pct, total_amount,
            payment_method, sales_channel, region, store_id
        ))
    
    # Create DataFrame
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("transaction_date", TimestampType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("discount_percentage", DoubleType(), True),
        StructField("total_amount", DoubleType(), False),
        StructField("payment_method", StringType(), True),
        StructField("sales_channel", StringType(), True),
        StructField("region", StringType(), True),
        StructField("store_id", StringType(), True)
    ])
    
    return spark.createDataFrame(sales_data, schema)

def generate_customer_data(num_customers=2000):
    """Generate synthetic customer data"""
    
    customers = []
    
    for i in range(1, num_customers + 1):
        customer_id = f"CUST{i:04d}"
        first_name = f"FirstName{i}"
        last_name = f"LastName{i}"
        email = f"customer{i}@example.com"
        phone = f"+1-555-{random.randint(1000, 9999)}"
        registration_date = datetime.now() - timedelta(days=random.randint(1, 1000))
        country = "USA"
        region = random.choice(['North', 'South', 'East', 'West', 'Central'])
        city = f"City{random.randint(1, 100)}"
        segment = random.choice(['Retail', 'Enterprise', 'SMB'])
        is_active = random.choice(['Y', 'Y', 'Y', 'N'])  # 75% active
        
        customers.append((
            customer_id, first_name, last_name, email, phone,
            registration_date, country, region, city, segment, is_active
        ))
    
    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("registration_date", DateType(), False),
        StructField("country", StringType(), True),
        StructField("region", StringType(), True),
        StructField("city", StringType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("is_active", StringType(), True)
    ])
    
    return spark.createDataFrame(customers, schema)

def generate_product_data(num_products=500):
    """Generate synthetic product catalog data"""
    
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books']
    products = []
    
    for i in range(1, num_products + 1):
        product_id = f"PROD{i:04d}"
        product_name = f"Product {i}"
        category = random.choice(categories)
        subcategory = f"{category}_Sub{random.randint(1, 5)}"
        brand = f"Brand{random.randint(1, 20)}"
        unit_cost = round(random.uniform(5, 300), 2)
        unit_price = round(unit_cost * random.uniform(1.3, 2.5), 2)
        launch_date = datetime.now() - timedelta(days=random.randint(1, 1500))
        is_active = 'Y'
        
        products.append((
            product_id, product_name, category, subcategory, brand,
            unit_cost, unit_price, launch_date, is_active
        ))
    
    schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("unit_cost", DoubleType(), True),
        StructField("unit_price", DoubleType(), False),
        StructField("launch_date", DateType(), True),
        StructField("is_active", StringType(), True)
    ])
    
    return spark.createDataFrame(products, schema)

# Generate sample datasets
print("Generating sample data...")
sales_df = generate_sample_sales_data(10000)
customer_df = generate_customer_data(2000)
product_df = generate_product_data(500)

print(f"Generated {sales_df.count()} sales transactions")
print(f"Generated {customer_df.count()} customers")
print(f"Generated {product_df.count()} products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer: Data Ingestion
# MAGIC 
# MAGIC Load raw data into Bronze layer (landing zone).

# COMMAND ----------

# Write to Bronze layer as Delta tables
print("Writing to Bronze layer...")

sales_df.write.format("delta").mode("overwrite").save(f"{config['paths']['bronze']}sales")
customer_df.write.format("delta").mode("overwrite").save(f"{config['paths']['bronze']}customers")
product_df.write.format("delta").mode("overwrite").save(f"{config['paths']['bronze']}products")

print("Bronze layer data written successfully")

# Verify data
print("\nBronze Layer Statistics:")
print(f"Sales: {spark.read.format('delta').load(f\"{config['paths']['bronze']}sales\").count()} records")
print(f"Customers: {spark.read.format('delta').load(f\"{config['paths']['bronze']}customers\").count()} records")
print(f"Products: {spark.read.format('delta').load(f\"{config['paths']['bronze']}products\").count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Data Validation & Cleaning
# MAGIC 
# MAGIC Apply data quality checks and cleansing transformations.

# COMMAND ----------

# Read from Bronze
bronze_sales = spark.read.format("delta").load(f"{config['paths']['bronze']}sales")
bronze_customers = spark.read.format("delta").load(f"{config['paths']['bronze']}customers")
bronze_products = spark.read.format("delta").load(f"{config['paths']['bronze']}products")

# Data Quality Checks
def validate_and_clean_sales(df):
    """Apply validation and cleaning rules to sales data"""
    
    # Remove duplicates
    df = df.dropDuplicates(["transaction_id"])
    
    # Remove records with null critical fields
    df = df.filter(
        F.col("transaction_id").isNotNull() &
        F.col("customer_id").isNotNull() &
        F.col("product_id").isNotNull() &
        F.col("total_amount").isNotNull()
    )
    
    # Data quality flags
    df = df.withColumn(
        "is_valid_amount",
        (F.col("total_amount") > 0) & (F.col("total_amount") < 100000)
    ).withColumn(
        "is_valid_quantity",
        (F.col("quantity") > 0) & (F.col("quantity") <= 1000)
    )
    
    # Filter only valid records
    df_clean = df.filter(
        F.col("is_valid_amount") & F.col("is_valid_quantity")
    )
    
    return df_clean

# Apply cleaning
silver_sales = validate_and_clean_sales(bronze_sales)

print(f"Bronze sales: {bronze_sales.count()}")
print(f"Silver sales (cleaned): {silver_sales.count()}")
print(f"Records filtered: {bronze_sales.count() - silver_sales.count()}")

# COMMAND ----------

# Write to Silver layer
silver_sales.write.format("delta").mode("overwrite").save(f"{config['paths']['silver']}sales")
bronze_customers.write.format("delta").mode("overwrite").save(f"{config['paths']['silver']}customers")
bronze_products.write.format("delta").mode("overwrite").save(f"{config['paths']['silver']}products")

print("Silver layer data written successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Business Transformations
# MAGIC 
# MAGIC Apply business logic to create analytics-ready datasets.

# COMMAND ----------

# Read from Silver
silver_sales = spark.read.format("delta").load(f"{config['paths']['silver']}sales")
silver_customers = spark.read.format("delta").load(f"{config['paths']['silver']}customers")
silver_products = spark.read.format("delta").load(f"{config['paths']['silver']}products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer Analytics

# COMMAND ----------

# Calculate Customer Lifetime Value (CLV)
customer_metrics = silver_sales.groupBy("customer_id").agg(
    F.sum("total_amount").alias("total_revenue"),
    F.count("transaction_id").alias("total_transactions"),
    F.avg("total_amount").alias("avg_transaction_value"),
    F.min("transaction_date").alias("first_purchase_date"),
    F.max("transaction_date").alias("last_purchase_date")
)

# Calculate customer age and purchase frequency
customer_clv = customer_metrics \
    .withColumn(
        "customer_age_months",
        F.months_between(F.col("last_purchase_date"), F.col("first_purchase_date"))
    ) \
    .withColumn(
        "purchase_frequency",
        F.when(F.col("customer_age_months") > 0,
               F.col("total_transactions") / F.col("customer_age_months"))
        .otherwise(F.col("total_transactions"))
    ) \
    .withColumn(
        "projected_12m_clv",
        F.col("avg_transaction_value") * F.col("purchase_frequency") * 12
    )

# Join with customer master data
gold_customer_analytics = silver_customers.join(
    customer_clv,
    "customer_id",
    "left"
)

display(gold_customer_analytics.orderBy(F.desc("projected_12m_clv")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Performance Analysis

# COMMAND ----------

# Product sales aggregation
product_sales = silver_sales.groupBy("product_id").agg(
    F.sum("total_amount").alias("total_revenue"),
    F.sum("quantity").alias("total_quantity_sold"),
    F.count("transaction_id").alias("transaction_count"),
    F.countDistinct("customer_id").alias("unique_buyers")
)

# Join with product data and calculate profit
gold_product_performance = silver_products.join(
    product_sales,
    "product_id",
    "left"
) \
.withColumn(
    "total_cost",
    F.col("unit_cost") * F.col("total_quantity_sold")
) \
.withColumn(
    "gross_profit",
    F.col("total_revenue") - F.col("total_cost")
) \
.withColumn(
    "profit_margin_pct",
    (F.col("gross_profit") / F.col("total_revenue") * 100)
)

# Rank products
window_spec = Window.orderBy(F.desc("total_revenue"))
gold_product_performance = gold_product_performance.withColumn(
    "revenue_rank",
    F.row_number().over(window_spec)
)

display(gold_product_performance.orderBy("revenue_rank").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monthly Sales Trends

# COMMAND ----------

# Calculate monthly aggregations
monthly_sales = silver_sales \
    .withColumn("year", F.year("transaction_date")) \
    .withColumn("month", F.month("transaction_date")) \
    .withColumn("year_month", F.date_trunc("month", "transaction_date")) \
    .groupBy("year", "month", "year_month", "region") \
    .agg(
        F.sum("total_amount").alias("monthly_revenue"),
        F.count("transaction_id").alias("transaction_count"),
        F.countDistinct("customer_id").alias("unique_customers")
    )

# Calculate month-over-month growth
window_mom = Window.partitionBy("region").orderBy("year", "month")

gold_monthly_trends = monthly_sales \
    .withColumn(
        "prev_month_revenue",
        F.lag("monthly_revenue", 1).over(window_mom)
    ) \
    .withColumn(
        "mom_growth_pct",
        ((F.col("monthly_revenue") - F.col("prev_month_revenue")) / 
         F.col("prev_month_revenue") * 100)
    )

display(gold_monthly_trends.orderBy(F.desc("year_month")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Regional Performance

# COMMAND ----------

gold_regional_performance = silver_sales.groupBy("region").agg(
    F.sum("total_amount").alias("total_revenue"),
    F.count("transaction_id").alias("transaction_count"),
    F.countDistinct("customer_id").alias("unique_customers"),
    F.avg("total_amount").alias("avg_order_value")
) \
.withColumn(
    "revenue_per_customer",
    F.col("total_revenue") / F.col("unique_customers")
)

display(gold_regional_performance.orderBy(F.desc("total_revenue")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Gold Layer

# COMMAND ----------

# Write analytics outputs to Gold layer
gold_customer_analytics.write.format("delta").mode("overwrite") \
    .save(f"{config['paths']['gold']}customer_analytics")

gold_product_performance.write.format("delta").mode("overwrite") \
    .save(f"{config['paths']['gold']}product_performance")

gold_monthly_trends.write.format("delta").mode("overwrite") \
    .save(f"{config['paths']['gold']}monthly_trends")

gold_regional_performance.write.format("delta").mode("overwrite") \
    .save(f"{config['paths']['gold']}regional_performance")

print("Gold layer analytics written successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Optimization

# COMMAND ----------

# Enable caching for frequently accessed data
silver_sales.cache()
print(f"Cached {silver_sales.count()} sales records")

# Show execution plan (for optimization analysis)
silver_sales.groupBy("region").agg(F.sum("total_amount")).explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

print("=" * 60)
print("PIPELINE EXECUTION SUMMARY")
print("=" * 60)
print(f"\nBronze Layer:")
print(f"  Sales Transactions: {spark.read.format('delta').load(f\"{config['paths']['bronze']}sales\").count():,}")
print(f"  Customers: {spark.read.format('delta').load(f\"{config['paths']['bronze']}customers\").count():,}")
print(f"  Products: {spark.read.format('delta').load(f\"{config['paths']['bronze']}products\").count():,}")

print(f"\nSilver Layer (Cleaned):")
print(f"  Sales Transactions: {spark.read.format('delta').load(f\"{config['paths']['silver']}sales\").count():,}")

print(f"\nGold Layer (Analytics):")
print(f"  Customer Analytics: {spark.read.format('delta').load(f\"{config['paths']['gold']}customer_analytics\").count():,}")
print(f"  Product Performance: {spark.read.format('delta').load(f\"{config['paths']['gold']}product_performance\").count():,}")

print(f"\n{'=' * 60}")
print("Pipeline completed successfully!")
print("=" * 60)

# COMMAND ----------


