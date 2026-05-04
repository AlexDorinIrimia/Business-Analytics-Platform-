# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Analytics - Business Intelligence Queries
# MAGIC 
# MAGIC This notebook demonstrates SQL-based analytics using Spark SQL.
# MAGIC Designed for integration with Power BI, Tableau, and other BI tools.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup - Load Data and Create Views

# COMMAND ----------

from pyspark.sql import functions as F

# Load data from Gold layer
sales_df = spark.read.format("delta").load("/dbfs/FileStore/business-analytics/gold/sales")
customers_df = spark.read.format("delta").load("/dbfs/FileStore/business-analytics/gold/customer_analytics")
products_df = spark.read.format("delta").load("/dbfs/FileStore/business-analytics/gold/product_performance")

# Create SQL views
sales_df.createOrReplaceTempView("sales")
customers_df.createOrReplaceTempView("customers")
products_df.createOrReplaceTempView("products")

print("✓ SQL views created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executive Dashboard Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- KPI Summary: Current Month Performance
# MAGIC SELECT 
# MAGIC   COUNT(DISTINCT transaction_id) as total_transactions,
# MAGIC   COUNT(DISTINCT customer_id) as unique_customers,
# MAGIC   SUM(total_amount) as total_revenue,
# MAGIC   AVG(total_amount) as avg_order_value,
# MAGIC   SUM(total_amount) / COUNT(DISTINCT customer_id) as revenue_per_customer
# MAGIC FROM sales
# MAGIC WHERE YEAR(transaction_date) = YEAR(CURRENT_DATE)
# MAGIC   AND MONTH(transaction_date) = MONTH(CURRENT_DATE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revenue Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monthly Revenue Trend with Growth
# MAGIC WITH monthly_revenue AS (
# MAGIC   SELECT 
# MAGIC     DATE_TRUNC('month', transaction_date) as month,
# MAGIC     SUM(total_amount) as revenue
# MAGIC   FROM sales
# MAGIC   GROUP BY DATE_TRUNC('month', transaction_date)
# MAGIC )
# MAGIC SELECT 
# MAGIC   month,
# MAGIC   revenue,
# MAGIC   LAG(revenue, 1) OVER (ORDER BY month) as prev_month_revenue,
# MAGIC   ROUND(((revenue - LAG(revenue, 1) OVER (ORDER BY month)) / 
# MAGIC          LAG(revenue, 1) OVER (ORDER BY month) * 100), 2) as mom_growth_pct
# MAGIC FROM monthly_revenue
# MAGIC ORDER BY month DESC
# MAGIC LIMIT 12

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Revenue by Region (for map visualization)
# MAGIC SELECT 
# MAGIC   region,
# MAGIC   COUNT(DISTINCT customer_id) as customers,
# MAGIC   COUNT(DISTINCT transaction_id) as transactions,
# MAGIC   SUM(total_amount) as total_revenue,
# MAGIC   AVG(total_amount) as avg_order_value,
# MAGIC   SUM(total_amount) / COUNT(DISTINCT customer_id) as revenue_per_customer
# MAGIC FROM sales
# MAGIC GROUP BY region
# MAGIC ORDER BY total_revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Analytics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer Segmentation Summary
# MAGIC SELECT 
# MAGIC   customer_segment,
# MAGIC   COUNT(*) as customer_count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
# MAGIC   AVG(projected_clv) as avg_clv,
# MAGIC   AVG(frequency) as avg_purchases
# MAGIC FROM customers
# MAGIC WHERE customer_segment IS NOT NULL
# MAGIC GROUP BY customer_segment
# MAGIC ORDER BY avg_clv DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- High-Value Customers at Risk of Churn
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   email,
# MAGIC   projected_clv,
# MAGIC   days_since_last_purchase,
# MAGIC   churn_risk
# MAGIC FROM customers
# MAGIC WHERE projected_clv > 5000
# MAGIC   AND churn_risk IN ('High', 'Medium')
# MAGIC ORDER BY projected_clv DESC
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer Cohort Analysis
# MAGIC WITH first_purchase AS (
# MAGIC   SELECT 
# MAGIC     customer_id,
# MAGIC     DATE_TRUNC('month', MIN(transaction_date)) as cohort_month
# MAGIC   FROM sales
# MAGIC   GROUP BY customer_id
# MAGIC ),
# MAGIC cohort_activity AS (
# MAGIC   SELECT 
# MAGIC     fp.cohort_month,
# MAGIC     DATE_TRUNC('month', s.transaction_date) as activity_month,
# MAGIC     COUNT(DISTINCT s.customer_id) as active_customers
# MAGIC   FROM first_purchase fp
# MAGIC   JOIN sales s ON fp.customer_id = s.customer_id
# MAGIC   GROUP BY fp.cohort_month, DATE_TRUNC('month', s.transaction_date)
# MAGIC )
# MAGIC SELECT 
# MAGIC   cohort_month,
# MAGIC   activity_month,
# MAGIC   active_customers,
# MAGIC   ROUND(MONTHS_BETWEEN(activity_month, cohort_month), 0) as months_since_first_purchase
# MAGIC FROM cohort_activity
# MAGIC ORDER BY cohort_month DESC, months_since_first_purchase

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Performance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 20 Products by Revenue
# MAGIC SELECT 
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   category,
# MAGIC   total_revenue,
# MAGIC   total_quantity_sold,
# MAGIC   unique_buyers,
# MAGIC   ROUND(profit_margin_pct, 2) as profit_margin_pct,
# MAGIC   revenue_rank
# MAGIC FROM products
# MAGIC WHERE revenue_rank <= 20
# MAGIC ORDER BY revenue_rank

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Category Performance Comparison
# MAGIC SELECT 
# MAGIC   category,
# MAGIC   COUNT(DISTINCT product_id) as product_count,
# MAGIC   SUM(total_revenue) as category_revenue,
# MAGIC   SUM(total_quantity_sold) as units_sold,
# MAGIC   AVG(profit_margin_pct) as avg_profit_margin,
# MAGIC   COUNT(DISTINCT unique_buyers) as total_customers
# MAGIC FROM products
# MAGIC WHERE category IS NOT NULL
# MAGIC GROUP BY category
# MAGIC ORDER BY category_revenue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Underperforming Products (candidates for discontinuation)
# MAGIC SELECT 
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   category,
# MAGIC   total_revenue,
# MAGIC   profit_margin_pct,
# MAGIC   total_quantity_sold
# MAGIC FROM products
# MAGIC WHERE total_revenue < 1000
# MAGIC   OR profit_margin_pct < 10
# MAGIC ORDER BY total_revenue ASC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales Channel Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Channel Performance Comparison
# MAGIC SELECT 
# MAGIC   sales_channel,
# MAGIC   COUNT(DISTINCT transaction_id) as transactions,
# MAGIC   COUNT(DISTINCT customer_id) as customers,
# MAGIC   SUM(total_amount) as revenue,
# MAGIC   AVG(total_amount) as avg_order_value,
# MAGIC   ROUND(SUM(total_amount) * 100.0 / SUM(SUM(total_amount)) OVER (), 2) as revenue_share_pct
# MAGIC FROM sales
# MAGIC GROUP BY sales_channel
# MAGIC ORDER BY revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time-Based Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Day of Week Performance
# MAGIC SELECT 
# MAGIC   DAYOFWEEK(transaction_date) as day_of_week,
# MAGIC   CASE DAYOFWEEK(transaction_date)
# MAGIC     WHEN 1 THEN 'Sunday'
# MAGIC     WHEN 2 THEN 'Monday'
# MAGIC     WHEN 3 THEN 'Tuesday'
# MAGIC     WHEN 4 THEN 'Wednesday'
# MAGIC     WHEN 5 THEN 'Thursday'
# MAGIC     WHEN 6 THEN 'Friday'
# MAGIC     WHEN 7 THEN 'Saturday'
# MAGIC   END as day_name,
# MAGIC   COUNT(*) as transactions,
# MAGIC   SUM(total_amount) as revenue,
# MAGIC   AVG(total_amount) as avg_order_value
# MAGIC FROM sales
# MAGIC GROUP BY DAYOFWEEK(transaction_date)
# MAGIC ORDER BY day_of_week

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Hour of Day Analysis (for operational planning)
# MAGIC SELECT 
# MAGIC   HOUR(transaction_date) as hour,
# MAGIC   COUNT(*) as transactions,
# MAGIC   SUM(total_amount) as revenue
# MAGIC FROM sales
# MAGIC GROUP BY HOUR(transaction_date)
# MAGIC ORDER BY hour

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Intelligence Export Views
# MAGIC 
# MAGIC These queries create views optimized for Power BI/Tableau connection

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create view for Power BI: Executive Dashboard
# MAGIC CREATE OR REPLACE VIEW gold.vw_executive_dashboard AS
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('month', transaction_date) as month,
# MAGIC   region,
# MAGIC   sales_channel,
# MAGIC   COUNT(DISTINCT transaction_id) as transactions,
# MAGIC   COUNT(DISTINCT customer_id) as customers,
# MAGIC   SUM(total_amount) as revenue,
# MAGIC   AVG(total_amount) as avg_order_value
# MAGIC FROM sales
# MAGIC GROUP BY DATE_TRUNC('month', transaction_date), region, sales_channel

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create view for Power BI: Customer Health
# MAGIC CREATE OR REPLACE VIEW gold.vw_customer_health AS
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   first_name || ' ' || last_name as customer_name,
# MAGIC   email,
# MAGIC   region,
# MAGIC   customer_segment,
# MAGIC   projected_clv,
# MAGIC   frequency as total_purchases,
# MAGIC   monetary_value as lifetime_spend,
# MAGIC   days_since_last_purchase,
# MAGIC   churn_risk
# MAGIC FROM customers

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC This notebook provides SQL-based analytics ready for:
# MAGIC - **Power BI:** Connect via Databricks SQL endpoint
# MAGIC - **Tableau:** Direct connection to Delta tables
# MAGIC - **Looker:** LookML models from these views
# MAGIC - **Custom dashboards:** REST API queries
# MAGIC 
# MAGIC All queries are optimized for performance with pre-aggregated Gold layer tables.