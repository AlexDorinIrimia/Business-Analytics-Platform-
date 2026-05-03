"""
Sales Analytics Module
Business logic for sales performance analysis and reporting
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import logging

logger = logging.getLogger(__name__)


class SalesAnalytics:
    """
    Sales performance analytics and aggregations.
    Implements time-series analysis, product performance, and regional reporting.
    """
    
    def __init__(self, spark):
        self.spark = spark
    
    def calculate_daily_sales(self, sales_df: DataFrame) -> DataFrame:
        """
        Aggregate sales by day.
        
        Args:
            sales_df: Raw sales transactions
            
        Returns:
            Daily sales aggregations
        """
        logger.info("Calculating daily sales metrics")
        
        daily_sales = sales_df.withColumn(
            "sale_date",
            F.to_date("transaction_date")
        ).groupBy("sale_date", "region", "sales_channel").agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("transaction_id").alias("transaction_count"),
            F.avg("total_amount").alias("avg_transaction_value"),
            F.sum("quantity").alias("total_units_sold"),
            F.countDistinct("customer_id").alias("unique_customers")
        )
        
        # Calculate revenue per customer
        daily_sales = daily_sales.withColumn(
            "revenue_per_customer",
            F.col("total_revenue") / F.col("unique_customers")
        )
        
        return daily_sales
    
    def calculate_monthly_trends(self, sales_df: DataFrame) -> DataFrame:
        """
        Calculate monthly sales trends with year-over-year comparisons.
        
        Args:
            sales_df: Sales transactions DataFrame
            
        Returns:
            Monthly trend analysis
        """
        logger.info("Calculating monthly sales trends")
        
        # Extract year and month
        monthly_df = sales_df.withColumn(
            "year",
            F.year("transaction_date")
        ).withColumn(
            "month",
            F.month("transaction_date")
        ).withColumn(
            "year_month",
            F.concat(
                F.col("year").cast("string"),
                F.lit("-"),
                F.format_string("%02d", F.col("month"))
            )
        )
        
        # Monthly aggregations
        monthly_agg = monthly_df.groupBy("year", "month", "year_month", "region").agg(
            F.sum("total_amount").alias("monthly_revenue"),
            F.count("transaction_id").alias("transaction_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.avg("total_amount").alias("avg_order_value")
        )
        
        # Calculate month-over-month growth
        window_spec = Window.partitionBy("region").orderBy("year", "month")
        
        monthly_trends = monthly_agg.withColumn(
            "prev_month_revenue",
            F.lag("monthly_revenue", 1).over(window_spec)
        ).withColumn(
            "mom_growth_pct",
            F.when(
                F.col("prev_month_revenue").isNotNull(),
                ((F.col("monthly_revenue") - F.col("prev_month_revenue")) / 
                 F.col("prev_month_revenue") * 100)
            ).otherwise(None)
        )
        
        # Calculate year-over-year growth
        window_yoy = Window.partitionBy("region", "month").orderBy("year")
        
        monthly_trends = monthly_trends.withColumn(
            "prev_year_revenue",
            F.lag("monthly_revenue", 1).over(window_yoy)
        ).withColumn(
            "yoy_growth_pct",
            F.when(
                F.col("prev_year_revenue").isNotNull(),
                ((F.col("monthly_revenue") - F.col("prev_year_revenue")) / 
                 F.col("prev_year_revenue") * 100)
            ).otherwise(None)
        )
        
        return monthly_trends.orderBy("year", "month", "region")
    
    def analyze_product_performance(
        self,
        sales_df: DataFrame,
        product_df: DataFrame
    ) -> DataFrame:
        """
        Analyze product sales performance.
        
        Args:
            sales_df: Sales transactions
            product_df: Product master data
            
        Returns:
            Product performance metrics
        """
        logger.info("Analyzing product performance")
        
        # Product-level aggregations
        product_sales = sales_df.groupBy("product_id").agg(
            F.sum("total_amount").alias("total_revenue"),
            F.sum("quantity").alias("total_quantity_sold"),
            F.count("transaction_id").alias("transaction_count"),
            F.countDistinct("customer_id").alias("unique_buyers"),
            F.avg("total_amount").alias("avg_sale_amount")
        )
        
        # Join with product master
        product_performance = product_df.join(
            product_sales,
            "product_id",
            "left"
        )
        
        # Calculate profit metrics (if cost data available)
        product_performance = product_performance.withColumn(
            "total_cost",
            F.when(
                F.col("unit_cost").isNotNull(),
                F.col("unit_cost") * F.col("total_quantity_sold")
            ).otherwise(None)
        ).withColumn(
            "gross_profit",
            F.when(
                F.col("total_cost").isNotNull(),
                F.col("total_revenue") - F.col("total_cost")
            ).otherwise(None)
        ).withColumn(
            "profit_margin_pct",
            F.when(
                (F.col("total_revenue") > 0) & F.col("gross_profit").isNotNull(),
                (F.col("gross_profit") / F.col("total_revenue") * 100)
            ).otherwise(None)
        )
        
        # Rank products by revenue
        window_spec = Window.orderBy(F.desc("total_revenue"))
        
        product_performance = product_performance.withColumn(
            "revenue_rank",
            F.row_number().over(window_spec)
        )
        
        return product_performance.orderBy("revenue_rank")
    
    def calculate_category_performance(
        self,
        sales_df: DataFrame,
        product_df: DataFrame
    ) -> DataFrame:
        """
        Aggregate sales performance by product category.
        
        Args:
            sales_df: Sales transactions
            product_df: Product master data
            
        Returns:
            Category-level performance metrics
        """
        logger.info("Calculating category performance")
        
        # Join sales with products to get categories
        sales_with_category = sales_df.join(
            product_df.select("product_id", "category", "subcategory"),
            "product_id",
            "left"
        )
        
        # Category aggregations
        category_performance = sales_with_category.groupBy("category").agg(
            F.sum("total_amount").alias("category_revenue"),
            F.sum("quantity").alias("total_units_sold"),
            F.countDistinct("product_id").alias("product_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.avg("total_amount").alias("avg_transaction_value")
        )
        
        # Calculate category contribution to total revenue
        total_revenue = sales_df.agg(F.sum("total_amount")).collect()[0][0]
        
        category_performance = category_performance.withColumn(
            "revenue_contribution_pct",
            (F.col("category_revenue") / F.lit(total_revenue) * 100)
        )
        
        return category_performance.orderBy(F.desc("category_revenue"))
    
    def analyze_regional_performance(self, sales_df: DataFrame) -> DataFrame:
        """
        Analyze sales performance by region.
        
        Args:
            sales_df: Sales transactions
            
        Returns:
            Regional performance metrics
        """
        logger.info("Analyzing regional performance")
        
        regional_sales = sales_df.groupBy("region").agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("transaction_id").alias("transaction_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.avg("total_amount").alias("avg_order_value"),
            F.sum("quantity").alias("total_units_sold")
        )
        
        # Calculate metrics per customer
        regional_sales = regional_sales.withColumn(
            "revenue_per_customer",
            F.col("total_revenue") / F.col("unique_customers")
        ).withColumn(
            "transactions_per_customer",
            F.col("transaction_count") / F.col("unique_customers")
        )
        
        # Rank regions
        window_spec = Window.orderBy(F.desc("total_revenue"))
        regional_sales = regional_sales.withColumn(
            "revenue_rank",
            F.row_number().over(window_spec)
        )
        
        return regional_sales.orderBy("revenue_rank")
    
    def calculate_sales_velocity(
        self,
        sales_df: DataFrame,
        window_days: int = 30
    ) -> DataFrame:
        """
        Calculate sales velocity (sales rate over time).
        
        Args:
            sales_df: Sales transactions
            window_days: Rolling window in days
            
        Returns:
            Sales velocity metrics
        """
        logger.info(f"Calculating {window_days}-day sales velocity")
        
        # Create date-product combinations
        daily_product_sales = sales_df.withColumn(
            "sale_date",
            F.to_date("transaction_date")
        ).groupBy("sale_date", "product_id").agg(
            F.sum("quantity").alias("daily_quantity"),
            F.sum("total_amount").alias("daily_revenue")
        )
        
        # Calculate rolling averages
        window_spec = Window.partitionBy("product_id").orderBy("sale_date") \
            .rangeBetween(-window_days, 0)
        
        velocity_df = daily_product_sales.withColumn(
            f"avg_daily_quantity_{window_days}d",
            F.avg("daily_quantity").over(window_spec)
        ).withColumn(
            f"avg_daily_revenue_{window_days}d",
            F.avg("daily_revenue").over(window_spec)
        )
        
        return velocity_df
    
    def identify_top_customers(
        self,
        sales_df: DataFrame,
        top_n: int = 100
    ) -> DataFrame:
        """
        Identify top customers by revenue.
        
        Args:
            sales_df: Sales transactions
            top_n: Number of top customers to return
            
        Returns:
            Top N customers by revenue
        """
        logger.info(f"Identifying top {top_n} customers")
        
        customer_revenue = sales_df.groupBy("customer_id").agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("transaction_id").alias("transaction_count"),
            F.avg("total_amount").alias("avg_order_value"),
            F.min("transaction_date").alias("first_purchase"),
            F.max("transaction_date").alias("last_purchase")
        )
        
        # Rank and filter top N
        window_spec = Window.orderBy(F.desc("total_revenue"))
        
        top_customers = customer_revenue.withColumn(
            "revenue_rank",
            F.row_number().over(window_spec)
        ).filter(F.col("revenue_rank") <= top_n)
        
        return top_customers.orderBy("revenue_rank")
