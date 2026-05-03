"""
Customer Analytics Module
Business logic for customer segmentation, RFM analysis, and CLV calculation
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class CustomerAnalytics:
    """
    Customer analytics and segmentation transformations.
    Implements RFM analysis, CLV, churn prediction, and customer 360 views.
    """
    
    def __init__(self, spark):
        self.spark = spark
    
    def calculate_rfm_metrics(
        self, 
        sales_df: DataFrame,
        customer_df: DataFrame,
        reference_date: str = None
    ) -> DataFrame:
        """
        Calculate RFM (Recency, Frequency, Monetary) metrics for customers.
        
        Args:
            sales_df: Sales transactions DataFrame
            customer_df: Customer master DataFrame
            reference_date: Date to calculate recency from (default: today)
            
        Returns:
            DataFrame with RFM scores per customer
        """
        logger.info("Calculating RFM metrics")
        
        if not reference_date:
            reference_date = datetime.now().strftime("%Y-%m-%d")
        
        # Calculate metrics
        rfm_df = sales_df.groupBy("customer_id").agg(
            # Recency: Days since last purchase
            F.datediff(
                F.lit(reference_date), 
                F.max("transaction_date")
            ).alias("recency_days"),
            
            # Frequency: Number of transactions
            F.count("transaction_id").alias("frequency"),
            
            # Monetary: Total spend
            F.sum("total_amount").alias("monetary_value")
        )
        
        # Calculate RFM scores (1-5 scale, 5 being best)
        # Using percentile-based scoring
        recency_percentiles = rfm_df.approxQuantile("recency_days", [0.2, 0.4, 0.6, 0.8], 0.01)
        frequency_percentiles = rfm_df.approxQuantile("frequency", [0.2, 0.4, 0.6, 0.8], 0.01)
        monetary_percentiles = rfm_df.approxQuantile("monetary_value", [0.2, 0.4, 0.6, 0.8], 0.01)
        
        rfm_scored = rfm_df \
            .withColumn("recency_score", 
                F.when(F.col("recency_days") <= recency_percentiles[0], 5)
                .when(F.col("recency_days") <= recency_percentiles[1], 4)
                .when(F.col("recency_days") <= recency_percentiles[2], 3)
                .when(F.col("recency_days") <= recency_percentiles[3], 2)
                .otherwise(1)) \
            .withColumn("frequency_score",
                F.when(F.col("frequency") >= frequency_percentiles[3], 5)
                .when(F.col("frequency") >= frequency_percentiles[2], 4)
                .when(F.col("frequency") >= frequency_percentiles[1], 3)
                .when(F.col("frequency") >= frequency_percentiles[0], 2)
                .otherwise(1)) \
            .withColumn("monetary_score",
                F.when(F.col("monetary_value") >= monetary_percentiles[3], 5)
                .when(F.col("monetary_value") >= monetary_percentiles[2], 4)
                .when(F.col("monetary_value") >= monetary_percentiles[1], 3)
                .when(F.col("monetary_value") >= monetary_percentiles[0], 2)
                .otherwise(1))
        
        # Calculate composite RFM score
        rfm_final = rfm_scored.withColumn(
            "rfm_score",
            F.concat(
                F.col("recency_score").cast("string"),
                F.col("frequency_score").cast("string"),
                F.col("monetary_score").cast("string")
            )
        )
        
        # Join with customer data
        result = customer_df.join(rfm_final, "customer_id", "left")
        
        logger.info(f"Calculated RFM for {result.count()} customers")
        return result
    
    def segment_customers(self, rfm_df: DataFrame) -> DataFrame:
        """
        Segment customers based on RFM scores.
        
        Args:
            rfm_df: DataFrame with RFM scores
            
        Returns:
            DataFrame with customer segments
        """
        logger.info("Segmenting customers")
        
        segmented_df = rfm_df.withColumn(
            "customer_segment",
            F.when(
                (F.col("recency_score") >= 4) & 
                (F.col("frequency_score") >= 4) & 
                (F.col("monetary_score") >= 4),
                "Champions"
            )
            .when(
                (F.col("recency_score") >= 3) & 
                (F.col("frequency_score") >= 3) & 
                (F.col("monetary_score") >= 3),
                "Loyal Customers"
            )
            .when(
                (F.col("recency_score") >= 4) & 
                (F.col("frequency_score") <= 2),
                "New Customers"
            )
            .when(
                (F.col("recency_score") <= 2) & 
                (F.col("frequency_score") >= 4),
                "At Risk"
            )
            .when(
                (F.col("recency_score") <= 2) & 
                (F.col("frequency_score") <= 2) & 
                (F.col("monetary_score") >= 4),
                "Hibernating High Value"
            )
            .when(
                (F.col("recency_score") <= 2),
                "Lost Customers"
            )
            .otherwise("Potential Loyalists")
        )
        
        return segmented_df
    
    def calculate_customer_lifetime_value(
        self,
        sales_df: DataFrame,
        customer_df: DataFrame,
        projection_months: int = 12
    ) -> DataFrame:
        """
        Calculate Customer Lifetime Value (CLV).
        
        Args:
            sales_df: Sales transactions DataFrame
            customer_df: Customer master DataFrame
            projection_months: Number of months to project CLV
            
        Returns:
            DataFrame with CLV per customer
        """
        logger.info("Calculating Customer Lifetime Value")
        
        # Calculate customer metrics
        customer_metrics = sales_df.groupBy("customer_id").agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("transaction_id").alias("total_transactions"),
            F.avg("total_amount").alias("avg_transaction_value"),
            F.min("transaction_date").alias("first_purchase_date"),
            F.max("transaction_date").alias("last_purchase_date")
        )
        
        # Calculate customer age in months
        customer_metrics = customer_metrics.withColumn(
            "customer_age_months",
            F.months_between(
                F.col("last_purchase_date"),
                F.col("first_purchase_date")
            )
        )
        
        # Calculate average purchase frequency (purchases per month)
        customer_metrics = customer_metrics.withColumn(
            "purchase_frequency_monthly",
            F.when(
                F.col("customer_age_months") > 0,
                F.col("total_transactions") / F.col("customer_age_months")
            ).otherwise(F.col("total_transactions"))
        )
        
        # Project CLV = avg_transaction_value * purchase_frequency * projection_months
        # This is a simplified CLV model
        clv_df = customer_metrics.withColumn(
            "projected_clv",
            F.col("avg_transaction_value") * 
            F.col("purchase_frequency_monthly") * 
            F.lit(projection_months)
        )
        
        # Add historical CLV
        clv_df = clv_df.withColumn(
            "historical_clv",
            F.col("total_revenue")
        )
        
        # Join with customer master data
        result = customer_df.join(clv_df, "customer_id", "left")
        
        logger.info(f"Calculated CLV for {result.count()} customers")
        return result
    
    def identify_churn_risk(
        self,
        sales_df: DataFrame,
        customer_df: DataFrame,
        inactivity_threshold_days: int = 180
    ) -> DataFrame:
        """
        Identify customers at risk of churning.
        
        Args:
            sales_df: Sales transactions DataFrame
            customer_df: Customer master DataFrame
            inactivity_threshold_days: Days of inactivity to flag as churn risk
            
        Returns:
            DataFrame with churn risk flags
        """
        logger.info("Identifying churn risk customers")
        
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        # Calculate days since last purchase
        last_purchase = sales_df.groupBy("customer_id").agg(
            F.max("transaction_date").alias("last_purchase_date")
        )
        
        churn_df = last_purchase.withColumn(
            "days_since_last_purchase",
            F.datediff(F.lit(current_date), F.col("last_purchase_date"))
        ).withColumn(
            "churn_risk",
            F.when(
                F.col("days_since_last_purchase") >= inactivity_threshold_days,
                "High"
            )
            .when(
                F.col("days_since_last_purchase") >= (inactivity_threshold_days * 0.7),
                "Medium"
            )
            .otherwise("Low")
        )
        
        # Join with customer data
        result = customer_df.join(churn_df, "customer_id", "left")
        
        return result
    
    def create_customer_360_view(
        self,
        customer_df: DataFrame,
        rfm_df: DataFrame,
        clv_df: DataFrame,
        churn_df: DataFrame
    ) -> DataFrame:
        """
        Create a comprehensive 360-degree customer view.
        
        Args:
            customer_df: Customer master DataFrame
            rfm_df: RFM analysis DataFrame
            clv_df: CLV DataFrame
            churn_df: Churn risk DataFrame
            
        Returns:
            Consolidated customer 360 DataFrame
        """
        logger.info("Creating customer 360 view")
        
        # Start with customer master
        customer_360 = customer_df
        
        # Add RFM metrics
        rfm_cols = ["customer_id", "recency_days", "frequency", "monetary_value", 
                    "rfm_score", "customer_segment"]
        customer_360 = customer_360.join(
            rfm_df.select(rfm_cols),
            "customer_id",
            "left"
        )
        
        # Add CLV metrics
        clv_cols = ["customer_id", "projected_clv", "historical_clv"]
        customer_360 = customer_360.join(
            clv_df.select(clv_cols),
            "customer_id",
            "left"
        )
        
        # Add churn risk
        churn_cols = ["customer_id", "churn_risk", "days_since_last_purchase"]
        customer_360 = customer_360.join(
            churn_df.select(churn_cols),
            "customer_id",
            "left"
        )
        
        logger.info(f"Created 360 view for {customer_360.count()} customers")
        return customer_360
