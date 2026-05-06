"""
Unit Tests for Data Transformations
Tests business logic and data quality functions
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from transformations.customer_analytics import CustomerAnalytics
from transformations.sales_analytics import SalesAnalytics
from validation.data_validator import DataValidator


@pytest.fixture(scope="session")
def spark():
    """Create SparkSession for testing"""
    return SparkSession.builder \
        .appName("PySparkTests") \
        .master("local[*]") \
        .getOrCreate()


@pytest.fixture
def sample_sales_data(spark):
    """Create sample sales data for testing"""
    data = [
        ("TXN001", datetime(2024, 1, 1), "CUST001", "PROD001", 2, 100.0, 0.1, 180.0, "Online", "North"),
        ("TXN002", datetime(2024, 1, 2), "CUST001", "PROD002", 1, 200.0, 0.0, 200.0, "Store", "North"),
        ("TXN003", datetime(2024, 1, 3), "CUST002", "PROD001", 3, 100.0, 0.0, 300.0, "Mobile App", "South"),
        ("TXN004", datetime(2024, 2, 1), "CUST001", "PROD003", 1, 150.0, 0.05, 142.5, "Online", "North"),
        ("TXN005", datetime(2024, 2, 2), "CUST003", "PROD001", 5, 100.0, 0.2, 400.0, "Store", "East"),
    ]
    
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("transaction_date", TimestampType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("discount_percentage", DoubleType(), True),
        StructField("total_amount", DoubleType(), False),
        StructField("sales_channel", StringType(), True),
        StructField("region", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_customer_data(spark):
    """Create sample customer data for testing"""
    data = [
        ("CUST001", "John", "Doe", "john@example.com", datetime(2023, 1, 1), "North", "Y"),
        ("CUST002", "Jane", "Smith", "jane@example.com", datetime(2023, 6, 1), "South", "Y"),
        ("CUST003", "Bob", "Johnson", "bob@example.com", datetime(2024, 1, 1), "East", "Y"),
    ]
    
    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("registration_date", DateType(), False),
        StructField("region", StringType(), True),
        StructField("is_active", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_product_data(spark):
    """Create sample product data for testing"""
    data = [
        ("PROD001", "Product A", "Electronics", 50.0, 100.0),
        ("PROD002", "Product B", "Clothing", 80.0, 200.0),
        ("PROD003", "Product C", "Electronics", 75.0, 150.0),
    ]
    
    schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), True),
        StructField("unit_cost", DoubleType(), True),
        StructField("unit_price", DoubleType(), False)
    ])
    
    return spark.createDataFrame(data, schema)


class TestCustomerAnalytics:
    """Test cases for CustomerAnalytics class"""
    
    def test_calculate_rfm_metrics(self, spark, sample_sales_data, sample_customer_data):
        """Test RFM calculation"""
        analytics = CustomerAnalytics(spark)
        
        rfm_df = analytics.calculate_rfm_metrics(
            sample_sales_data,
            sample_customer_data,
            reference_date="2024-02-15"
        )
        
        # Verify columns exist
        assert "recency_days" in rfm_df.columns
        assert "frequency" in rfm_df.columns
        assert "monetary_value" in rfm_df.columns
        assert "rfm_score" in rfm_df.columns
        
        # Verify data
        result = rfm_df.filter(F.col("customer_id") == "CUST001").collect()[0]
        assert result["frequency"] == 3  # CUST001 has 3 transactions
        assert result["monetary_value"] == 522.5  # Sum of all CUST001 transactions
    
    def test_segment_customers(self, spark, sample_sales_data, sample_customer_data):
        """Test customer segmentation"""
        analytics = CustomerAnalytics(spark)
        
        rfm_df = analytics.calculate_rfm_metrics(
            sample_sales_data,
            sample_customer_data,
            reference_date="2024-02-15"
        )
        
        segmented_df = analytics.segment_customers(rfm_df)
        
        # Verify segment column exists
        assert "customer_segment" in segmented_df.columns
        
        # Verify all customers have a segment
        null_segments = segmented_df.filter(F.col("customer_segment").isNull()).count()
        assert null_segments == 0
    
    def test_calculate_clv(self, spark, sample_sales_data, sample_customer_data):
        """Test CLV calculation"""
        analytics = CustomerAnalytics(spark)
        
        clv_df = analytics.calculate_customer_lifetime_value(
            sample_sales_data,
            sample_customer_data,
            projection_months=12
        )
        
        # Verify CLV columns exist
        assert "projected_clv" in clv_df.columns
        assert "historical_clv" in clv_df.columns
        
        # Verify CLV is calculated for customers with transactions
        result = clv_df.filter(F.col("customer_id") == "CUST001").collect()[0]
        assert result["historical_clv"] == 522.5
        assert result["projected_clv"] is not None


class TestSalesAnalytics:
    """Test cases for SalesAnalytics class"""
    
    def test_calculate_daily_sales(self, spark, sample_sales_data):
        """Test daily sales aggregation"""
        analytics = SalesAnalytics(spark)
        
        daily_df = analytics.calculate_daily_sales(sample_sales_data)
        
        # Verify aggregation columns exist
        assert "total_revenue" in daily_df.columns
        assert "transaction_count" in daily_df.columns
        assert "avg_transaction_value" in daily_df.columns
        assert "unique_customers" in daily_df.columns
        
        # Verify aggregation accuracy
        total_revenue = daily_df.agg(F.sum("total_revenue")).collect()[0][0]
        expected_total = sample_sales_data.agg(F.sum("total_amount")).collect()[0][0]
        assert abs(total_revenue - expected_total) < 0.01
    
    def test_analyze_product_performance(self, spark, sample_sales_data, sample_product_data):
        """Test product performance analysis"""
        analytics = SalesAnalytics(spark)
        
        performance_df = analytics.analyze_product_performance(
            sample_sales_data,
            sample_product_data
        )
        
        # Verify metrics exist
        assert "total_revenue" in performance_df.columns
        assert "total_quantity_sold" in performance_df.columns
        assert "gross_profit" in performance_df.columns
        assert "profit_margin_pct" in performance_df.columns
        
        # Verify PROD001 metrics (appears in 3 transactions)
        prod001 = performance_df.filter(F.col("product_id") == "PROD001").collect()[0]
        assert prod001["total_quantity_sold"] == 10  # 2 + 3 + 5
    
    def test_analyze_regional_performance(self, spark, sample_sales_data):
        """Test regional performance analysis"""
        analytics = SalesAnalytics(spark)
        
        regional_df = analytics.analyze_regional_performance(sample_sales_data)
        
        # Verify metrics
        assert "total_revenue" in regional_df.columns
        assert "unique_customers" in regional_df.columns
        assert "revenue_per_customer" in regional_df.columns
        
        # Verify North region (has 3 transactions)
        north = regional_df.filter(F.col("region") == "North").collect()[0]
        assert north["transaction_count"] == 3
        assert north["unique_customers"] == 1  # Only CUST001


class TestDataValidator:
    """Test cases for DataValidator class"""
    
    def test_validate_dataframe(self, spark, sample_sales_data):
        """Test DataFrame validation"""
        config = {
            "min_record_count": 3,
            "max_null_percentage": 0.1,
            "duplicate_threshold": 0.01
        }
        
        validator = DataValidator(spark, config)
        
        validated_df, report = validator.validate_dataframe(
            sample_sales_data,
            "test_sales",
            required_columns=["transaction_id", "customer_id"]
        )
        
        # Verify report structure
        assert "dataset_name" in report
        assert "checks" in report
        assert "overall_status" in report
        
        # Verify checks
        assert "record_count" in report["checks"]
        assert "required_columns" in report["checks"]
        assert "null_values" in report["checks"]
        assert "duplicates" in report["checks"]
    
    def test_remove_duplicates(self, spark):
        """Test duplicate removal"""
        # Create data with duplicates
        data = [
            ("TXN001", "CUST001", 100.0),
            ("TXN001", "CUST001", 100.0),  # Duplicate
            ("TXN002", "CUST002", 200.0),
        ]
        
        schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("amount", DoubleType(), False)
        ])
        
        df = spark.createDataFrame(data, schema)
        
        validator = DataValidator(spark)
        clean_df = validator.remove_duplicates(df, subset=["transaction_id"])
        
        assert clean_df.count() == 2  # Should remove 1 duplicate
    
    def test_validate_date_range(self, spark, sample_sales_data):
        """Test date range validation"""
        validator = DataValidator(spark)
        
        filtered_df, report = validator.validate_date_range(
            sample_sales_data,
            "transaction_date",
            min_date="2024-01-01",
            max_date="2024-01-31"
        )
        
        # Should filter out February transactions
        assert filtered_df.count() == 3  # Only January transactions
        assert report["invalid_records"] == 2  # Two February records


class TestIntegration:
    """Integration tests combining multiple components"""
    
    def test_end_to_end_customer_analytics(
        self, 
        spark, 
        sample_sales_data, 
        sample_customer_data
    ):
        """Test complete customer analytics pipeline"""
        
        # Step 1: Validate data
        validator = DataValidator(spark)
        validated_sales, _ = validator.validate_dataframe(
            sample_sales_data,
            "sales"
        )
        
        # Step 2: Calculate RFM
        analytics = CustomerAnalytics(spark)
        rfm_df = analytics.calculate_rfm_metrics(
            validated_sales,
            sample_customer_data
        )
        
        # Step 3: Segment customers
        segmented_df = analytics.segment_customers(rfm_df)
        
        # Verify pipeline output
        assert segmented_df.count() == sample_customer_data.count()
        assert "customer_segment" in segmented_df.columns
        assert "rfm_score" in segmented_df.columns
    
    def test_end_to_end_sales_analytics(
        self,
        spark,
        sample_sales_data,
        sample_product_data
    ):
        """Test complete sales analytics pipeline"""
        
        # Step 1: Validate data
        validator = DataValidator(spark)
        validated_sales, _ = validator.validate_dataframe(
            sample_sales_data,
            "sales"
        )
        
        # Step 2: Analyze product performance
        analytics = SalesAnalytics(spark)
        product_perf = analytics.analyze_product_performance(
            validated_sales,
            sample_product_data
        )
        
        # Step 3: Calculate regional performance
        regional_perf = analytics.analyze_regional_performance(validated_sales)
        
        # Verify outputs
        assert product_perf.count() == sample_product_data.count()
        assert regional_perf.count() > 0
        assert "gross_profit" in product_perf.columns
        assert "revenue_per_customer" in regional_perf.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
