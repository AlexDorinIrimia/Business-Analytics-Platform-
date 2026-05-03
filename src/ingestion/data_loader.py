"""
Data Loader Module
Handles ingestion of various data sources into PySpark DataFrames
"""

from typing import Dict, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, DateType
)
import logging

logger = logging.getLogger(__name__)


class DataLoader:
    """
    Centralized data loading class for all data sources.
    Supports multiple file formats and includes schema validation.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def load_sales_data(
        self, 
        path: str, 
        file_format: str = "parquet"
    ) -> DataFrame:
        """
        Load sales transaction data.
        
        Args:
            path: Path to sales data files
            file_format: Format of files (parquet, csv, json)
            
        Returns:
            DataFrame with sales transactions
        """
        schema = self._get_sales_schema()
        
        logger.info(f"Loading sales data from {path}")
        
        if file_format == "csv":
            df = self.spark.read.format("csv") \
                .option("header", "true") \
                .schema(schema) \
                .load(path)
        elif file_format == "json":
            df = self.spark.read.format("json") \
                .schema(schema) \
                .load(path)
        else:  # parquet (default)
            df = self.spark.read.format("parquet") \
                .load(path)
        
        logger.info(f"Loaded {df.count()} sales records")
        return df
    
    def load_customer_data(
        self, 
        path: str, 
        file_format: str = "parquet"
    ) -> DataFrame:
        """
        Load customer master data.
        
        Args:
            path: Path to customer data files
            file_format: Format of files
            
        Returns:
            DataFrame with customer information
        """
        schema = self._get_customer_schema()
        
        logger.info(f"Loading customer data from {path}")
        
        df = self.spark.read.format(file_format) \
            .option("header", "true") \
            .schema(schema) \
            .load(path)
        
        logger.info(f"Loaded {df.count()} customer records")
        return df
    
    def load_product_data(
        self, 
        path: str, 
        file_format: str = "parquet"
    ) -> DataFrame:
        """
        Load product catalog data.
        
        Args:
            path: Path to product data files
            file_format: Format of files
            
        Returns:
            DataFrame with product information
        """
        schema = self._get_product_schema()
        
        logger.info(f"Loading product data from {path}")
        
        df = self.spark.read.format(file_format) \
            .option("header", "true") \
            .schema(schema) \
            .load(path)
        
        logger.info(f"Loaded {df.count()} product records")
        return df
    
    def load_inventory_data(
        self, 
        path: str, 
        file_format: str = "parquet"
    ) -> DataFrame:
        """
        Load inventory snapshot data.
        
        Args:
            path: Path to inventory data files
            file_format: Format of files
            
        Returns:
            DataFrame with inventory levels
        """
        schema = self._get_inventory_schema()
        
        logger.info(f"Loading inventory data from {path}")
        
        df = self.spark.read.format(file_format) \
            .option("header", "true") \
            .schema(schema) \
            .load(path)
        
        logger.info(f"Loaded {df.count()} inventory records")
        return df
    
    @staticmethod
    def _get_sales_schema() -> StructType:
        """Define schema for sales transactions"""
        return StructType([
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
    
    @staticmethod
    def _get_customer_schema() -> StructType:
        """Define schema for customer data"""
        return StructType([
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
    
    @staticmethod
    def _get_product_schema() -> StructType:
        """Define schema for product data"""
        return StructType([
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
    
    @staticmethod
    def _get_inventory_schema() -> StructType:
        """Define schema for inventory data"""
        return StructType([
            StructField("product_id", StringType(), False),
            StructField("warehouse_id", StringType(), False),
            StructField("quantity_on_hand", IntegerType(), False),
            StructField("quantity_reserved", IntegerType(), True),
            StructField("quantity_available", IntegerType(), False),
            StructField("reorder_point", IntegerType(), True),
            StructField("snapshot_date", DateType(), False)
        ])
