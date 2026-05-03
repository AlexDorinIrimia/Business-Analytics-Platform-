"""
Data Validation Module
Implements comprehensive data quality checks and validation rules
"""

from typing import Dict, List, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DateType
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class DataValidator:
    """
    Performs data quality checks and validation on DataFrames.
    Returns detailed quality reports and cleaned datasets.
    """
    
    def __init__(self, spark: SparkSession, config: Dict = None):
        self.spark = spark
        self.config = config or {}
        self.validation_results = []
        
    def validate_dataframe(
        self, 
        df: DataFrame, 
        dataset_name: str,
        required_columns: List[str] = None
    ) -> Tuple[DataFrame, Dict]:
        """
        Perform comprehensive validation on a DataFrame.
        
        Args:
            df: Input DataFrame to validate
            dataset_name: Name of the dataset for reporting
            required_columns: List of columns that must exist
            
        Returns:
            Tuple of (validated_df, validation_report)
        """
        logger.info(f"Starting validation for {dataset_name}")
        
        report = {
            "dataset_name": dataset_name,
            "validation_timestamp": datetime.now().isoformat(),
            "checks": {}
        }
        
        # Check 1: Record count validation
        record_count = df.count()
        min_records = self.config.get("min_record_count", 0)
        report["checks"]["record_count"] = {
            "total_records": record_count,
            "passed": record_count >= min_records,
            "threshold": min_records
        }
        
        # Check 2: Required columns validation
        if required_columns:
            missing_cols = set(required_columns) - set(df.columns)
            report["checks"]["required_columns"] = {
                "missing_columns": list(missing_cols),
                "passed": len(missing_cols) == 0
            }
        
        # Check 3: Null value analysis
        null_report = self._check_null_values(df)
        report["checks"]["null_values"] = null_report
        
        # Check 4: Duplicate detection
        duplicate_report = self._check_duplicates(df)
        report["checks"]["duplicates"] = duplicate_report
        
        # Check 5: Data type validation
        type_report = self._check_data_types(df)
        report["checks"]["data_types"] = type_report
        
        # Overall validation status
        all_checks_passed = all(
            check.get("passed", True) 
            for check in report["checks"].values()
            if isinstance(check, dict)
        )
        report["overall_status"] = "PASSED" if all_checks_passed else "FAILED"
        
        logger.info(f"Validation complete for {dataset_name}: {report['overall_status']}")
        
        return df, report
    
    def _check_null_values(self, df: DataFrame) -> Dict:
        """Check for null values in all columns"""
        max_null_pct = self.config.get("max_null_percentage", 0.1)
        
        null_counts = df.select([
            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
            for c in df.columns
        ]).collect()[0].asDict()
        
        total_rows = df.count()
        null_percentages = {
            col: (count / total_rows) if total_rows > 0 else 0
            for col, count in null_counts.items()
        }
        
        violations = {
            col: pct for col, pct in null_percentages.items()
            if pct > max_null_pct
        }
        
        return {
            "null_percentages": null_percentages,
            "violations": violations,
            "passed": len(violations) == 0,
            "threshold": max_null_pct
        }
    
    def _check_duplicates(self, df: DataFrame) -> Dict:
        """Check for duplicate records"""
        total_records = df.count()
        distinct_records = df.distinct().count()
        duplicate_count = total_records - distinct_records
        duplicate_percentage = duplicate_count / total_records if total_records > 0 else 0
        
        max_dup_pct = self.config.get("duplicate_threshold", 0.01)
        
        return {
            "total_records": total_records,
            "distinct_records": distinct_records,
            "duplicate_count": duplicate_count,
            "duplicate_percentage": duplicate_percentage,
            "passed": duplicate_percentage <= max_dup_pct,
            "threshold": max_dup_pct
        }
    
    def _check_data_types(self, df: DataFrame) -> Dict:
        """Validate data types are appropriate"""
        type_issues = []
        
        for field in df.schema.fields:
            col_name = field.name
            col_type = str(field.dataType)
            
            # Check for common type issues
            if "date" in col_name.lower() or "timestamp" in col_name.lower():
                if not isinstance(field.dataType, (DateType, TimestampType)):
                    type_issues.append({
                        "column": col_name,
                        "expected_type": "date/timestamp",
                        "actual_type": col_type
                    })
        
        return {
            "type_issues": type_issues,
            "passed": len(type_issues) == 0
        }
    
    def remove_duplicates(self, df: DataFrame, subset: List[str] = None) -> DataFrame:
        """
        Remove duplicate records from DataFrame.
        
        Args:
            df: Input DataFrame
            subset: List of columns to consider for duplicates
            
        Returns:
            DataFrame with duplicates removed
        """
        initial_count = df.count()
        
        if subset:
            df_clean = df.dropDuplicates(subset)
        else:
            df_clean = df.distinct()
        
        final_count = df_clean.count()
        removed = initial_count - final_count
        
        logger.info(f"Removed {removed} duplicate records")
        
        return df_clean
    
    def handle_null_values(
        self, 
        df: DataFrame, 
        strategy: str = "drop",
        fill_values: Dict = None
    ) -> DataFrame:
        """
        Handle null values based on specified strategy.
        
        Args:
            df: Input DataFrame
            strategy: "drop" or "fill"
            fill_values: Dictionary of column: fill_value for fill strategy
            
        Returns:
            DataFrame with null values handled
        """
        if strategy == "drop":
            df_clean = df.na.drop()
            logger.info("Dropped rows with null values")
        elif strategy == "fill" and fill_values:
            df_clean = df.na.fill(fill_values)
            logger.info(f"Filled null values: {fill_values}")
        else:
            df_clean = df
            logger.warning("No null handling strategy applied")
        
        return df_clean
    
    def validate_date_range(
        self, 
        df: DataFrame, 
        date_column: str,
        min_date: str = None,
        max_date: str = None
    ) -> Tuple[DataFrame, Dict]:
        """
        Validate that dates fall within expected range.
        
        Args:
            df: Input DataFrame
            date_column: Name of date column to validate
            min_date: Minimum allowed date (YYYY-MM-DD)
            max_date: Maximum allowed date (YYYY-MM-DD)
            
        Returns:
            Tuple of (filtered_df, validation_report)
        """
        total_records = df.count()
        
        if min_date:
            df = df.filter(F.col(date_column) >= F.lit(min_date))
        if max_date:
            df = df.filter(F.col(date_column) <= F.lit(max_date))
        
        valid_records = df.count()
        invalid_records = total_records - valid_records
        
        report = {
            "total_records": total_records,
            "valid_records": valid_records,
            "invalid_records": invalid_records,
            "date_range": f"{min_date} to {max_date}",
            "passed": invalid_records == 0
        }
        
        logger.info(f"Date validation: {valid_records}/{total_records} records valid")
        
        return df, report
