# Business Analytics Platform - PySpark ETL Pipeline

A production-ready PySpark data engineering project demonstrating ETL pipelines, data quality checks, and business intelligence transformations for an enterprise business management system.

## 🎯 Project Overview

This project simulates a real-world business analytics platform that processes sales transactions, customer data, and product inventory to generate actionable business intelligence insights. It showcases enterprise-grade data engineering practices applicable to ERP and business management systems.

## 📊 Business Problem

Enterprise organizations need to:
- Process millions of daily transactions from multiple sales channels
- Maintain accurate customer 360-degree views
- Track inventory levels and product performance
- Generate real-time business intelligence dashboards
- Ensure data quality and consistency across systems

## 🏗️ Architecture

```
Raw Data (Bronze Layer)
    ↓
Data Validation & Cleaning (Silver Layer)
    ↓
Business Aggregations (Gold Layer)
    ↓
Analytics & Reporting
```

## 🚀 Features

### ETL Pipeline Components
- **Data Ingestion**: Multiple file formats (CSV, JSON, Parquet)
- **Data Validation**: Schema validation, null checks, duplicate detection
- **Data Transformation**: Complex joins, window functions, aggregations
- **Performance Optimization**: Partitioning, caching, broadcast joins
- **Error Handling**: Comprehensive logging and data quality reporting

### Business Analytics
- Customer Lifetime Value (CLV) calculation
- Product performance analysis
- Sales trend analysis with time-series aggregations
- Inventory turnover metrics
- Regional sales performance

## 📁 Project Structure

```
pyspark-business-analytics-project/
├── README.md
├── requirements.txt
├── setup.py
├── config/
│   ├── pipeline_config.yaml
│   └── databricks_config.json
├── data/
│   ├── raw/               # Sample input data
│   ├── processed/         # Intermediate data
│   └── output/            # Final analytics outputs
├── src/
│   ├── __init__.py
│   ├── ingestion/         # Data ingestion modules
│   ├── transformations/   # Business logic transformations
│   ├── validation/        # Data quality checks
│   └── utils/             # Helper functions
├── notebooks/
│   ├── 01_data_exploration.ipynb
│   ├── 02_etl_pipeline.ipynb
│   └── 03_analytics_dashboard.ipynb
├── tests/
│   └── test_transformations.py
└── docs/
    └── technical_design.md
```

## 🛠️ Technology Stack

- **Apache Spark 3.5+** (PySpark)
- **Databricks Runtime 14.3 LTS**
- **Python 3.10+**
- **Delta Lake** for ACID transactions
- **PyYAML** for configuration management

## 🔧 Installation & Setup

### Prerequisites
- Databricks account (Community Edition works)
- Python 3.10+
- Git

### Local Development Setup

```bash
# Clone the repository
git clone <your-repo-url>
cd pyspark-business-analytics-project

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Databricks Setup

1. **Upload to Databricks**:
   - Create a new Databricks workspace folder
   - Upload all project files via Databricks CLI or UI

2. **Configure Cluster**:
   - Runtime: 14.3 LTS (Spark 3.5.0, Scala 2.12)
   - Worker type: Standard_DS3_v2 (or equivalent)
   - Workers: 2-4 nodes (depending on data volume)

3. **Install Libraries**:
   ```
   PyYAML
   delta-spark
   ```

## 📖 Usage

### Running the Full Pipeline

```python
from src.pipeline import BusinessAnalyticsPipeline

# Initialize pipeline
pipeline = BusinessAnalyticsPipeline(
    config_path="config/pipeline_config.yaml",
    spark=spark
)

# Execute end-to-end pipeline
pipeline.run()
```

### Running Individual Components

```python
# Data ingestion only
from src.ingestion.data_loader import DataLoader

loader = DataLoader(spark)
sales_df = loader.load_sales_data("data/raw/sales/")

# Apply transformations
from src.transformations.customer_analytics import CustomerAnalytics

customer_analytics = CustomerAnalytics(spark)
clv_df = customer_analytics.calculate_customer_lifetime_value(sales_df)
```

## 🧪 Testing

```bash
# Run unit tests
pytest tests/

# Run with coverage
pytest --cov=src tests/
```

## 📊 Sample Analytics Outputs

The pipeline generates the following business intelligence outputs:

1. **Customer Segmentation**: RFM analysis and customer segments
2. **Sales Performance**: Daily/weekly/monthly aggregations by region and product
3. **Inventory Metrics**: Stock levels, turnover rates, reorder alerts
4. **Product Analytics**: Best/worst performers, cross-sell opportunities

## 🎓 Key Learning Demonstrations

This project demonstrates:

- ✅ **PySpark Fundamentals**: DataFrames, SQL, transformations
- ✅ **Advanced Operations**: Window functions, UDFs, complex aggregations
- ✅ **Performance Tuning**: Partitioning strategies, caching, broadcast joins
- ✅ **Data Quality**: Validation frameworks, error handling
- ✅ **Production Patterns**: Modular code, configuration management, logging
- ✅ **Delta Lake**: ACID transactions, time travel, schema evolution
- ✅ **Testing**: Unit tests for transformations and business logic

## 🔍 Code Quality

- Type hints throughout
- PEP 8 compliant
- Comprehensive docstrings
- Error handling and logging
- Configurable and maintainable

## 📝 Documentation

See [Technical Design Document](docs/technical_design.md) for detailed architecture and design decisions.

## 🤝 Contributing

This is a portfolio project, but suggestions and improvements are welcome!

## 📄 License

MIT License - feel free to use this for your learning and portfolio.

## 👤 Author

Created as a demonstration project for data engineering roles.

---

**Note**: This project uses synthetic data for demonstration purposes. All business scenarios are fictional but representative of real-world enterprise analytics requirements.
