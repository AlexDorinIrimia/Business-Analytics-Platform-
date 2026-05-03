"""
Setup configuration for Business Analytics Platform
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="business-analytics-platform",
    version="1.0.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="Production-ready PySpark ETL pipeline for business analytics",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/pyspark-business-analytics-project",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.10",
    install_requires=[
        "pyspark>=3.5.0",
        "delta-spark>=3.0.0",
        "PyYAML>=6.0.1",
        "python-dateutil>=2.8.2",
        "pandas>=2.1.4",
        "numpy>=1.26.3",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.4",
            "pytest-cov>=4.1.0",
            "pytest-spark>=0.6.0",
            "black>=23.12.1",
            "flake8>=7.0.0",
            "mypy>=1.8.0",
        ],
        "quality": [
            "great-expectations>=0.18.8",
        ],
    },
    entry_points={
        "console_scripts": [
            "run-pipeline=pipeline:main",
        ],
    },
)
