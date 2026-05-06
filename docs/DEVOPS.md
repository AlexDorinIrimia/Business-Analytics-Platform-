# DevOps Practices & CI/CD

## Overview

This document outlines the DevOps practices, CI/CD pipeline, and deployment strategies for the Business Analytics Platform.

---

## Development Workflow

### Branching Strategy (Git Flow)

```
main (production)
  ↑
  └─ develop (integration)
       ↑
       ├─ feature/customer-analytics
       ├─ feature/sales-dashboard
       └─ hotfix/data-quality-bug
```

**Branch Types:**
- `main`: Production-ready code, auto-deploys to prod
- `develop`: Integration branch for features
- `feature/*`: New features (branch from develop)
- `hotfix/*`: Urgent production fixes (branch from main)
- `release/*`: Release preparation (branch from develop)

**Workflow:**
1. Create feature branch from `develop`
2. Develop and commit changes
3. Open pull request to `develop`
4. Code review + automated tests
5. Merge to `develop`
6. When ready for release, merge `develop` → `main`
7. Auto-deploy to production

---

## Testing Strategy

### Test Pyramid

```
        /\
       /  \  E2E Tests (5%)
      /────\
     /      \  Integration Tests (15%)
    /────────\
   /          \  Unit Tests (80%)
  /────────────\
```

### 1. Unit Tests (80% of tests)

**What:** Test individual functions and business logic

**Location:** `tests/test_transformations.py`, `tests/test_validation.py`

**Example:**
```python
def test_rfm_calculation():
    # Given
    test_data = create_sample_customer_data()
    
    # When
    result = calculate_rfm(test_data)
    
    # Then
    assert result["CUST001"]["rfm_score"] == "555"
    assert result["CUST001"]["customer_segment"] == "Champions"
```

**Run:**
```bash
pytest tests/test_transformations.py -v
```

---

### 2. Integration Tests (15% of tests)

**What:** Test end-to-end pipeline flows

**Example:**
```python
def test_bronze_to_gold_pipeline():
    # Given: Raw data in Bronze
    bronze_df = load_bronze_sales()
    
    # When: Run full pipeline
    silver_df = clean_and_validate(bronze_df)
    gold_df = generate_analytics(silver_df)
    
    # Then: Verify output quality
    assert gold_df.count() > 0
    assert gold_df.filter(col("revenue").isNull()).count() == 0
```

---

### 3. End-to-End Tests (5% of tests)

**What:** Test complete user workflows

**Example:**
```python
def test_executive_dashboard_query():
    # Simulate Power BI query
    result = spark.sql("""
        SELECT region, SUM(revenue) as total
        FROM gold.monthly_revenue
        WHERE month >= '2024-01-01'
        GROUP BY region
    """)
    
    assert result.count() == 5  # 5 regions
```

---

### Test Coverage Goals

- **Minimum:** 80% code coverage
- **Target:** 85%+ code coverage
- **Critical paths:** 100% coverage (data ingestion, transformations)

**Check coverage:**
```bash
pytest --cov=src --cov-report=html tests/
open htmlcov/index.html
```

---

## CI/CD Pipeline

### Pipeline Stages

```
Code Push → Code Quality → Tests → Security Scan → Deploy → Verify
```

### 1. Code Quality Stage

**Tools:**
- **Black**: Code formatting
- **Flake8**: Linting
- **MyPy**: Type checking

**Run locally:**
```bash
# Format code
black src/ tests/

# Lint
flake8 src/ tests/ --max-line-length=100

# Type check
mypy src/ --ignore-missing-imports
```

---

### 2. Test Stage

**Runs:**
- All unit tests
- Integration tests
- Generates coverage report

**GitHub Actions automatically:**
- Runs tests on every PR
- Blocks merge if tests fail
- Uploads coverage to Codecov

---

### 3. Security Scan Stage

**Tools:**
- **Bandit**: Finds security issues in Python code
- **Safety**: Checks for known vulnerabilities in dependencies

**Run locally:**
```bash
bandit -r src/
safety check -r requirements.txt
```

---

### 4. Deployment Stage

**Triggered:** Only on merge to `main` branch

**Steps:**
1. Deploy notebooks to Databricks workspace
2. Update job configurations
3. Trigger test run
4. Monitor for errors

**Rollback:** Revert to previous Git commit if deployment fails

---

## Deployment Environments

| Environment | Branch | Auto-Deploy | Purpose |
|-------------|--------|-------------|---------|
| Development | feature/* | No | Local testing |
| Staging | develop | Yes | Integration testing |
| Production | main | Yes | Live customer data |

### Environment Configuration

**config/dev.yaml:**
```yaml
environment: development
databricks:
  workspace_url: https://dev-workspace.databricks.com
  cluster_id: dev-cluster-123
paths:
  bronze: /mnt/dev/bronze
  silver: /mnt/dev/silver
  gold: /mnt/dev/gold
```

**config/prod.yaml:**
```yaml
environment: production
databricks:
  workspace_url: https://prod-workspace.databricks.com
  cluster_id: prod-cluster-456
paths:
  bronze: /mnt/prod/bronze
  silver: /mnt/prod/silver
  gold: /mnt/prod/gold
```

---

## Secrets Management

**Never commit:**
- API keys
- Database passwords
- Service principal credentials
- Access tokens

**Use GitHub Secrets:**

```yaml
# In GitHub Actions
env:
  DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
  AZURE_CLIENT_ID: ${{ secrets.AZURE_CLIENT_ID }}
```

**Locally use .env:**
```bash
# .env (never commit this file!)
DATABRICKS_HOST=https://your-workspace.databricks.com
DATABRICKS_TOKEN=your-token-here
```

---

## Monitoring & Observability

### Pipeline Monitoring

**Key Metrics:**
- Job success rate (target: >99%)
- Pipeline duration (baseline: 3 minutes)
- Data quality score (target: >95%)
- Cost per run (track trend)

**Alerts:**
- Email on job failure
- Slack notification on quality degradation
- PagerDuty for critical failures

### Databricks Monitoring

```python
# Add to each notebook
import time

start_time = time.time()

# ... pipeline code ...

duration = time.time() - start_time
dbutils.jobs.taskValues.set(key="duration", value=duration)

# Log metrics
spark.sql(f"""
  INSERT INTO monitoring.pipeline_metrics
  VALUES (current_timestamp(), 'etl-pipeline', {duration}, {record_count})
""")
```

---

## Debugging & Troubleshooting

### Common Issues

**1. Out of Memory Errors**
```python
# Solution: Reduce partition size
df.repartition(200)  # More partitions = smaller chunks
```

**2. Slow Joins**
```python
# Solution: Broadcast small tables
from pyspark.sql.functions import broadcast
large_df.join(broadcast(small_df), "key")
```

**3. Data Skew**
```python
# Solution: Salt the join key
df.withColumn("salt", (rand() * 10).cast("int"))
  .withColumn("salted_key", concat(col("key"), lit("_"), col("salt")))
```

### Debugging Tips

**1. Use DataFrame.explain() to see execution plan:**
```python
df.groupBy("region").sum("revenue").explain(True)
```

**2. Check Spark UI for slow stages:**
- Go to Databricks cluster → Spark UI
- Look for stages taking >50% of total time
- Check data shuffle size

**3. Add logging:**
```python
import logging
logger = logging.getLogger(__name__)

logger.info(f"Processing {df.count()} records")
logger.warning(f"Found {null_count} null values")
```

---

## Pre-Deployment Checklist

Before deploying to production:

- [ ] All tests passing (unit + integration)
- [ ] Code coverage >80%
- [ ] No security vulnerabilities
- [ ] Configuration files updated
- [ ] Documentation updated
- [ ] Peer code review completed
- [ ] Staging deployment successful
- [ ] Performance benchmarks met
- [ ] Rollback plan documented
- [ ] Stakeholders notified

---

## Release Process

### 1. Pre-Release

```bash
# Create release branch
git checkout -b release/v1.2.0 develop

# Update version
echo "1.2.0" > VERSION

# Run full test suite
pytest tests/ -v

# Update CHANGELOG
vim CHANGELOG.md
```

### 2. Release

```bash
# Merge to main
git checkout main
git merge release/v1.2.0

# Tag release
git tag -a v1.2.0 -m "Release version 1.2.0"
git push origin v1.2.0

# Auto-deploy via GitHub Actions
```

### 3. Post-Release

```bash
# Merge back to develop
git checkout develop
git merge main

# Monitor production
# - Check Databricks job logs
# - Verify data quality metrics
# - Monitor dashboards
```

---

## Performance Benchmarks

**Track these metrics over time:**

| Metric | Baseline | Target |
|--------|----------|--------|
| Bronze ingestion | 45s | <60s |
| Silver validation | 60s | <90s |
| Gold analytics | 90s | <120s |
| Total pipeline | 3m | <5m |
| Cost per run | $2 | <$5 |

---

## Best Practices

### Code Quality

1. **Follow PEP 8** - Python style guide
2. **Type hints** - Add to all function signatures
3. **Docstrings** - Document all public functions
4. **DRY principle** - Don't repeat yourself
5. **Small functions** - <50 lines each

### Git Commits

**Good commit messages:**
```
Add customer churn prediction model

- Implement logistic regression classifier
- Add feature engineering for recency/frequency
- Include unit tests with 85% coverage
```

**Bad commit messages:**
```
fixed stuff
updates
wip
```

### Code Reviews

**Review checklist:**
- [ ] Code follows style guide
- [ ] Tests added for new features
- [ ] Documentation updated
- [ ] No hardcoded secrets
- [ ] Performance considered
- [ ] Error handling adequate

---

## Tools & Technologies

| Category | Tool | Purpose |
|----------|------|---------|
| Version Control | Git, GitHub | Code management |
| CI/CD | GitHub Actions | Automation |
| Testing | pytest, pytest-cov | Test execution |
| Code Quality | Black, Flake8, MyPy | Linting, formatting |
| Security | Bandit, Safety | Vulnerability scanning |
| Deployment | Databricks CLI | Deploy to cloud |
| Monitoring | Databricks, Azure Monitor | Observability |