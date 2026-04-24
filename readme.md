# Pharma Forecast Pipeline

An end-to-end data engineering pipeline for pharmaceutical sales forecasting,
built with PySpark, Prophet, and Apache Airflow.

## Overview

This pipeline processes synthetic pharma sales data for two product categories
— ANTIVEGF (EYLEA, LUCENTIS, BEOVU) and MS (AVONEX, REBIF, TECFIDERA, OCREVUS)
— and generates 6-month revenue forecasts using Facebook Prophet.

## Architecture

Follows the **Medallion Architecture** pattern:

- **Bronze** — raw data ingested as-is, partitioned by product category
- **Silver** — cleaned and validated data (null removal, deduplication, negative revenue filter, standardization)
- **Gold** — aggregated business-ready tables (product monthly, team monthly)
- **Forecasting** — Prophet time-series model trained per product, predicts next 6 months

## Tech Stack

| Tool | Purpose |
|---|---|
| PySpark 4.0 | Distributed data processing |
| Prophet | Time-series forecasting |
| Apache Airflow | Pipeline orchestration and scheduling |
| pytest | Unit testing |
| GitHub Actions | CI/CD |
| Python 3.11 | Core language |

## Project Structure

pharma_pipeline/
├── config/
│   ├── settings.py              # paths and constants
│   └── spark_session.py         # shared Spark session
├── src/
│   ├── ingestion/
│   │   ├── generate_data.py     # synthetic data generation
│   │   └── bronze_ingestion.py  # raw CSV to parquet
│   ├── transforms/
│   │   ├── silver_transform.py  # cleaning and validation
│   │   └── gold_transform.py    # aggregations
│   └── ml/
│       └── forecasting.py       # Prophet forecasting
├── dags/
│   └── pharma_pipeline_dag.py   # Airflow DAG
├── tests/
│   └── test_silver_transform.py # pytest unit tests
├── main.py                      # single entry point
└── .github/
└── workflows/
└── ci.yml               # GitHub Actions CI

## Pipeline DAG
Scheduled daily at 6am UTC. Each task retries twice on failure with a 5 minute delay.

## Running Locally

Clone the repo and set up environment:

```bash
git clone https://github.com/6kr9/pharma-forecast-pipeline.git
cd pharma-forecast-pipeline

py -3.11 -m venv venv311
venv311\Scripts\activate

pip install pyspark==4.0.0 prophet pandas faker openpyxl pytest
```

Run the full pipeline:

```bash
python main.py
```

Run tests:

```bash
pytest tests/ -v
```

## Data Quality Checks (Silver Layer)

| Check | Logic |
|---|---|
| Null handling | Drop records where record_id, sale_date, product, or revenue is null |
| Deduplication | Remove duplicate record_ids, keep first occurrence |
| Revenue filter | Remove records where revenue is zero or negative |
| Date filter | Remove records with future sale dates |
| Standardization | Uppercase and trim all string fields |

## Medallion Architecture

### Bronze
- Reads raw CSV with explicit schema (no schema inference)
- Adds ingestion metadata columns (ingested_at, source_file, pipeline_version)
- Partitioned by product category for downstream predicate pushdown

### Silver
- Applies all data quality checks listed above
- Casts sale_date from string to DateType
- Derives sale_year, sale_month, sale_quarter, avg_unit_price
- Partitioned by category and sale_year

### Gold
Two aggregated tables written to separate parquet paths:

**product_monthly** — grouped by year, month, category, product, subject_area
**team_monthly** — grouped by year, month, sales_team, category

Both tables include total_revenue, total_units, record_count, avg_unit_price.

## Forecasting

Prophet model trained independently per product on 24 months of historical data.

- yearly_seasonality enabled
- weekly and daily seasonality disabled (monthly data)
- 95% confidence intervals on all predictions
- Output covers both historical fit and 6-month future forecast

Sample output:
## Orchestration

Apache Airflow DAG with 5 tasks:

```python
generate_data >> bronze_ingestion >> silver_transform >> gold_transform >> ml_forecasting
```

- Schedule: daily at 6am UTC
- Retries: 2 attempts per task with 5 minute delay
- catchup=False to prevent backfill on deployment

In production this would be deployed to AWS MWAA or a Linux-based Airflow cluster
with data stored in S3 instead of local filesystem.

## CI/CD

Every push to main triggers GitHub Actions:

1. Spins up Ubuntu with Python 3.11 and Java 17
2. Installs all dependencies
3. Runs full pytest suite
4. Reports pass/fail status

[![CI](https://github.com/6kr9/pharma-forecast-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/6kr9/pharma-forecast-pipeline/actions/workflows/ci.yml)

## Key Technical Decisions

**Why Medallion Architecture?** Each layer serves a distinct purpose — Bronze preserves raw data for reprocessing, Silver ensures data quality, Gold optimizes for query performance. Failures at any layer don't corrupt upstream data.

**Why partition by category?** Downstream queries almost always filter by product category. Partitioning enables predicate pushdown — Spark skips irrelevant partitions entirely instead of scanning all data.

**Why Prophet over other models?** Prophet handles pharma sales patterns well — it captures yearly seasonality (Q1 formulary resets, Q4 budget spending) without requiring large training datasets. Our 24 months of data is sufficient.

**Why explicit schema in Bronze?** Schema inference reads the entire file to determine types. Explicit schema is faster, predictable, and fails loudly on unexpected data rather than silently inferring wrong types.