# Lufthansa Data Intelligence Platform

A production-grade data intelligence platform built on Databricks lakehouse architecture that ingests, transforms, and analyzes Lufthansa operational and reference data using modern data engineering best practices.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Setup & Installation](#setup--installation)
- [First Run Setup](#first-run-setup)
- [Data Pipeline](#data-pipeline)
- [Running the Project](#running-the-project)
- [Documentation](#documentation)

## Overview

This project demonstrates end-to-end data intelligence delivery using the medallion architecture (Bronze → Silver → Gold). It ingests Lufthansa OpenAPI data (flight status, airports, airlines, aircraft, etc.), transforms it into analytics-ready datasets, and exposes insights through dashboards.

### Key Features

- ✅ **Automated Ingestion**: Daily/monthly scheduled pipelines for operational and reference data
- ✅ **Data Quality**: Validation rules, SCD Type 2 dimension handling, error quarantine tables
- ✅ **Medallion Architecture**: Raw → Cleaned → Analytics-ready layers
- ✅ **Infrastructure as Code**: Databricks Asset Bundles for reproducible deployments
- ✅ **Production Ready**: Comprehensive logging, error handling, and documentation

## Architecture

### Medallion Architecture Pattern

```
┌─────────────────────────────────────────────────┐
│  BRONZE (Raw)                                   │
│  └─ AutoLoader ingestion from APIs              │
│  └─ Raw JSON preservation                       │
│  └─ Ingest metadata (timestamps, run IDs)       │
└────────────────┬────────────────────────────────┘
                 │
┌────────────────┴─────────────────────────────────┐
│  SILVER (Clean)                                  │
│  └─ Data validation & quality checks             │
│  └─ SCD Type 2 for reference dimensions          │
│  └─ Deduplication & standardization              │
│  └─ Error quarantine tables                      │
└────────────────┬─────────────────────────────────┘
                 │
┌────────────────┴─────────────────────────────────┐
│  GOLD (Analytics)                                │
│  └─ Aggregated operational metrics               │
│  └─ Business logic applied                       │
│  └─ Optimized for dashboard consumption          │
└──────────────────────────────────────────────────┘
```

### Data Sources

- **Reference Data** (Monthly): Countries, Cities, Airports, Airlines, Aircraft
- **Operations Data** (Daily): Flight Status by Route

## Prerequisites

- Databricks workspace (Free Edition or higher)
- Lufthansa OpenAPI credentials (or use [LH Proxy](LH-proxy.md))
- Python 3.10+
- Databricks CLI configured
- Git for version control

### Optional Dependencies

- `pytest` - Testing
- `ruff` - Linting
- `databricks-dlt` - DLT development
- `databricks-connect` - Local development

## Project Structure

```
.
├── README.md                          # This file
├── pyproject.toml                     # Python dependencies
├── databricks.yml                     # Databricks Bundle config
│
├── src/
│   ├── app/                          # Application initialization
│   │   ├── clients.py                # HTTP client factory
│   │   ├── init_app.py               # App initialization
│   │   ├── logger.py                 # Logging configuration
│   │   ├── runtime.py                # Runtime utilities
│   │   └── secrets.py                # Secret management
│   │
│   ├── client/                       # API clients
│   │   ├── base_http_client.py       # Base HTTP client
│   │   ├── lufthansa_oauth_client.py # OAuth client
│   │   └── lufthansa_proxy_client.py # Proxy client
│   │
│   ├── config/                       # Configuration
│   │   ├── config_properties.py      # Config loading
│   │   ├── endpoints.py              # Endpoint definitions
│   │   └── resources/                # YAML configs
│   │
│   ├── services/
│   │   ├── ingestion_service.py      # Data ingestion logic
│   │   ├── storage_service.py        # File storage utilities
│   │   ├── parsing_schemas.py        # JSON schema definitions
│   │   └── scd/                      # Slowly Changing Dimensions
│   │       ├── operations/           # Flight status SCD
│   │       ├── references/           # Dimension SCD (airlines, airports, etc.)
│   │       └── utils/                # Validation rules
│   │
│   ├── util/                         # Utilities
│   │   ├── json_utils.py             # JSON navigation
│   │   └── tables_utils.py           # Table naming
│   │
│   ├── 0_init/                       # Initialization jobs
│   │   ├── setup_catalog.py
│   │   └── load_references.py
│   │
│   ├── 1_bronze/                     # Ingestion layer
│   │   ├── daily_ingestion_job.py
│   │   ├── monthly_references_ingestion.py
│   │   ├── operations_autoloader.py
│   │   └── references_autoloader.py
│   │
│   ├── 2_silver/                     # Transformation layer
│   │   └── references_cdc_job.py
│   │
│   └── 3_gold/                       # Analytics layer
│       └── operations_analytics_job_dlt.py
│
└── resources/
    ├── jobs/                         # Job definitions
    ├── pipelines/                    # Pipeline definitions
    └── (config files - see src/config/resources)

```

## Setup & Installation

### 1. Clone Repository

```bash
git clone https://github.com/ybutkov/LEVEL3-Data-Intelligence.git
```

### 2. Configure Databricks CLI

```bash
databricks configure --token
# Enter your Databricks host and personal access token
```

### 3. Set Secrets

Create Databricks secret scope for Lufthansa API credentials:

```bash
databricks secrets create-scope --scope lufthansa

# Store OAuth credentials
databricks secrets put --scope lufthansa --key client_id --string-value "YOUR_CLIENT_ID"
databricks secrets put --scope lufthansa --key client_secret --string-value "YOUR_CLIENT_SECRET"

# Or store proxy password if using LH Proxy
databricks secrets put --scope lufthansa --key proxy_password --string-value "YOUR_PROXY_PASSWORD"
```

### 4. Update Configuration

Edit `src/config/resources/application-<profile>.yaml`:

```yaml
api:
  base_url: "https://api.lufthansa.com"
  version: "v1"
  url_oauth_token: "https://api.lufthansa.com/v1/oauth/token"

storage:
  catalog: "lufthansa_level"
  bronze_schema: "bronze"
  silver_schema: "silver"
  gold_schema: "gold"

secrets:
  secret_scope: "lufthansa"
  oauth_token_client_id: "client_id"
  oauth_token_client_secret: "client_secret"
```

### 5. Deploy Bundle

```bash
databricks bundle deploy --profile <profile>
```

## First Run Setup

**Important**: Follow these steps in order for first-time setup.

### Step 1: Initialize Catalog & Schemas

The initialization job creates the complete catalog structure and loads reference dimension data (flight status codes, time status codes).

```bash
# Run the initialization job
databricks jobs run-now --job-id $(databricks jobs list --filter 'name="Lufthansa Initialization"' | grep lufthansa_init_setup | awk '{print $1}')

# Or manually via Databricks UI:
# 1. Go to "Workflows" in sidebar
# 2. Find "Lufthansa Initialization" job
# 3. Click "Run now"
# 4. Wait for completion (should take < 5 minutes)
```

**What this does**:
- ✓ Creates `lufthansa_level` catalog
- ✓ Creates schemas: `bronze`, `silver`, `silver_audit`, `gold`
- ✓ Creates volumes: `landing_area`, `autoloader_metadata`
- ✓ Loads reference dimension tables:
  - `dim_time_status` (ON, DL, AR, BD, etc.)
  - `dim_flight_status` (SC, DP, LND, CA, etc.)

**Verify success**:
```sql
-- Check catalog
SHOW CATALOGS LIKE 'lufthansa_level';

-- Check schemas
SHOW SCHEMAS IN lufthansa_level;

-- Check reference dimensions loaded
SELECT * FROM lufthansa_level.silver.dim_flight_status;
SELECT * FROM lufthansa_level.silver.dim_time_status;
```

### Step 2: Verify Secrets Configuration

Ensure all required secrets are stored:

```bash
# List secrets in lufthansa scope
databricks secrets list --scope lufthansa

# Should show:
# client_id
# client_secret
# (or proxy_password if using LH Proxy)
```

### Step 3: Run Initial Data Ingestion

After initialization succeeds, manually trigger the daily ingestion job to load reference data:

```bash
# Find and run the daily operational job
databricks jobs run-now --job-id $(databricks jobs list --filter 'name="Lufthansa daily operational Job"' | grep lufthansa_daily_workflow | awk '{print $1}')
```

**This will**:
1. Fetch operational data from Lufthansa API
2. Store raw JSON in `landing_area` volume
3. Auto Loader processes and lands in bronze tables
4. Silver layer validates and cleans
5. Gold layer creates analytics tables

**Expected duration**: 5-15 minutes depending on data volume

### Step 4: Verify Data Loaded

```sql
-- Check raw data in volumes
SELECT * FROM volume_files('lufthansa_level.bronze.landing_area');

-- Check bronze tables
SELECT COUNT(*) FROM lufthansa_level.bronze.flightstatus_by_route_raw;
SELECT COUNT(*) FROM lufthansa_level.bronze.aircraft_raw;
SELECT COUNT(*) FROM lufthansa_level.bronze.airports_raw;

-- Check silver tables
SELECT COUNT(*) FROM lufthansa_level.silver.fact_flight_status;
SELECT COUNT(*) FROM lufthansa_level.silver.ref_dim_aircraft;

-- Check gold tables
SELECT COUNT(*) FROM lufthansa_level.gold.fact_flight_analytics;
```

### Step 5: Schedule Recurring Jobs

Once verified, jobs are automatically scheduled per `resources/jobs/*.yml`:

| Job | Frequency | Time |
|-----|-----------|------|
| Lufthansa Initialization | One-time | Manual |
| Daily Operational | Daily | 10:30 AM CET |
| Monthly References | Monthly | 1st of month, 2:00 AM CET |

To pause/unpause scheduling:

```bash
# Pause job
databricks jobs update --job-id <job-id> --pause-status PAUSED

# Resume job
databricks jobs update --job-id <job-id> --pause-status UNPAUSED
```

## Complete First-Run Checklist

- [ ] Databricks workspace created
- [ ] CLI configured (`databricks configure --token`)
- [ ] Secrets stored (`databricks secrets put ...`)
- [ ] Configuration updated (`application-dev.yaml`)
- [ ] Bundle deployed (`databricks bundle deploy`)
- [ ] Initialization job run and succeeded
- [ ] Reference dimensions verified
- [ ] Daily ingestion job executed
- [ ] Data verified in all layers
- [ ] Scheduled jobs enabled

## Data Pipeline

## Technologies Used

- Databricks Jobs and Databricks Asset Bundles
  - Jobs orchestrate fetch and pipeline tasks.
  - Bundle configuration in `databricks.yml` manages environments and deployment.

- Request-based ingestion (API fetch layer)
  - Python ingestion jobs call Lufthansa endpoints per request and persist raw JSON into Unity Catalog volumes.
  - Fetching includes retry with backoff for temporary errors, fail-fast for permanent errors, and request/response validation.

- Auto Loader (cloudFiles)
  - Bronze ingestion uses Structured Streaming with `cloudFiles` and `binaryFile` format.
  - Schema tracking/checkpoint metadata is stored in the configured metadata volume.
  - New landed files are incrementally discovered and loaded to bronze tables.

- DLT/SDP (Spark Declarative Pipelines)
  - Pipelines are defined with `pyspark.pipelines as dp` using declarative constructs such as `@dp.table`, `@dp.view`, `@dp.materialized_view`, and flows.
  - Operational pipeline processes Bronze -> Silver -> Gold.
  - Reference pipeline processes Bronze -> Silver dimensions.

- CDC and SCD modeling
  - `dp.create_auto_cdc_flow(..., stored_as_scd_type=1)` is used for current-state tables (overwrite latest values by key).
  - `dp.create_auto_cdc_flow(..., stored_as_scd_type=2)` is used for history-preserving reference dimensions.
  - In this project, operational facts are handled with SCD Type 1, while reference dimensions use SCD Type 1/2 patterns depending on table purpose.

- Data quality and auditability
  - Validation rules are applied before silver publication.
  - Invalid records are routed to silver audit/quarantine tables for troubleshooting.
  - Ingestion metadata columns (source file, ingest timestamp, run id) support traceability.

### Ingestion Flow (Bronze Layer)

```
Lufthansa APIs → HTTP Client → Validation → Storage Service → Unity Catalog Volumes
      ↓              ↓                ↓              ↓                    ↓
  Reference     OAuth/Proxy        JSON Check    Atomic Write        Raw JSON
  Operations    Request            Structure     w/ Metadata         Files
```

#### Daily Jobs

- **Flight Status Ingestion**: Fetches flight status from three endpoints:
  - By Route
  - By Departure Airport  
  - By Arrival Airport

#### Monthly Jobs

- **Reference Data**: Countries, Cities, Airports, Airlines, Aircraft

### Transformation Flow (Silver → Gold)

1. **Silver Layer**:
   - Parse JSON with schema validation
   - Remove duplicates
   - Handle NULL values
   - Apply SCD Type 2 for reference dimensions
   - Route invalid records to error tables

2. **Gold Layer**:
   - Aggregate flight metrics by hour
   - Calculate on-time performance
   - Denormalize for dashboard consumption

### Validation Rules

See [src/services/scd/utils/rules.py](src/services/scd/utils/rules.py) for comprehensive validation rules applied to each entity type.

## Running the Project

### Deploy All Resources

```bash
databricks bundle deploy
```

This creates all jobs and pipelines defined in `resources/` but does NOT automatically run them.

### One-Time Initialization

**First time only** - Run after initial deploy:

```bash
# Option 1: Using Databricks CLI
databricks jobs run-now --job-id $(databricks jobs get-by-name --name "Lufthansa Initialization" | jq .job_id)

# Option 2: Via Databricks UI
# 1. Navigate to "Workflows" → "Jobs"
# 2. Find "Lufthansa Initialization"
# 3. Click "Run now" button
# 4. Monitor execution
```

Monitor progress:
```bash
# Watch logs in real-time
databricks jobs run-now --job-id <job-id> --watch
```

### Running Daily/Monthly Jobs

#### Manual Trigger

```bash
# Run daily operational job
databricks jobs run-now --job-id <daily-job-id>

# Run monthly references job  
databricks jobs run-now --job-id <monthly-job-id>
```

#### View Job Status

```bash
# List all jobs
databricks jobs list --filter 'name like "Lufthansa%"'

# Get specific job details
databricks jobs get --job-id <job-id>

# List recent runs
databricks jobs list-runs --job-id <job-id> --limit 5

# Get run details and logs
databricks jobs get-run --run-id <run-id>
```

#### Monitor Dashboard

Access via Databricks UI:
1. Workflows → Jobs
2. Click job name to see execution history
3. View logs, execution time, data volumes
4. Check for failures or warnings

### Data Inspection Queries

After first run, verify data flow:

```sql
-- 1. Check raw volume files
SELECT 
  name,
  size,
  creation_time 
FROM volume_files('lufthansa_level.bronze.landing_area')
ORDER BY creation_time DESC;

-- 2. Count records by layer
SELECT 
  'Bronze' as layer,
  COUNT(*) as record_count,
  MAX(bronze_ingested_at) as latest_load
FROM lufthansa_level.bronze.flightstatus_by_route_raw

UNION ALL

SELECT 
  'Silver' as layer,
  COUNT(*),
  MAX(dbt_valid_from)
FROM lufthansa_level.silver.fact_flight_status

UNION ALL

SELECT 
  'Gold' as layer,
  COUNT(*),
  MAX(analytics_date)
FROM lufthansa_level.gold.fact_flight_analytics;

-- 3. Check data quality (errors)
SELECT 
  table_name,
  error_type,
  COUNT(*) as error_count,
  MAX(error_timestamp) as latest_error
FROM lufthansa_level.silver_audit.error_quarantine
GROUP BY table_name, error_type
ORDER BY error_count DESC;

-- 4. Sample flight data
SELECT 
  flight_key,
  departure_time,
  arrival_time,
  aircraft_type,
  status
FROM lufthansa_level.gold.fact_flight_analytics
WHERE analytics_date = CURRENT_DATE()
LIMIT 10;
```

### Deploy Specific Components

```bash
# Deploy only jobs (not pipelines)
databricks bundle deploy --target resources.jobs

# Deploy specific job
databricks bundle deploy --target resources.jobs.lufthansa_init_setup

# Deploy pipelines
databricks bundle deploy --target resources.pipelines
```

### Troubleshooting Runs

```bash
# Check job status
databricks runs get --run-id <run-id>

# Get detailed logs
databricks runs get-output --run-id <run-id>

# Check for failures
databricks runs list --job-id <job-id> --completed-only | grep -i failed

# Cancel a stuck run
databricks runs cancel --run-id <run-id>
```

## Key Components

### Ingestion Service

Core logic for fetching data with retry strategy:

```python
from src.services.ingestion_service import fetch_data

response = fetch_data(
    url="https://api.lufthansa.com/v1/...",
    query_params={"param": "value"},
    max_retries=5,
    timeout=20
)
```

**Features**:
- Exponential backoff retry logic
- Automatic error classification (retryable vs non-retryable)
- Comprehensive logging
- Connection timeout handling

How it works (high-level):
- Send API request.
- If the error is temporary, retry with increasing wait time between attempts.
- If the error is permanent, fail fast and log the reason.
- If response is valid, save JSON to storage and continue pipeline processing.

### Configuration Management

Dynamic configuration loading from YAML with profile-specific overrides:

```python
from src.config.config_properties import get_ConfigProperties

config = get_ConfigProperties()
api_url = config.api.base_url
```

### Logging

Colored console logging with file path shortening and millisecond timestamps:

```python
from src.app.logger import get_logger

logger = get_logger(__name__)
logger.info("Processing flight data")
logger.error("Failed to parse response", exc_info=True)
```

## Data Models

## Jobs And Pipelines

Main scheduled jobs:
- `lufthansa_daily_workflow` (daily operational data)
- `monthly_references_ingestion` (monthly reference data)

General flow for both jobs:
1. Fetch step (Python job task): call Lufthansa API endpoints and save raw JSON to Unity Catalog volume (`landing_area`).
2. Pipeline step (DLT pipeline task): process landed files with Auto Loader and transform data through medallion layers.

What each job runs:

1. `lufthansa_daily_workflow`
   - Fetch task: `src/1_bronze/daily_ingestion_job.py`
   - Then pipeline: `operations_pipeline`
   - Pipeline stages:
     - `src/1_bronze/operations_autoloader.py` (files -> bronze tables)
     - `src/services/scd/operations/flight_status_scd.py` (bronze -> silver facts + audit)
     - `src/3_gold/operations_analytics_job_dlt.py` (silver -> gold analytics)

2. `monthly_references_ingestion`
   - Fetch task: `src/1_bronze/monthly_references_ingeston.py`
   - Then pipeline: `references_pipeline`
   - Pipeline stages:
     - `src/1_bronze/references_autoloader.py` (files -> bronze tables)
     - `src/2_silver/references_cdc_job.py` (bronze -> silver reference dimensions)

In short: each main job first fetches data, then runs a pipeline to transform it. Daily job ends in Gold metrics; monthly job refreshes Silver reference dimensions.

### Reference Dimensions (SCD Type 2)

- **Airports**: airport_code, city_code, country_code, lat/lon, location_type
- **Airlines**: airline_code, airline_name, country
- **Aircraft**: aircraft_code, aircraft_name, manufacturer
- **Countries**: country_code, country_name (multi-lingual)
- **Cities**: city_code, country_code, city_name (multi-lingual)

### Fact Tables

- **Flight Status**: flight_key, departure_time, arrival_time, status, aircraft_type
- **Flight Schedules**: scheduled_departure, scheduled_arrival, aircraft_type

## Performance & Optimization

- **Auto Loader**: Incremental ingestion prevents reprocessing
- **Delta Lake**: Transactional guarantees and ACID compliance
- **SCD Type 2**: Efficient dimension history tracking
- **Error Quarantine**: Failed records separated for analysis

## Troubleshooting

### Authentication Issues

```bash
# Verify secret scope
databricks secrets list-scopes

# Check if secrets exist
databricks secrets list --scope lufthansa
```

### Data Quality Issues

Check error tables in silver layer:
```sql
SELECT * FROM lufthansa_level.silver_audit.err_* 
ORDER BY ingested_at DESC
```

### Pipeline Failures

1. Check job logs in Databricks UI
2. Verify configuration in `application-dev.yaml`
3. Test API connectivity manually
4. Check for rate limiting (429 responses)

## Monitoring & Observability

- **Logs**: Structured logging with timestamps and module names
- **Execution Logs**: Stored with each ingestion run (metadata columns: `source_file`, `bronze_ingested_at`, `ingest_run_id`)
- **Data Quality Metrics**: Validation rule results in audit tables
- **Dashboard**: Real-time operations analytics in Databricks SQL

## CI/CD & Deployment

This project uses **Databricks Asset Bundles** for infrastructure-as-code:

```bash
# Deploy to dev environment
databricks bundle deploy --profile dev

# Deploy to production (requires separate prod workspace)
databricks bundle deploy --profile prod
```

Configuration is in `databricks.yml` with environment-specific settings.

## Development

### Running Tests

```bash
pytest tests/
```

### Code Style

```bash
ruff check src/
ruff format src/
```

## Documentation

- [Task Requirements](Task) - Original project specifications
- [Lufthansa API Docs](https://developer.lufthansa.com/docs) - API reference
- [Databricks Lakehouse Documentation](https://docs.databricks.com) - Platform reference
- [Medallion Architecture](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion) - Design pattern

## Code Documentation

All major functions and classes include comprehensive docstrings. Key entry points:

- [src/app/init_app.py](src/app/init_app.py) - Application initialization
- [src/services/ingestion_service.py](src/services/ingestion_service.py) - Data ingestion logic
- [src/services/scd/](src/services/scd/) - Transformation pipelines with detailed comments

## Contributing

1. Create feature branch: `git checkout -b feature/your-feature`
2. Make changes with clear commit messages
3. Test locally: `pytest tests/`
4. Push and create merge request
5. Deploy to dev workspace: `databricks bundle deploy --profile dev`

## Support

For issues or questions:
1. Check [Troubleshooting](#troubleshooting) section
2. Review Databricks documentation
3. Check logs in job runs
4. Open an issue in repository

## License

Project for educational purposes - Lufthansa Data Intelligence Platform

