# ETL Pipeline System

A robust ETL (Extract, Transform, Load) system that processes data from multiple sources into a data warehouse.

## Architecture

### 1. Data Sources
- **APIs**:
  - Weather API (OpenWeatherMap)
  - Stock Market API (Alpha Vantage)
  - Social Media API (Twitter)
- **Databases**:
  - MySQL (Customer data)
  - PostgreSQL (Transaction data)
- **Files**:
  - CSV files (Product catalog)
  - JSON files (User preferences)
  - Excel files (Sales reports)

### 2. ETL Process
- **Extract**:
  - API connectors with rate limiting
  - Database readers with connection pooling
  - File parsers with validation
- **Transform**:
  - Data cleaning (missing values, duplicates)
  - Data validation (schemas, constraints)
  - Data enrichment (joining, aggregating)
- **Load**:
  - Incremental loading
  - Batch processing
  - Error handling and retries

### 3. Data Warehouse
- **Schema**:
  ```sql
  -- Fact Tables
  fact_sales
  fact_transactions
  fact_weather_metrics
  
  -- Dimension Tables
  dim_customers
  dim_products
  dim_locations
  dim_dates
  ```

### 4. Monitoring
- Job status tracking
- Error logging
- Performance metrics
- Data quality checks

## Implementation Details

1. **Configuration**:
   ```yaml
   sources:
     apis:
       weather:
         url: "https://api.openweathermap.org/data/2.5"
         key: "${WEATHER_API_KEY}"
       stocks:
         url: "https://www.alphavantage.co/query"
         key: "${ALPHA_VANTAGE_KEY}"
     
     databases:
       mysql:
         host: "localhost"
         port: 3306
         database: "customers"
       postgres:
         host: "localhost"
         port: 5432
         database: "transactions"
     
     files:
       paths:
         products: "/data/products/*.csv"
         preferences: "/data/preferences/*.json"
         sales: "/data/sales/*.xlsx"
   
   warehouse:
     type: "postgresql"
     host: "localhost"
     port: 5432
     database: "warehouse"
   
   schedule:
     apis: "*/15 * * * *"  # Every 15 minutes
     databases: "0 * * * *"  # Hourly
     files: "0 0 * * *"  # Daily
   ```

2. **Data Quality Rules**:
   ```yaml
   validations:
     customers:
       - field: "email"
         type: "email"
         required: true
       - field: "phone"
         pattern: "^\+?[1-9]\d{1,14}$"
     
     transactions:
       - field: "amount"
         type: "decimal"
         min: 0
       - field: "date"
         type: "date"
         range: ["2020-01-01", "now"]
   ```

3. **Error Handling**:
   ```yaml
   retries:
     max_attempts: 3
     delay: 5  # seconds
   
   notifications:
     email:
       errors: ["admin@company.com"]
       warnings: ["team@company.com"]
     slack:
       channel: "#etl-alerts"
   ```

## Usage

1. **Setup**:
   ```bash
   # Install dependencies
   pip install -r requirements.txt
   
   # Configure environment
   cp .env.example .env
   
   # Initialize database
   python scripts/init_db.py
   ```

2. **Run Pipeline**:
   ```bash
   # Run specific extractor
   python etl.py extract --source weather
   
   # Run full pipeline
   python etl.py run-all
   
   # Schedule jobs
   python etl.py schedule
   ```

3. **Monitor**:
   ```bash
   # Check job status
   python etl.py status
   
   # View logs
   python etl.py logs --level ERROR
   
   # Generate report
   python etl.py report --date 2024-01-01
   ```

## Project Structure
```
etl_pipeline/
├── src/
│   ├── extractors/
│   │   ├── api.py
│   │   ├── database.py
│   │   └── file.py
│   ├── transformers/
│   │   ├── cleaner.py
│   │   ├── validator.py
│   │   └── enricher.py
│   ├── loaders/
│   │   ├── warehouse.py
│   │   └── staging.py
│   └── utils/
│       ├── config.py
│       ├── logging.py
│       └── monitoring.py
├── tests/
│   ├── test_extractors.py
│   ├── test_transformers.py
│   └── test_loaders.py
├── config/
│   ├── sources.yml
│   ├── validations.yml
│   └── schedule.yml
├── scripts/
│   ├── init_db.py
│   └── generate_report.py
└── requirements.txt
```

## Testing
```bash
# Run unit tests
pytest tests/

# Run integration tests
pytest tests/ --integration

# Generate coverage report
pytest --cov=src tests/
```

## Monitoring Dashboard
- Job Status Overview
- Data Quality Metrics
- Processing Times
- Error Rates
- Resource Usage

## Documentation
- API Documentation
- Database Schema
- Data Dictionary
- Troubleshooting Guide

## Run all pipelines
```bash
python main.py run --mode all
```

# Run specific pipeline
```bash
# Run specific pipeline
python main.py run --mode customers
```

```bash
python main.py run --mode transactions  
```

# Start scheduler
```bash
python main.py schedule
```

## Set environment variables
```bash
WEATHER_API_KEY=your_weather_api_key
ALPHA_VANTAGE_KEY=your_alpha_vantage_key
MYSQL_PASSWORD=your_mysql_password
POSTGRES_PASSWORD=your_postgres_password
WAREHOUSE_PASSWORD=your_warehouse_password
```