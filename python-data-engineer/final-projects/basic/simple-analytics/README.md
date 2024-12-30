# Simple Analytics Platform

A lightweight analytics platform for data ingestion, analysis, and visualization.

## Purpose
- Ingest data from multiple sources
- Perform basic analytics
- Generate automated reports
- Visualize insights through dashboards

## Features

### 1. Data Ingestion
- Multiple source support (CSV, Excel, API, Database)
- Automated ingestion scheduling
- Data validation and cleaning
- Incremental loading

### 2. Analytics Engine
- Basic statistical analysis
- Trend detection
- Aggregation pipelines
- Custom metrics calculation

### 3. Report Generation
- Automated PDF reports
- Email distribution
- Multiple templates
- Scheduled generation

### 4. Dashboard
- Real-time metrics
- Interactive charts
- Data filtering
- Export capabilities

## Database Structure

### 1. Fact Tables

```sql
-- Sales transactions
CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    product_id INTEGER,
    customer_id INTEGER,
    store_id INTEGER,
    sale_date DATE,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer interactions
CREATE TABLE fact_interactions (
    interaction_id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    channel_id INTEGER,
    interaction_type VARCHAR(50),
    interaction_date TIMESTAMP,
    duration INTEGER,
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 2. Dimension Tables

```sql
-- Products dimension
CREATE TABLE dim_products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    unit_cost DECIMAL(10,2),
    unit_price DECIMAL(10,2),
    supplier_id INTEGER,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customers dimension
CREATE TABLE dim_customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(50),
    country VARCHAR(50),
    segment VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Stores dimension
CREATE TABLE dim_stores (
    store_id SERIAL PRIMARY KEY,
    store_name VARCHAR(100),
    address TEXT,
    city VARCHAR(50),
    country VARCHAR(50),
    manager VARCHAR(100),
    open_date DATE,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Channels dimension
CREATE TABLE dim_channels (
    channel_id SERIAL PRIMARY KEY,
    channel_name VARCHAR(50),
    channel_type VARCHAR(20),
    description TEXT,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 3. Metrics Tables

```sql
-- Daily metrics
CREATE TABLE metrics_daily (
    metric_date DATE PRIMARY KEY,
    total_sales DECIMAL(12,2),
    total_transactions INTEGER,
    avg_transaction_value DECIMAL(10,2),
    total_customers INTEGER,
    new_customers INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product metrics
CREATE TABLE metrics_products (
    product_id INTEGER,
    metric_date DATE,
    units_sold INTEGER,
    revenue DECIMAL(10,2),
    profit DECIMAL(10,2),
    stock_level INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id, metric_date)
);
```

## Implementation Approach

### 1. Data Ingestion Layer
- Source connectors for different data types
- Validation rules and data cleaning
- Staging area for raw data
- Incremental loading logic

### 2. Analytics Processing
- ETL pipelines for metrics calculation
- Statistical analysis functions
- Aggregation workflows
- Caching mechanism for performance

### 3. Report Generation
- Template-based report generation
- PDF and Excel export
- Email distribution system
- Scheduling system

### 4. Web Dashboard
- FastAPI backend
- React frontend
- Real-time data updates
- Interactive visualizations

## Project Structure
```
simple-analytics/
├── src/
│   ├── ingestion/
│   │   ├── connectors/
│   │   ├── validators/
│   │   └── loaders/
│   ├── analytics/
│   │   ├── processors/
│   │   ├── calculators/
│   │   └── aggregators/
│   ├── reporting/
│   │   ├── templates/
│   │   ├── generators/
│   │   └── distributors/
│   ├── dashboard/
│   │   ├── backend/
│   │   └── frontend/
│   └── utils/
│       ├── config.py
│       ├── database.py
│       └── logging.py
├── config/
│   ├── sources.yml
│   ├── metrics.yml
│   └── reports.yml
├── tests/
│   ├── test_ingestion.py
│   ├── test_analytics.py
│   ├── test_reporting.py
│   └── test_api.py
├── docs/
│   ├── setup.md
│   ├── api.md
│   └── metrics.md
└── main.py
```

## Setup and Installation

1. Database Setup:
```bash
# Create database
createdb analytics

# Run migrations
python scripts/migrate.py
```

2. Dependencies:
```bash
pip install -r requirements.txt
```

3. Configuration:
```bash
# Copy example config
cp config/example.yml config/local.yml

# Edit configuration
vim config/local.yml
```

4. Run Platform:
```bash
# Start API server
python main.py server

# Run ingestion
python main.py ingest --source sales

# Generate reports
python main.py report --type daily
```

## Usage Examples

1. Data Ingestion:
```python
from src.ingestion import DataIngestion

# Initialize ingestion
ingestion = DataIngestion()

# Load sales data
ingestion.load_sales("data/sales.csv")
```

2. Analytics:
```python
from src.analytics import Analytics

# Initialize analytics
analytics = Analytics()

# Calculate daily metrics
metrics = analytics.calculate_daily_metrics()
```

3. Reporting:
```python
from src.reporting import ReportGenerator

# Initialize generator
generator = ReportGenerator()

# Generate daily report
generator.generate_daily_report()
```

## Contributing
1. Fork repository
2. Create feature branch
3. Commit changes
4. Push to branch
5. Create pull request

## License
MIT License 