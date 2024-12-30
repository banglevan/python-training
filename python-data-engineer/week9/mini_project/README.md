# Analytics Platform

A production-ready analytics platform that integrates multiple data sources and provides real-time visualizations.

## Features

- Multiple data source integration (PostgreSQL, MongoDB, Kafka)
- Real-time data processing
- Custom visualization components
- User authentication and authorization
- API endpoints for data access
- Configurable dashboards
- Alert system

## Architecture

```
analytics_platform/
├── src/
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config.py        # Configuration management
│   │   ├── database.py      # Database connections
│   │   └── security.py      # Authentication & authorization
│   ├── data/
│   │   ├── __init__.py
│   │   ├── sources.py       # Data source connectors
│   │   ├── processors.py    # Data processing logic
│   │   └── cache.py        # Data caching
│   ├── viz/
│   │   ├── __init__.py
│   │   ├── charts.py       # Chart components
│   │   ├── dashboards.py   # Dashboard management
│   │   └── themes.py       # Visual themes
│   └── api/
│       ├── __init__.py
│       ├── routes.py       # API endpoints
│       └── schemas.py      # Data schemas
├── config/
│   └── config.yml         # Configuration file
├── tests/
│   ├── __init__.py
│   ├── test_core.py
│   ├── test_data.py
│   └── test_viz.py
├── requirements.txt
└── main.py               # Application entry point
```

## Database Structure

### PostgreSQL Tables

```sql
-- Users and Authentication
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data Sources
CREATE TABLE data_sources (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    type VARCHAR(20) NOT NULL,
    config JSONB NOT NULL,
    enabled BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dashboards
CREATE TABLE dashboards (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    layout JSONB NOT NULL,
    owner_id INTEGER REFERENCES users(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Visualizations
CREATE TABLE visualizations (
    id SERIAL PRIMARY KEY,
    dashboard_id INTEGER REFERENCES dashboards(id),
    type VARCHAR(50) NOT NULL,
    config JSONB NOT NULL,
    data_source_id INTEGER REFERENCES data_sources(id),
    position JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Alerts
CREATE TABLE alerts (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    condition JSONB NOT NULL,
    visualization_id INTEGER REFERENCES visualizations(id),
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure database connections in `config/config.yml`

3. Initialize database:
```bash
python -m src.core.database init
```

4. Start the application:
```bash
python main.py
```

## Usage

1. **Authentication**:
```bash
curl -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "user", "password": "pass"}'
```

2. **Create Dashboard**:
```bash
curl -X POST http://localhost:8000/api/dashboards \
  -H "Authorization: Bearer {token}" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Sales Overview",
    "layout": {
      "type": "grid",
      "columns": 2
    }
  }'
```

3. **Add Visualization**:
```bash
curl -X POST http://localhost:8000/api/visualizations \
  -H "Authorization: Bearer {token}" \
  -H "Content-Type: application/json" \
  -d '{
    "dashboard_id": 1,
    "type": "line_chart",
    "config": {
      "title": "Sales Trend",
      "x_axis": "date",
      "y_axis": "amount"
    }
  }'
```

## API Documentation

Full API documentation is available at `/api/docs` when running the application.

## Development

1. Run tests:
```bash
pytest tests/
```

2. Format code:
```bash
black src/ tests/
```

3. Check types:
```bash
mypy src/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit changes
4. Create a pull request

## License

MIT License 