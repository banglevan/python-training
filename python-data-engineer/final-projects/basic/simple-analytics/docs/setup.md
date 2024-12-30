# Setup Guide

## Prerequisites
- Python 3.8+
- PostgreSQL 12+
- Node.js 14+ (for dashboard)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/simple-analytics.git
cd simple-analytics
```

2. Create and activate virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
.\venv\Scripts\activate  # Windows
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

5. Initialize database:
```bash
python scripts/init_db.py
```

## Configuration

1. Update config files in `config/` directory:
   - `sources.yml`: Data source configurations
   - `metrics.yml`: Metrics definitions
   - `reports.yml`: Report templates and schedules

2. Configure data sources in `sources.yml`:
```yaml
warehouse:
  driver: postgresql
  host: localhost
  port: 5432
  database: analytics
  username: "${DB_USERNAME}"
  password: "${DB_PASSWORD}"
```

## Running the Application

1. Start the API server:
```bash
python main.py server
```

2. Start the dashboard (development):
```bash
cd src/dashboard/frontend
npm install
npm start
```

## Development

1. Run tests:
```bash
pytest tests/
```

2. Generate documentation:
```bash
pdoc --html src/
```

## Deployment

1. Build frontend:
```bash
cd src/dashboard/frontend
npm run build
```

2. Deploy API:
```bash
gunicorn main:app --workers 4 --bind 0.0.0.0:8000
``` 