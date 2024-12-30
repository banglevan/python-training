# ML Feature Platform

A scalable feature engineering and serving platform for machine learning applications.

## Architecture Overview

```
                                     ┌──────────────┐
                                     │              │
                              ┌─────►│  Feature     │
┌──────────────┐             │       │  Registry    │
│              │             │       │  (MLflow)    │
│  Data        │             │       │              │
│  Sources     ├─────┐       │       └──────────────┘
│  (PostgreSQL)│     │       │
│              │     │       │      ┌──────────────┐
└──────────────┘     │       │      │              │
                     ▼       │      │  Feature     │
┌──────────────┐   ┌─────────────┐  │  Store       │
│              │   │             ├──►│  (Feast)    │
│  Streaming   ├──►│  Feature    │  │              │
│  Data        │   │  Pipeline   │  └──────────────┘
│  (Kafka)     │   │  (Airflow)  │
│              │   │             │      ┌──────────────┐
└──────────────┘   └─────────────┘      │              │
                          │             │  Online      │
                          │             │  Store       │
                          └────────────►│  (Redis)     │
                                        │              │
                                        └──────────────┘
```

## Project Structure
```
ml-feature-platform/
├── src/
│   ├── data/
│   │   ├── sources/
│   │   │   ├── postgresql.py    # PostgreSQL data source
│   │   │   └── kafka.py         # Kafka data source
│   │   └── sinks/
│   │       ├── redis.py         # Redis online store
│   │       └── feast.py         # Feast feature store
│   ├── features/
│   │   ├── definitions/         # Feature definitions
│   │   ├── transformations/     # Feature transformations
│   │   └── validation/          # Feature validation
│   ├── pipelines/
│   │   ├── batch/              # Batch processing DAGs
│   │   └── streaming/          # Streaming processors
│   ├── serving/
│   │   ├── api/                # Feature serving API
│   │   └── client/             # Feature client SDK
│   └── monitoring/
│       ├── metrics/            # Feature metrics
│       └── alerts/             # Monitoring alerts
├── config/
│   ├── feast/                  # Feast configurations
│   ├── airflow/                # Airflow DAG configs
│   ├── mlflow/                 # MLflow configs
│   └── monitoring/             # Monitoring configs
├── tests/
│   ├── unit/                   # Unit tests
│   ├── integration/            # Integration tests
│   └── performance/            # Performance tests
├── airflow/
│   └── dags/                   # Airflow DAG definitions
├── notebooks/                  # Example notebooks
└── docker/
    └── docker-compose.yml      # Docker services
```

## Features & Capabilities

1. **Feature Engineering**
   - Batch and streaming feature computation
   - Complex feature transformations
   - Feature validation and testing
   - Point-in-time correctness

2. **Feature Storage & Serving**
   - Online/offline feature serving
   - Feature versioning
   - Feature sharing and reuse
   - Access control

3. **Monitoring & Operations**
   - Feature drift detection
   - Data quality monitoring
   - Service health metrics
   - Alert management

## Tech Stack

- **Feature Store**: Feast
- **Workflow Orchestration**: Apache Airflow
- **Online Store**: Redis
- **Offline Store**: PostgreSQL
- **Model Registry**: MLflow
- **Monitoring**: Prometheus + Grafana

## Setup & Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/ml-feature-platform.git
cd ml-feature-platform
```

2. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configurations
```

3. Start the platform:
```bash
docker-compose up -d
```

## Usage Examples

1. **Define Features**
```python
from feast import Entity, Feature, FeatureView, ValueType

# Define entity
customer = Entity(
    name="customer",
    value_type=ValueType.INT64,
    description="Customer ID"
)

# Define feature view
customer_features = FeatureView(
    name="customer_features",
    entities=["customer"],
    ttl=timedelta(days=1),
    features=[
        Feature(name="total_purchases", dtype=ValueType.FLOAT),
        Feature(name="avg_order_value", dtype=ValueType.FLOAT),
    ]
)
```

2. **Compute Features**
```python
# Via Airflow DAG
from airflow import DAG
from airflow.operators.python import PythonOperator

def compute_features():
    # Feature computation logic
    pass

with DAG('feature_computation', ...) as dag:
    compute_task = PythonOperator(
        task_id='compute_features',
        python_callable=compute_features
    )
```

3. **Serve Features**
```python
from feast import FeatureStore

store = FeatureStore("feature_repo/")

features = store.get_online_features(
    features=[
        "customer_features:total_purchases",
        "customer_features:avg_order_value"
    ],
    entity_rows=[{"customer": 1001}]
)
```

## Development

1. Set up development environment:
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

2. Run tests:
```bash
pytest tests/
```

3. Start development services:
```bash
docker-compose -f docker-compose.dev.yml up
```

## Monitoring

Access monitoring dashboards:
- Feast UI: http://localhost:8080
- Airflow UI: http://localhost:8081
- MLflow UI: http://localhost:5000
- Grafana: http://localhost:3000

## License

MIT License 