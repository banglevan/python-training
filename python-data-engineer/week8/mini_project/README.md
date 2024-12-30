# Real-time Data Sync System

A production-grade data synchronization system that handles multiple data sources, transformations, and real-time updates.

## Use Case: E-commerce Multi-Platform Integration
Synchronizes product, inventory, and order data across multiple platforms:
- Shopify stores
- WooCommerce sites 
- Custom ERP system

## Architecture

### Components
1. **Source Connectors**
   - Shopify CDC connector
   - WooCommerce webhook listener **-> needs to compare with Shopify**
   - ERP database CDC (using Debezium)

2. **Data Pipeline**
   - Kafka for event streaming
   - Flink for transformations
   - Redis for caching

3. **Sync Manager**
   - Conflict resolution
   - State management
   - Error handling

4. **Monitoring**
   - Prometheus metrics
   - Grafana dashboards
   - Alert manager

### Database Structure

#### Source Events
```sql
CREATE TABLE source_events (
    event_id UUID PRIMARY KEY,
    source_type VARCHAR(50),  -- SHOPIFY, WOOCOMMERCE, ERP
    source_id VARCHAR(100),
    event_type VARCHAR(50),
    data JSONB,
    timestamp TIMESTAMP,
    status VARCHAR(20),
    checksum VARCHAR(64)
);

CREATE INDEX idx_source_events_timestamp ON source_events(timestamp);
CREATE INDEX idx_source_events_status ON source_events(status);
```

#### Sync State
```sql
CREATE TABLE sync_state (
    entity_id VARCHAR(100),
    source_type VARCHAR(50),
    last_sync_at TIMESTAMP,
    version INTEGER,
    data JSONB,
    status VARCHAR(20),
    PRIMARY KEY (entity_id, source_type)
);
```

#### Conflict Log
```sql
CREATE TABLE conflict_log (
    conflict_id UUID PRIMARY KEY,
    entity_id VARCHAR(100),
    source_type VARCHAR(50),
    conflict_type VARCHAR(50),
    resolution VARCHAR(50),
    details JSONB,
    created_at TIMESTAMP
);
```

#### Metrics
```sql
CREATE TABLE sync_metrics (
    metric_id UUID PRIMARY KEY,
    metric_name VARCHAR(100),
    metric_value FLOAT,
    labels JSONB,
    timestamp TIMESTAMP
);

CREATE INDEX idx_sync_metrics_name_timestamp 
ON sync_metrics(metric_name, timestamp);
```

## Module Structure
```
mini_project/
├── src/
│   ├── connectors/
│   │   ├── __init__.py
│   │   ├── shopify.py
│   │   ├── woocommerce.py
│   │   └── erp.py
│   ├── pipeline/
│   │   ├── __init__.py
│   │   ├── kafka_manager.py
│   │   ├── flink_processor.py
│   │   └── cache_manager.py
│   ├── sync/
│   │   ├── __init__.py
│   │   ├── manager.py
│   │   ├── conflict.py
│   │   └── state.py
│   └── monitoring/
│       ├── __init__.py
│       ├── metrics.py
│       └── alerts.py
├── tests/
│   ├── __init__.py
│   ├── test_connectors.py
│   ├── test_pipeline.py
│   ├── test_sync.py
│   └── test_monitoring.py
├── config/
│   ├── config.yml
│   └── logging.yml
└── requirements.txt
```

## Setup & Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure data sources in `config/config.yml`

3. Initialize databases:
```bash
python -m src.scripts.init_db
```

4. Start services:
```bash
python -m src.main
```

## Monitoring

### Key Metrics
- Sync latency
- Event processing rate
- Error rates
- Conflict rates
- Resource usage

### Alerts
- High latency
- Error spikes
- Sync failures
- Resource constraints

## Error Handling
- Automatic retries with backoff
- Dead letter queues
- Alert notifications
- State recovery

## Performance Considerations
- Batch processing
- Caching strategies
- Connection pooling
- Resource limits 