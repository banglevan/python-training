# Real-time Data Pipeline

A real-time data processing pipeline that integrates multiple message queues for IoT sensor data collection, transformation, and analytics.

## Architecture

```
[IoT Devices] -> [MQTT Broker] -> [Kafka] -> [Processing] -> [Redis Streams] -> [Analytics]
                                                         -> [PostgreSQL] -> [Monitoring]
```

### Components

1. **Data Sources**
   - IoT sensors (temperature, humidity, pressure)
   - System metrics (CPU, memory, network)
   - Application logs

2. **Message Queues**
   - MQTT: Device data ingestion
   - Kafka: Stream processing and transformation
   - Redis Streams: Real-time analytics

3. **Storage**
   - PostgreSQL: Persistent storage and historical data
   - Redis: Real-time metrics and caching

4. **Processing**
   - Data validation
   - Transformation
   - Aggregation
   - Anomaly detection

## Database Structure

### PostgreSQL Tables

1. **sensors**
```sql
CREATE TABLE sensors (
    sensor_id VARCHAR(50) PRIMARY KEY,
    type VARCHAR(20) NOT NULL,
    location VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

2. **measurements**
```sql
CREATE TABLE measurements (
    id BIGSERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) REFERENCES sensors(sensor_id),
    metric VARCHAR(20) NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    processed BOOLEAN DEFAULT FALSE
);
```

3. **aggregations**
```sql
CREATE TABLE aggregations (
    id BIGSERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) REFERENCES sensors(sensor_id),
    metric VARCHAR(20) NOT NULL,
    avg_value DOUBLE PRECISION NOT NULL,
    min_value DOUBLE PRECISION NOT NULL,
    max_value DOUBLE PRECISION NOT NULL,
    count INTEGER NOT NULL,
    period VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL
);
```

4. **alerts**
```sql
CREATE TABLE alerts (
    id BIGSERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) REFERENCES sensors(sensor_id),
    type VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    severity VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP
);
```

## Features

1. **Data Collection**
   - MQTT device integration
   - Automatic sensor registration
   - Data validation

2. **Processing**
   - Stream processing with Kafka
   - Real-time aggregations
   - Anomaly detection

3. **Analytics**
   - Real-time metrics
   - Historical trends
   - Alert generation

4. **Monitoring**
   - System health checks
   - Pipeline metrics
   - Error tracking
   - Performance monitoring

## Setup

1. **Dependencies**
```bash
pip install -r requirements.txt
```

2. **Environment Variables**
```bash
# Create .env file
cp .env.example .env
# Edit variables
```

3. **Database**
```bash
# Run migrations
python migrations.py
```

4. **Start Services**
```bash
# Start pipeline
python realtime_pipeline.py
```

## Configuration

1. **Queue Settings**
   - MQTT broker: localhost:1883
   - Kafka broker: localhost:9092
   - Redis: localhost:6379

2. **Database Settings**
   - PostgreSQL: localhost:5432
   - Database name: sensor_data

3. **Processing Settings**
   - Batch size: 1000
   - Processing interval: 5s
   - Alert thresholds: Configurable per metric

## Monitoring

Access monitoring dashboard:
- URL: http://localhost:8080
- Metrics: http://localhost:8080/metrics
- Alerts: http://localhost:8080/alerts

## Error Handling

1. **Retry Mechanisms**
   - Failed message processing
   - Database operations
   - Queue operations

2. **Dead Letter Queues**
   - Invalid messages
   - Processing failures
   - Retry exhaustion

3. **Logging**
   - Error tracking
   - Performance metrics
   - System events

## Performance

- Throughput: 10,000 messages/second
- Latency: < 100ms
- Storage: Configurable retention 