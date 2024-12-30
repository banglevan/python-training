#!/bin/bash

# Exit on error
set -e

# Load environment variables
source .env

# Functions
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

check_dependencies() {
    log "Checking dependencies..."
    command -v docker >/dev/null 2>&1 || { echo "Docker is required but not installed. Aborting." >&2; exit 1; }
    command -v docker-compose >/dev/null 2>&1 || { echo "Docker Compose is required but not installed. Aborting." >&2; exit 1; }
}

setup_network() {
    log "Setting up Docker network..."
    docker network create data-platform-network 2>/dev/null || true
}

start_infrastructure() {
    log "Starting infrastructure services..."
    docker-compose up -d zookeeper kafka elasticsearch
    
    # Wait for services
    log "Waiting for services to be ready..."
    sleep 30
}

initialize_services() {
    log "Initializing services..."
    
    # Initialize Kafka topics
    log "Creating Kafka topics..."
    python scripts/setup/init_kafka.py
    
    # Initialize Delta tables
    log "Creating Delta tables..."
    python src/storage/delta/table_definitions.py
    
    # Initialize Elasticsearch indices
    log "Creating Elasticsearch indices..."
    python src/storage/elasticsearch/mappings.py
}

start_applications() {
    log "Starting applications..."
    
    # Start Spark streaming job
    docker-compose up -d spark-master spark-worker
    
    # Submit Spark job
    log "Submitting Spark streaming job..."
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,io.delta:delta-core_2.12:2.4.0 \
        /opt/spark/src/processing/streaming/event_processor.py
    
    # Start metrics collector
    log "Starting metrics collector..."
    python src/monitoring/metrics/collector.py &
    
    # Start Grafana
    log "Starting Grafana..."
    docker-compose up -d grafana
}

# Main deployment process
main() {
    log "Starting deployment..."
    
    # Check dependencies
    check_dependencies
    
    # Setup network
    setup_network
    
    # Start infrastructure
    start_infrastructure
    
    # Initialize services
    initialize_services
    
    # Start applications
    start_applications
    
    log "Deployment completed successfully"
}

# Run deployment
main 