#!/bin/bash

# Exit on error
set -e

# Load environment variables
source .env

# Functions
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Start platform
start_platform() {
    log "Starting ML Feature Platform..."
    
    # Start infrastructure
    log "Starting infrastructure services..."
    docker-compose up -d
    
    # Wait for services
    log "Waiting for services to be ready..."
    sleep 30
    
    # Apply Feast features
    log "Applying Feast feature definitions..."
    cd feature_repo
    feast apply
    cd ..
    
    # Start API server
    log "Starting API server..."
    uvicorn src.serving.api.app:app --host 0.0.0.0 --port 8000 --reload &
    
    log "Platform started successfully!"
}

# Stop platform
stop_platform() {
    log "Stopping ML Feature Platform..."
    
    # Stop API server
    pkill -f "uvicorn src.serving.api.app:app"
    
    # Stop infrastructure
    docker-compose down
    
    log "Platform stopped successfully!"
}

# Main script
case "$1" in
    start)
        start_platform
        ;;
    stop)
        stop_platform
        ;;
    restart)
        stop_platform
        start_platform
        ;;
    *)
        echo "Usage: $0 {start|stop|restart}"
        exit 1
        ;;
esac 