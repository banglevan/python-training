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
    log "Starting real-time data platform..."
    
    # Start infrastructure using docker-compose
    log "Starting infrastructure services..."
    docker-compose up -d
    
    # Wait for services to be ready
    log "Waiting for services to be ready..."
    sleep 30
    
    # Start platform components
    log "Starting platform components..."
    python main.py start-all
}

# Stop platform
stop_platform() {
    log "Stopping platform..."
    
    # Stop all Python processes
    pkill -f "python main.py" || true
    
    # Stop infrastructure
    docker-compose down
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