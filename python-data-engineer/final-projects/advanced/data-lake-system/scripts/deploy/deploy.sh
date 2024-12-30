#!/bin/bash

# Exit on error
set -e

# Load environment variables
source .env

# Functions
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Start system
start_system() {
    log "Starting Data Lake System..."
    
    # Start infrastructure
    log "Starting infrastructure services..."
    docker-compose up -d
    
    # Wait for services
    log "Waiting for services to be ready..."
    sleep 30
    
    # Initialize components
    log "Initializing components..."
    ./scripts/deploy/init_components.sh
    
    log "System started successfully!"
}

# Stop system
stop_system() {
    log "Stopping Data Lake System..."
    
    # Stop components
    log "Stopping components..."
    ./scripts/deploy/stop_components.sh
    
    # Stop infrastructure
    docker-compose down
    
    log "System stopped successfully!"
}

# Main script
case "$1" in
    start)
        start_system
        ;;
    stop)
        stop_system
        ;;
    restart)
        stop_system
        start_system
        ;;
    *)
        echo "Usage: $0 {start|stop|restart}"
        exit 1
        ;;
esac 