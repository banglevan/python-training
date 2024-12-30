#!/bin/bash

# Exit on error
set -e

# Load environment variables
source .env

# Functions
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

stop_services() {
    log "Stopping all services..."
    docker-compose down
    
    # Kill any remaining Python processes
    pkill -f "python src/monitoring/metrics/collector.py" || true
}

cleanup() {
    log "Cleaning up..."
    
    # Remove temporary files
    rm -rf /tmp/checkpoints/*
    
    # Remove Docker volumes (optional)
    if [[ "${CLEANUP_VOLUMES}" == "true" ]]; then
        docker volume prune -f
    fi
}

# Main stop process
main() {
    log "Starting shutdown..."
    
    # Stop services
    stop_services
    
    # Cleanup
    cleanup
    
    log "Shutdown completed successfully"
}

# Run shutdown
main 