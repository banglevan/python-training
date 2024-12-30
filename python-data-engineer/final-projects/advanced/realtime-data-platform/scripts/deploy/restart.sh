#!/bin/bash

# Exit on error
set -e

# Stop services
./scripts/deploy/stop.sh

# Start services
./scripts/deploy/deploy.sh 