#!/bin/bash

# Exit on error
set -e

# Create directories
mkdir -p data/{delta,temp}
mkdir -p logs

# Install dependencies
pip install -r requirements.txt

# Setup environment
cp .env.example .env

# Initialize components
echo "Initializing components..."

# Atlas
echo "Setting up Atlas..."
./scripts/setup/atlas_setup.sh

# Ranger
echo "Setting up Ranger..."
./scripts/setup/ranger_setup.sh

# Trino
echo "Setting up Trino..."
./scripts/setup/trino_setup.sh

echo "Setup completed successfully!" 