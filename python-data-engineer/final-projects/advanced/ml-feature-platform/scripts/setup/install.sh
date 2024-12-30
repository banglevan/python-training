#!/bin/bash

# Exit on error
set -e

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Create necessary directories
mkdir -p data/features
mkdir -p logs

# Setup environment variables
cp .env.example .env

# Initialize Feast
feast init feature_repo
cd feature_repo
feast apply

echo "Setup completed successfully!" 