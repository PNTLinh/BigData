#!/bin/bash

# Setup script for Great Expectations Lab
# This script sets up the GE lab environment

set -e

echo "🚀 Setting up Great Expectations Lab Environment..."
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if conda is installed
if ! command -v conda &> /dev/null; then
    echo "❌ Conda is not installed. Please install Miniconda or Anaconda first."
    exit 1
fi

# Create conda environment if it doesn't exist
ENV_NAME="ge_lab"
if conda env list | grep -q "^${ENV_NAME} "; then
    echo "✅ Conda environment '${ENV_NAME}' already exists"
else
    echo "📦 Creating conda environment '${ENV_NAME}'..."
    conda create -n ${ENV_NAME} python=3.10 -y
fi

# Activate environment
echo "🔧 Activating conda environment..."
eval "$(conda shell.bash hook)"
conda activate ${ENV_NAME}

# Install dependencies
echo "📥 Installing Python dependencies..."
pip install -r requirements.txt

# Initialize Great Expectations
echo "🔮 Initializing Great Expectations..."
if [ ! -d "great_expectations" ]; then
    great_expectations init --no-view
    echo "✅ Great Expectations initialized"
else
    echo "✅ Great Expectations already initialized"
fi

# Start Docker containers
echo ""
echo "🐳 Starting Docker containers..."
if command -v docker-compose &> /dev/null; then
    docker-compose up -d
    echo "⏳ Waiting for PostgreSQL to be ready..."
    sleep 5
else
    echo "⚠️  docker-compose not found. Please start PostgreSQL manually."
fi

echo ""
echo -e "${GREEN}✅ Great Expectations Lab setup completed!${NC}"
echo ""
echo "📚 Next steps:"
echo "  1. Activate conda environment: conda activate ${ENV_NAME}"
echo "  2. Start Docker containers: docker-compose up -d"
echo "  3. Open Jupyter: jupyter notebook"
echo "  4. Navigate to notebooks/ directory"
echo ""
echo "🔗 Useful commands:"
echo "  - great_expectations init: Initialize GE project"
echo "  - great_expectations datasource new: Add datasource"
echo "  - great_expectations suite new: Create expectation suite"
echo "  - great_expectations checkpoint new: Create checkpoint"
echo ""

