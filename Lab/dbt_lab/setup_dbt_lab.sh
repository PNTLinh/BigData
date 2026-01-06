#!/bin/bash

# Setup script for dbt Lab
# This script sets up the dbt lab environment

set -e

echo "🚀 Setting up dbt Lab Environment..."
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
ENV_NAME="dbt_lab"
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

# Install dbt PostgreSQL adapter
echo "📥 Installing dbt PostgreSQL adapter..."
pip install dbt-postgres

# Initialize dbt project (if not already initialized)
if [ ! -f "dbt_project.yml" ]; then
    echo "⚠️  dbt_project.yml not found. Please ensure it exists."
else
    echo "✅ dbt_project.yml found"
fi

# Create profiles directory if it doesn't exist
mkdir -p ~/.dbt

# Copy profiles.yml to ~/.dbt/ if it doesn't exist
if [ ! -f ~/.dbt/profiles.yml ]; then
    echo "📋 Copying profiles.yml to ~/.dbt/"
    cp profiles.yml ~/.dbt/profiles.yml
else
    echo "✅ profiles.yml already exists in ~/.dbt/"
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

# Test dbt connection
echo ""
echo "🧪 Testing dbt connection..."
if dbt debug --profiles-dir . --project-dir .; then
    echo -e "${GREEN}✅ dbt connection successful!${NC}"
else
    echo "⚠️  dbt connection test failed. Please check your configuration."
fi

# Install dbt packages (if packages.yml exists)
if [ -f "packages.yml" ]; then
    echo ""
    echo "📦 Installing dbt packages..."
    dbt deps --profiles-dir . --project-dir .
fi

echo ""
echo -e "${GREEN}✅ dbt Lab setup completed!${NC}"
echo ""
echo "📚 Next steps:"
echo "  1. Activate conda environment: conda activate ${ENV_NAME}"
echo "  2. Start Docker containers: docker-compose up -d"
echo "  3. Open Jupyter: jupyter notebook"
echo "  4. Navigate to notebooks/ directory"
echo ""
echo "🔗 Useful commands:"
echo "  - dbt run: Run all models"
echo "  - dbt test: Run all tests"
echo "  - dbt docs generate: Generate documentation"
echo "  - dbt docs serve: Serve documentation"
echo ""

