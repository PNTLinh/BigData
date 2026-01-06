#!/bin/bash

# Setup script for Data Lakehouse Lab
# This script sets up the integrated data lakehouse environment

set -e

echo "🚀 Setting up Data Lakehouse Lab Environment..."
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
ENV_NAME="lakehouse_lab"
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

# Initialize Airflow directories
echo "🌪️  Creating Airflow directories..."
mkdir -p dags logs plugins config

# Note: Airflow sẽ được initialized trong Docker container
# Không cần init local database vì Airflow chạy trong Docker
echo "✅ Airflow directories created"
echo "💡 Airflow sẽ được initialized khi Docker containers start"

# Initialize dbt project (if not exists)
if [ ! -f "dbt_project/dbt_project.yml" ]; then
    echo "📊 Initializing dbt project..."
    mkdir -p dbt_project
    cd dbt_project
    dbt init dbt_lakehouse --skip-profile-setup || echo "dbt project may already exist"
    cd ..
fi

# Initialize Great Expectations (if not exists)
if [ ! -d "ge_project/great_expectations" ]; then
    echo "🔮 Initializing Great Expectations..."
    mkdir -p ge_project
    cd ge_project
    great_expectations init --no-view || echo "GE project may already exist"
    cd ..
fi

# Start Docker containers
echo ""
echo "🐳 Starting Docker containers..."
if command -v docker-compose &> /dev/null; then
    docker-compose up -d
    echo "⏳ Waiting for services to be ready..."
    sleep 10
    echo "✅ Services started"
else
    echo "⚠️  docker-compose not found. Please start services manually."
fi

echo ""
echo -e "${GREEN}✅ Data Lakehouse Lab setup completed!${NC}"
echo ""
echo "📚 Next steps:"
echo "  1. Activate conda environment: conda activate ${ENV_NAME}"
echo "  2. Start Docker services: docker-compose up -d"
echo "  3. Check services: docker-compose ps"
echo "  4. Access Airflow UI: http://localhost:8080 (admin/admin)"
echo "  5. Access Spark UI: http://localhost:8080 (Spark Master)"
echo "  6. Open Jupyter: jupyter notebook"
echo ""
echo "🔗 Service URLs:"
echo "  - Airflow Web UI: http://localhost:8080"
echo "  - Spark Master UI: http://localhost:8080"
echo "  - Kafka: localhost:9092"
echo "  - PostgreSQL: localhost:5432"
echo ""

