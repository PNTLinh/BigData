#!/bin/bash

# Airflow Lab Setup Script
echo "🚀 Setting up Airflow Lab environment..."

# Check if conda environment exists
if conda info --envs | grep -q "datalab"; then
    echo "✅ Found datalab conda environment"
else
    echo "❌ datalab conda environment not found. Please create it first."
    exit 1
fi

# Activate conda environment
echo "📦 Activating datalab environment..."
source $(conda info --base)/etc/profile.d/conda.sh
conda activate datalab

# Install requirements (for local development)
echo "📥 Installing Airflow lab dependencies..."
pip install -r requirements.txt

# Set AIRFLOW_UID if not set
if [ -z "$AIRFLOW_UID" ]; then
    export AIRFLOW_UID=$(id -u)
    echo "AIRFLOW_UID=$AIRFLOW_UID" >> .env
    echo "✅ Set AIRFLOW_UID=$AIRFLOW_UID"
fi

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p dags logs plugins config notebooks data scripts

# Make scripts executable
chmod +x scripts/*.sh 2>/dev/null || true

echo "✅ Airflow Lab setup completed!"
echo ""
echo "📋 Next steps:"
echo "1. Initialize Airflow database: docker compose up airflow-init"
echo "2. Start Airflow services: docker compose up -d"
echo "3. Access Airflow UI: http://localhost:8080"
echo "   Username: airflow"
echo "   Password: airflow"
echo "4. Start Jupyter Lab: jupyter lab"
echo "5. Open notebooks/01_airflow_basics.ipynb"
echo ""
echo "🌐 Airflow UI: http://localhost:8080"
echo "📊 PostgreSQL: localhost:5432 (airflow/airflow)"

