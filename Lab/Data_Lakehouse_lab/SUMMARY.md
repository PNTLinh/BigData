# Data Lakehouse Lab - Summary

## 📋 Overview
Hệ thống bài lab tổng hợp tích hợp tất cả các công nghệ Data Engineering:
- Kafka, Spark, Iceberg, dbt, Great Expectations, Airflow

## 🏗️ Lab Structure

```
Data_Lakehouse_lab/
├── notebooks/          # Jupyter notebooks
│   ├── 01_architecture_overview.ipynb
│   ├── 02_data_ingestion_kafka.ipynb
│   ├── 03_processing_spark_iceberg.ipynb
│   ├── 04_transformation_dbt.ipynb
│   ├── 05_data_quality_ge.ipynb
│   ├── 06_orchestration_airflow.ipynb
│   └── 07_end_to_end_pipeline.ipynb
├── dags/               # Airflow DAGs
├── spark_jobs/         # Spark jobs
├── dbt_project/       # dbt project
├── ge_project/         # Great Expectations project
└── docker-compose.yml  # All services
```

## 🚀 Quick Start

1. **Setup:**
```bash
./setup_lakehouse_lab.sh
```

2. **Start services:**
```bash
docker-compose up -d
```

3. **Access UIs:**
- Airflow: http://localhost:8080
- Spark Master: http://localhost:8081

## 📚 Lab Content

### Lab 1: Architecture Overview
- Data Lakehouse architecture
- Technology stack
- Integration patterns

### Lab 2: Data Ingestion với Kafka
- Kafka producers
- Schema validation
- Data quality at ingestion

### Lab 3: Processing với Spark + Iceberg
- Spark Streaming từ Kafka
- Write to Iceberg tables
- Batch processing

### Lab 4: Transformation với dbt
- dbt models trên Iceberg
- Staging → Marts
- Business logic

### Lab 5: Data Quality với GE
- GE expectations
- Validation checkpoints
- Quality monitoring

### Lab 6: Orchestration với Airflow
- Complete pipeline DAGs
- Task dependencies
- Error handling

### Lab 7: End-to-End Pipeline
- Complete implementation
- Integration testing
- Best practices

## 🐳 Services

- Kafka: 9092
- Spark Master: 8081 (UI)
- Airflow: 8080 (UI)
- PostgreSQL: 5432

## 🔗 Integration Flow

Kafka → Spark → Iceberg → dbt → GE → Airflow

## 📖 Key Concepts

- Medallion Architecture (Bronze/Silver/Gold)
- End-to-end data pipelines
- Multi-technology integration
- Production best practices

