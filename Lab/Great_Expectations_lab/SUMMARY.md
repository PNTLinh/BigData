# Great Expectations Lab - Summary

## 📋 Overview
Hệ thống bài lab học tập về **Great Expectations (GE)** - công cụ mạnh mẽ cho data quality và validation, bao gồm cả standalone và tích hợp với dbt.

## 🏗️ Lab Structure

```
Great_Expectations_lab/
├── notebooks/              # Jupyter notebooks
│   ├── 01_ge_basics.ipynb
│   ├── 02_expectations.ipynb
│   ├── 03_checkpoints.ipynb
│   ├── 04_data_docs.ipynb
│   ├── 05_dbt_integration.ipynb
│   └── 06_airflow_integration.ipynb
├── great_expectations/      # GE project (auto-generated)
├── data/                   # Sample data
└── docker-compose.yml      # Docker setup
```

## 🚀 Quick Start

1. **Setup environment:**
```bash
chmod +x setup_ge_lab.sh
./setup_ge_lab.sh
```

2. **Start Docker:**
```bash
docker-compose up -d
```

3. **Initialize GE:**
```bash
great_expectations init --no-view
```

## 📚 Lab Content

### Lab 1: GE Basics
- Giới thiệu Great Expectations
- Data Context và Data Sources
- Tạo Expectations đầu tiên

### Lab 2: Expectations
- Các loại Expectations
- Column và table-level expectations
- Custom expectations

### Lab 3: Checkpoints
- Tạo và run checkpoints
- Validation Actions
- Handle results

### Lab 4: Data Docs
- Generate và customize docs
- Share documentation

### Lab 5: dbt Integration
- dbt-expectations package
- GE-like tests trong dbt

### Lab 6: Airflow Integration
- Tích hợp GE với Airflow
- Automated validations

## 🐳 Docker Services

- **PostgreSQL**: Port 5433

## 📖 Common Commands

- `great_expectations init`: Initialize project
- `great_expectations datasource new`: Add datasource
- `great_expectations suite new`: Create suite
- `great_expectations checkpoint new`: Create checkpoint
- `great_expectations checkpoint run`: Run checkpoint
- `great_expectations docs build`: Generate docs
- `great_expectations docs serve`: Serve docs

## 🔗 Resources

- [Great Expectations Documentation](https://docs.greatexpectations.io/)
- [dbt-expectations](https://github.com/calogica/dbt-expectations)
- [GE Best Practices](https://docs.greatexpectations.io/docs/guides/expectations/expectations_best_practices/)

