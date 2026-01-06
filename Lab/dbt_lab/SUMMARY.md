# dbt Lab - Summary

## 📋 Overview
Hệ thống bài lab học tập về **dbt (Data Build Tool)** - công cụ phổ biến nhất trong Data Engineering để transform data trong data warehouse.

## 🏗️ Lab Structure

```
dbt_lab/
├── notebooks/          # Jupyter notebooks
│   ├── 01_dbt_basics.ipynb
│   ├── 02_models_and_sql.ipynb
│   ├── 03_testing_and_documentation.ipynb
│   ├── 04_macros_and_jinja.ipynb
│   └── 05_airflow_integration.ipynb
├── models/             # dbt SQL models
│   ├── staging/        # Staging models
│   ├── intermediate/   # Intermediate models
│   ├── marts/          # Final analytics models
│   └── sources.yml     # Source definitions
├── tests/              # Custom tests
├── macros/             # Jinja macros
├── data/               # Sample data
└── docker-compose.yml  # Docker setup
```

## 🚀 Quick Start

1. **Setup environment:**
```bash
chmod +x setup_dbt_lab.sh
./setup_dbt_lab.sh
```

2. **Start Docker:**
```bash
docker-compose up -d
```

3. **Test connection:**
```bash
dbt debug --profiles-dir . --project-dir .
```

4. **Run models:**
```bash
dbt run --profiles-dir . --project-dir .
```

## 📚 Lab Content

### Lab 1: dbt Basics
- Giới thiệu về dbt
- Cài đặt và cấu hình
- dbt project structure
- dbt commands cơ bản

### Lab 2: Models và SQL Transformations
- Tạo dbt models
- SQL transformations với Jinja
- Model dependencies
- Materializations

### Lab 3: Testing và Documentation
- Data quality tests
- Generic tests
- Custom tests
- Documentation

### Lab 4: Macros và Jinja
- Tạo và sử dụng macros
- Jinja templating nâng cao
- Code reuse patterns

### Lab 5: Airflow Integration
- Tích hợp dbt với Airflow
- dbt operators
- Scheduling dbt runs

## 🐳 Docker Services

- **PostgreSQL**: Port 5432
- **pgAdmin**: Port 5050

## 📖 Common Commands

- `dbt debug`: Test connection
- `dbt run`: Run models
- `dbt test`: Run tests
- `dbt docs generate`: Generate docs
- `dbt docs serve`: Serve docs

## 🔗 Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Discourse](https://discourse.getdbt.com/)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)

