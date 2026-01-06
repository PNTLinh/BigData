# dbt Lab - Data Build Tool Learning Lab

## 📋 Overview

Hệ thống bài lab học tập về **dbt (Data Build Tool)** - công cụ phổ biến nhất trong Data Engineering để transform data trong data warehouse. Lab này sẽ hướng dẫn bạn từ cơ bản đến nâng cao về dbt.

## 🎯 Learning Objectives

Sau khi hoàn thành lab này, bạn sẽ có thể:

- ✅ Hiểu kiến trúc và cách hoạt động của dbt
- ✅ Tạo và quản lý dbt models
- ✅ Sử dụng Jinja templating trong SQL
- ✅ Viết và chạy data quality tests
- ✅ Tạo documentation cho dbt project
- ✅ Sử dụng macros để tái sử dụng code
- ✅ Tích hợp dbt với Airflow
- ✅ Áp dụng best practices cho dbt projects

## 🏗️ Lab Structure

```
dbt_lab/
├── notebooks/          # Jupyter notebooks cho từng lab
│   ├── 01_dbt_basics.ipynb
│   ├── 02_models_and_sql.ipynb
│   ├── 03_testing_and_documentation.ipynb
│   ├── 04_macros_and_jinja.ipynb
│   └── 05_airflow_integration.ipynb
├── models/             # dbt SQL models
│   ├── staging/
│   ├── intermediate/
│   └── marts/
├── tests/              # Custom tests
├── macros/             # Jinja macros
├── data/               # Sample data và init scripts
├── docker-compose.yml   # Docker setup
├── dbt_project.yml     # dbt project configuration
├── profiles.yml        # dbt profiles configuration
└── requirements.txt    # Python dependencies
```

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- Conda hoặc Miniconda
- Docker và Docker Compose
- Git

### Setup Steps

1. **Clone và navigate đến thư mục:**
```bash
cd dbt_lab
```

2. **Run setup script:**
```bash
chmod +x setup_dbt_lab.sh
./setup_dbt_lab.sh
```

3. **Activate conda environment:**
```bash
conda activate dbt_lab
```

4. **Start Docker containers:**
```bash
docker-compose up -d
```

5. **Test dbt connection:**
```bash
dbt debug --profiles-dir . --project-dir .
```

6. **Run sample models:**
```bash
dbt run --profiles-dir . --project-dir .
```

7. **Open Jupyter:**
```bash
jupyter notebook
```

## 📚 Lab Content

### Lab 1: dbt Basics
- Giới thiệu về dbt
- Cài đặt và cấu hình
- dbt project structure
- dbt commands cơ bản
- Kết nối với database

### Lab 2: Models và SQL Transformations
- Tạo dbt models
- SQL transformations với Jinja
- Model dependencies và ref()
- Sources và source freshness
- Materializations (view, table, incremental)

### Lab 3: Testing và Documentation
- Data quality tests
- Generic tests (unique, not_null, etc.)
- Custom tests
- Documentation với YAML
- Generating và serving docs

### Lab 4: Macros và Jinja
- Tạo và sử dụng macros
- Jinja templating nâng cao
- dbt_utils package
- Code reuse patterns

### Lab 5: Airflow Integration
- Tích hợp dbt với Airflow
- dbt operators trong Airflow
- Scheduling dbt runs
- Error handling và monitoring

## 🐳 Docker Services

Lab này sử dụng Docker Compose để chạy:

- **PostgreSQL**: Database cho dbt models
  - Port: 5432
  - User: dbt_user
  - Password: dbt_password
  - Database: dbt_db

- **pgAdmin**: Web UI để quản lý PostgreSQL
  - Port: 5050
  - Email: admin@dbt.local
  - Password: admin

## 📖 Common dbt Commands

```bash
# Debug connection
dbt debug

# Run all models
dbt run

# Run specific models
dbt run --select model_name
dbt run --select staging.*
dbt run --select marts.*

# Run tests
dbt test

# Run specific tests
dbt test --select test_name

# Generate documentation
dbt docs generate

# Serve documentation
dbt docs serve

# Seed data
dbt seed

# Run operations (macros)
dbt run-operation macro_name

# Compile SQL without running
dbt compile

# List resources
dbt list
dbt list --select staging.*
```

## 🔗 Useful Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Discourse](https://discourse.getdbt.com/)
- [dbt GitHub](https://github.com/dbt-labs/dbt-core)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)

## 🐛 Troubleshooting

### Connection Issues

Nếu gặp lỗi kết nối database:

1. Kiểm tra Docker containers đang chạy:
```bash
docker-compose ps
```

2. Kiểm tra logs:
```bash
docker-compose logs postgres
```

3. Test connection:
```bash
dbt debug --profiles-dir . --project-dir .
```

### Profile Issues

Nếu gặp lỗi profile:

1. Kiểm tra `profiles.yml` trong `~/.dbt/` hoặc project root
2. Đảm bảo connection settings đúng với Docker Compose

### Model Errors

Nếu models fail:

1. Check logs trong `logs/` directory
2. Compile SQL để xem generated SQL:
```bash
dbt compile
```

## 📝 Notes

- Tất cả dbt commands nên chạy với `--profiles-dir . --project-dir .` trong lab này
- Models được organize theo staging → intermediate → marts pattern
- Sample data được load tự động khi start PostgreSQL container

## 🎓 Next Steps

Sau khi hoàn thành lab này, bạn có thể:

1. Tích hợp dbt vào production pipelines với Airflow
2. Sử dụng dbt Cloud cho team collaboration
3. Áp dụng dbt best practices vào real projects
4. Explore advanced features như snapshots, seeds, và Python models

---

**Happy Transforming! 🚀**

