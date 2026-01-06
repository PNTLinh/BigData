# Great Expectations Lab - Data Quality và Validation

## 📋 Overview

Hệ thống bài lab học tập về **Great Expectations (GE)** - công cụ mạnh mẽ cho data quality và validation. Lab này bao gồm cả Great Expectations standalone và tích hợp với dbt.

## 🎯 Learning Objectives

Sau khi hoàn thành lab này, bạn sẽ có thể:

- ✅ Hiểu Great Expectations là gì và tại sao sử dụng
- ✅ Tạo và quản lý Expectations
- ✅ Sử dụng Checkpoints để validate data
- ✅ Generate Data Docs tự động
- ✅ Tích hợp GE với dbt (dbt-expectations)
- ✅ Tích hợp GE với Airflow
- ✅ Áp dụng best practices cho data quality

## 🏗️ Lab Structure

```
Great_Expectations_lab/
├── notebooks/              # Jupyter notebooks cho từng lab
│   ├── 01_ge_basics.ipynb
│   ├── 02_expectations.ipynb
│   ├── 03_checkpoints.ipynb
│   ├── 04_data_docs.ipynb
│   ├── 05_dbt_integration.ipynb
│   └── 06_airflow_integration.ipynb
├── great_expectations/     # GE project directory (auto-generated)
├── data/                   # Sample data
├── expectations/           # Custom expectations
├── checkpoints/            # Checkpoint configurations
├── docker-compose.yml      # Docker setup
└── requirements.txt        # Python dependencies
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
cd Great_Expectations_lab
```

2. **Run setup script:**
```bash
chmod +x setup_ge_lab.sh
./setup_ge_lab.sh
```

3. **Activate conda environment:**
```bash
conda activate ge_lab
```

4. **Start Docker containers:**
```bash
docker-compose up -d
```

5. **Initialize Great Expectations (if not done):**
```bash
great_expectations init --no-view
```

6. **Open Jupyter:**
```bash
jupyter notebook
```

## 📚 Lab Content

### Lab 1: Great Expectations Basics
- Giới thiệu về Great Expectations
- Cài đặt và cấu hình
- Data Context và Data Sources
- Tạo Expectations đầu tiên

### Lab 2: Expectations
- Các loại Expectations
- Column-level expectations
- Table-level expectations
- Custom expectations
- Expectation suites

### Lab 3: Checkpoints
- Tạo và cấu hình Checkpoints
- Validation Actions
- Run checkpoints
- Handle validation results

### Lab 4: Data Docs
- Generate Data Docs
- Customize documentation
- Share documentation với team
- Data Docs best practices

### Lab 5: dbt Integration
- dbt-expectations package
- Sử dụng GE expectations trong dbt
- dbt tests với GE syntax
- Best practices

### Lab 6: Airflow Integration
- Tích hợp GE với Airflow
- Run validations trong pipelines
- Error handling và alerts
- Monitoring data quality

## 🐳 Docker Services

Lab này sử dụng Docker Compose để chạy:

- **PostgreSQL**: Database cho sample data
  - Port: 5433
  - User: ge_user
  - Password: ge_password
  - Database: ge_db

## 📖 Common Great Expectations Commands

```bash
# Initialize GE project
great_expectations init

# Add datasource
great_expectations datasource new

# Create expectation suite
great_expectations suite new

# Create checkpoint
great_expectations checkpoint new

# Run checkpoint
great_expectations checkpoint run <checkpoint_name>

# Generate docs
great_expectations docs build

# Serve docs
great_expectations docs serve
```

## 🔗 Useful Resources

- [Great Expectations Documentation](https://docs.greatexpectations.io/)
- [Great Expectations GitHub](https://github.com/great-expectations/great_expectations)
- [dbt-expectations Package](https://github.com/calogica/dbt-expectations)
- [GE Best Practices](https://docs.greatexpectations.io/docs/guides/expectations/expectations_best_practices/)

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

### GE Initialization Issues

Nếu GE initialization fails:

1. Xóa thư mục `great_expectations/` nếu có
2. Chạy lại: `great_expectations init --no-view`

## 📝 Notes

- Great Expectations project được tạo trong `great_expectations/` directory
- Data Docs được generate trong `great_expectations/uncommitted/data_docs/`
- Checkpoints và Expectations được lưu trong GE project

## 🎓 Next Steps

Sau khi hoàn thành lab này, bạn có thể:

1. Tích hợp GE vào production pipelines
2. Setup automated data quality checks
3. Integrate với monitoring systems
4. Build custom expectations cho use cases cụ thể

---

**Happy Validating! 🎯**

