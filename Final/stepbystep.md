# Reproduce the Financial Fraud Detection Project

## Table of Contents

* [1. Google Cloud Platform and Terraform](https://www.google.com/search?q=%231-google-cloud-platform-and-terraform)
* [2. Prefect, Docker and Cloud Run](https://www.google.com/search?q=%232-prefect-docker-and-cloud-run)
* [3. Analytics Engineering & Fraud Modeling](https://www.google.com/search?q=%233-analytics-engineering)
* [4. Spark ML & ETL on Dataproc](https://www.google.com/search?q=%234-spark-etl)
* [5. Real-time Fraud Detection with Kafka](https://www.google.com/search?q=%235-kafka)

---

## 1. Google Cloud Platform and Terraform

### 1.1. Google Cloud Platform (GCP) Setup

1. **Project & Service Account**: Tạo project trên GCP Console, tạo Service Account và tải file `.json` key.
2. **IAM Roles**: Gán các quyền sau cho Service Account:
* `Storage Admin`, `BigQuery Admin`, `Cloud Run Admin`, `Dataproc Admin`, `Pub/Sub Admin` (nếu dùng cloud native messaging).


3. **Authentication**:
* **Trên Windows (PowerShell)**:
```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\path\to\your\keys.json"
gcloud auth activate-service-account --key-file=$env:GOOGLE_APPLICATION_CREDENTIALS

```


* **Trên Linux/WSL**:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/keys.json"
gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS

```





### 1.2. Terraform (Infrastructure as Code)

Khởi tạo và triển khai GCS Bucket & BigQuery Dataset:

```bash
cd terraform
terraform init
terraform plan -var="project=<your-gcp-project-id>"
terraform apply

```

---

## 2. Prefect, Docker and Cloud Run

### 2.1. Ingestion: Web to GCS

Pipeline tải dữ liệu giao dịch tài chính (Kaggle/Simulated) và lưu vào tầng **Bronze** (Raw Data).

```bash
# Register blocks (GCS, Credentials) trên Prefect Cloud trước
prefect deployment build flows/etl_web_to_gcs_fraud.py:etl_parent_fraud -n "Fraud Ingestion" -a
prefect deployment run 'etl_parent_fraud/Fraud Ingestion'

```

### 2.2. Serverless Workers với Cloud Run

```bash
# Thay [PROJECT_ID] bằng ID thực tế của bạn
docker build -t asia-east2-docker.pkg.dev/[PROJECT_ID]/bigdata-repo/fraud-worker:latest .
gcloud auth configure-docker asia-east2-docker.pkg.dev
docker push asia-east2-docker.pkg.dev/[PROJECT_ID]/bigdata-repo/fraud-worker:latest

```

---

## 3. Analytics Engineering (dbt & BigQuery)

Tầng **Silver/Gold** để tổng hợp các chỉ số rủi ro:

* **dbt**: Chạy các model để xác định hành vi bất thường (ví dụ: một tài khoản thực hiện > 10 giao dịch/giờ).
* **Looker Studio**: Kết nối tới BigQuery table `gold_fraud_alerts`.

---

## 4. Spark ML & ETL (Batch Processing)

Xử lý dữ liệu lớn trên Dataproc để huấn luyện mô hình dự đoán.

```bash
# Upload code lên Cloud Storage
gsutil cp spark/*.py gs://[YOUR_CODE_BUCKET]/scripts/

# Submit Job (Lưu ý thư viện connector cho BigQuery)
gcloud dataproc jobs submit pyspark \
  --cluster=bigdata-cluster \
  --region=asia-east2 \
  --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
  gs://[YOUR_CODE_BUCKET]/scripts/train_fraud_model.py

```

---

## 5. Real-time Fraud Detection with Kafka

### 5.1. Kafka Cluster & Streaming Producer

Giả lập luồng giao dịch thời gian thực đổ vào hệ thống.

```bash
# Kết nối VM Kafka (đã setup via Docker)
gcloud compute ssh kafka-vm
cd Big-Data/kafka
docker-compose up -d

# Start Producer
python financial_transaction_producer.py

```

### 5.2. Spark Streaming Consumer

Đọc từ Kafka, kiểm tra điều kiện gian lận (Rule-based hoặc Model-based) và cảnh báo ngay lập tức.

```bash
export KAFKA_ADDRESS=[YOUR_VM_EXTERNAL_IP]

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3 \
  --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
  realtime_fraud_detector.py

```

