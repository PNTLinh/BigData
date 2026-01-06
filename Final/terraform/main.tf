terraform {
  required_version = ">= 1.0"
  backend "local" {}
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.47.0"
    }
  }
}

provider "google" {
  project     = var.project
  region      = var.region
  credentials = file(var.credentials)
}

# 1. Data Lake Bucket (Bronze Layer)
# Lưu trữ dữ liệu giao dịch thô (Raw Fraud Transactions)
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${local.data_lake_bucket}_${var.project}"
  location      = var.region
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 # Xóa dữ liệu cũ sau 30 ngày để tiết kiệm chi phí
    }
  }

  force_destroy = true
}

# 2. BigQuery Datasets (DWH)
# Dataset chứa dữ liệu thô phục vụ biến đổi
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  location   = var.region
}

# Dataset chính cho Production (Chứa các bảng Dim/Fact của bài toán Fraud)
resource "google_bigquery_dataset" "production_dataset" {
  dataset_id = "production"
  project    = var.project
  location   = var.region
}

# 3. Artifact Registry
# Lưu trữ Docker Images cho Prefect Agents xử lý Ingestion
resource "google_artifact_registry_repository" "bigdata-repo" {
  location      = var.region
  repository_id = "bigdata-repo"
  description   = "Docker repository cho Prefect agents xử lý Fraud Detection"
  format        = "DOCKER"
}

# 4. Firewall Rules cho Kafka
# Mở port 9092 để Spark Cluster có thể kết nối với Kafka Broker
resource "google_compute_firewall" "port_rules" {
  project     = var.project
  name        = "kafka-broker-port"
  network     = var.network
  description = "Mở port 9092 trong Kafka VM để Spark cluster kết nối"

  allow {
    protocol = "tcp"
    ports    = ["9092", "9093"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["kafka"]
}

# 5. Kafka VM Instance
# Máy chủ giả lập luồng giao dịch tài chính thời gian thực
resource "google_compute_instance" "kafka_vm_instance" {
  name                      = "kafka-instance"
  machine_type              = "e2-standard-4"
  zone                      = var.zone
  tags                      = ["kafka"]
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = var.vm_image
      size  = 30
    }
  }

  network_interface {
    network = var.network
    access_config {
      # Cấp IP External để Linh có thể SSH và Producer đẩy dữ liệu
    }
  }
}

# 6. Dataproc Cluster (Spark Managed Service)
# Dùng để chạy các Job huấn luyện mô hình Machine Learning phát hiện gian lận
resource "google_dataproc_cluster" "dataproc-cluster" {
  name     = "bigdata-cluster"
  project  = var.project
  region   = var.region

  cluster_config {
    gce_cluster_config {
      network = var.network
      zone    = var.zone

      shielded_instance_config {
        enable_secure_boot = true
      }
    }

    master_config {
      num_instances = 1
      machine_type  = "n2-standard-2"
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = 40
      }
    }

    worker_config {
      num_instances = 2
      machine_type  = "n2-standard-2"
      disk_config {
        boot_disk_size_gb = 40
      }
    }

    software_config {
      image_version = "2.0-debian10"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
      optional_components = ["JUPYTER"]
    }
  }
}