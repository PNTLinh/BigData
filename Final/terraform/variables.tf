locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
  description = "GCP Project ID của bạn"
  default     = "bigdata-405714"
  type        = string
}

variable "region" {
  description = "Vùng tài nguyên GCP. Dự án chọn asia-east2 (Hồng Kông) để tối ưu tốc độ từ Việt Nam."
  default     = "asia-east2"
  type        = string
}

variable "zone" {
  description = "Zone cụ thể trong Region."
  default     = "asia-east2-a"
  type        = string
}

variable "storage_class" {
  description = "Loại lưu trữ cho GCS Bucket."
  default     = "STANDARD"
}

variable "BQ_DATASET" {
  description = "Dataset chứa dữ liệu giao dịch thô (tương ứng tầng Bronze/Silver)."
  type        = string
  default     = "fraud_detection" # Đổi từ trips_data_all sang fraud_detection để khớp bài toán mới
}

variable "credentials" {
  description = "Đường dẫn tuyệt đối tới file JSON Service Account trên máy Windows của Linh."
  type        = string
  default     = "g:/School/Bigdata/Project/data/bigdata-405714-4d85ab4eb36b.json"
}

variable "network" {
  description = "Mạng VPC cho Kafka VM và Dataproc."
  default     = "default"
  type        = string
}

variable "vm_image" {
  description = "Hệ điều hành cho Kafka VM instance."
  default     = "ubuntu-os-cloud/ubuntu-2004-lts"
  type        = string
}

# Các biến bổ sung cho quản lý cụm
variable "cluster_name" {
  description = "Tên của Dataproc Cluster xử lý Machine Learning."
  default     = "bigdata-cluster"
}