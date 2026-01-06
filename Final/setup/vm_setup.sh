#!/bin/bash

set -e

echo "=================================================="
echo "BẮT ĐẦU THIẾT LẬP MÔI TRƯỜNG VM - FRAUD DETECTION"
echo "=================================================="

# 1. Cài đặt Anaconda để quản lý môi trường Python cho Spark/Prefect
echo "--- Đang tải và cài đặt Anaconda3 ---"
wget https://repo.anaconda.com/archive/Anaconda3-2021.11-Linux-x86_64.sh
bash Anaconda3-2021.11-Linux-x86_64.sh -b -p ~/anaconda
rm Anaconda3-2021.11-Linux-x86_64.sh

# Kích hoạt conda ngay lập tức trong session này
eval "$($HOME/anaconda/bin/conda shell.bash hook)"
conda init
conda update -y conda

echo "Phiên bản Conda đã cài đặt:"
conda --version

# 2. Cài đặt Docker (Trái tim của hệ thống Kafka & Zookeeper)
echo "--- Cài đặt Docker Engine ---"
sudo apt-get update
sudo apt-get -y install docker.io

# Thiết lập Docker chạy không cần quyền sudo (Quan trọng để các flows tự động không bị kẹt)
echo "--- Cấu hình Docker Permissions ---"
# Kiểm tra nếu group docker đã tồn tại để tránh lỗi
if ! getent group docker > /dev/null; then
    sudo groupadd docker
fi
sudo gpasswd -a $USER docker
sudo service docker restart

# 3. Cài đặt Docker-Compose (Để quản lý cụm Kafka/Zookeeper)
echo "--- Cài đặt Docker-Compose v2.3.3 ---"
mkdir -p ~/bin
cd ~/bin
wget https://github.com/docker/compose/releases/download/v2.3.3/docker-compose-linux-x86_64 -O docker-compose
sudo chmod +x docker-compose

# 4. Cấu hình Biến môi trường (.bashrc)
echo "--- Cấu hình Path và Google Credentials ---"
echo '' >> ~/.bashrc
echo 'export PATH=${HOME}/bin:${PATH}' >> ~/.bashrc
# Load lại bashrc để nhận docker-compose ngay lập tức
export PATH=${HOME}/bin:${PATH}

echo "Phiên bản Docker-Compose:"
docker-compose --version

# Tạo thư mục chứa GCP Keys phục vụ Prefect và Terraform
mkdir -p ~/.google/credentials

echo "=================================================="
echo "THIẾT LẬP HOÀN TẤT!"
echo "Linh hãy chạy lệnh: 'source ~/.bashrc' hoặc log out và log in lại"
echo "để các thay đổi về quyền Docker có hiệu lực."
echo "=================================================="