import csv
import json
import os
import time
from typing import List, Dict
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Cấu hình hệ thống
KAFKA_ADDRESS = "35.220.200.137"
KAFKA_BOOTSTRAP_SERVERS = f'{KAFKA_ADDRESS}:9092'
TRANSACTION_TOPIC = 'fraud_transactions'
# Đường dẫn tới file CSV dữ liệu tài chính của Linh trên VM
INPUT_DATA_PATH = 'resources/financial_transactions.csv'


class FinancialProducer:
    def __init__(self, bootstrap_servers: str):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: str(v).encode('utf-8'),  # Gửi dạng string cho Consumer parse
            acks='all'  # Đảm bảo dữ liệu được ghi nhận thành công để không mất giao dịch quan trọng
        )

    def publish_transactions(self, file_path: str, topic: str):
        """Đọc file CSV và đẩy từng giao dịch vào Kafka"""
        try:
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                header = next(reader)  # Bỏ qua dòng header

                print(f"Bắt đầu đẩy dữ liệu vào topic: {topic}...")
                count = 0
                for row in reader:
                    # Chuyển list thành chuỗi phân tách bởi dấu phẩy khớp với logic parse của Consumer
                    message = ','.join(row)

                    self.producer.send(topic, value=message)
                    count += 1

                    # Giả lập thời gian thực: Cứ mỗi 0.5 giây gửi một giao dịch
                    if count % 10 == 0:
                        print(f"Đã gửi {count} giao dịch...")
                        time.sleep(0.5)

                self.producer.flush()
                print(f"Hoàn thành! Tổng cộng đã gửi {count} giao dịch.")
        except FileNotFoundError:
            print(f"Lỗi: Không tìm thấy file tại {file_path}. Linh hãy kiểm tra lại thư mục resources.")
        except KafkaError as e:
            print(f"Lỗi kết nối Kafka: {e}")


if __name__ == "__main__":
    # Đảm bảo KAFKA_ADDRESS đã được cấu hình trong môi trường
    os.environ['KAFKA_ADDRESS'] = KAFKA_ADDRESS

    producer = FinancialProducer(KAFKA_BOOTSTRAP_SERVERS)
    producer.publish_transactions(INPUT_DATA_PATH, TRANSACTION_TOPIC)