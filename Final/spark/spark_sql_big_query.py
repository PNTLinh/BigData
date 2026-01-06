#!/usr/bin/env python
# coding: utf-8

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()

# Tham số đầu vào: Dữ liệu sạch (Silver) và Bảng đích (Gold)
parser.add_argument('--input_data', required=True, help="Đường dẫn GCS tới file Parquet giao dịch đã làm sạch")
parser.add_argument('--output', required=True, help="Bảng BigQuery đích (Ví dụ: production.fraud_summary_report)")

args = parser.parse_args()

input_data = args.input_data
output = args.output

# Khởi tạo Spark Session cho Dataproc
spark = SparkSession.builder \
    .appName('fraud_analytics_bigquery') \
    .getOrCreate()

# Cấu hình bucket tạm cho BigQuery connector (Dùng bucket hiện có của Linh)
spark.conf.set('temporaryGcsBucket', 'dataproc-temp-asia-east2-285145462114-ku4fpzno')

# 1. Đọc dữ liệu từ tầng Silver
df_fraud = spark.read.parquet(input_data)

# 2. Đăng ký bảng tạm để sử dụng Spark SQL
df_fraud.createOrReplaceTempView('fraud_transactions')

# 3. Phân tích các chỉ số gian lận quan trọng (Fraud Metrics)
# Chúng ta sẽ tính toán tỉ lệ gian lận và khối lượng giao dịch theo giờ
df_result = spark.sql("""
SELECT 
    -- Nhóm theo khung giờ giao dịch
    date_trunc('hour', transaction_timestamp) AS event_hour,
    type AS transaction_type,

    -- Thống kê tổng quan
    COUNT(*) AS total_tx_count,
    SUM(amount) AS total_amount_vol,

    -- Thống kê chi tiết gian lận (Confirmed Fraud)
    SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS total_fraud_cases,
    SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS total_fraud_amount,

    -- Tính toán tỉ lệ gian lận (%)
    ROUND(SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS fraud_rate_percentage,

    -- Phân tích sai lệch số dư (Dấu hiệu gian lận kỹ thuật)
    AVG(errorBalanceOrig) AS avg_balance_error
FROM
    fraud_transactions
GROUP BY
    1, 2
ORDER BY 
    event_hour DESC
""")

# 4. Lưu kết quả trực tiếp vào BigQuery
# Chế độ 'overwrite' để cập nhật báo cáo mới nhất
df_result.write.format('bigquery') \
    .option('table', output) \
    .mode('overwrite') \
    .save()

print(f"Dữ liệu phân tích đã được đẩy lên BigQuery: {output}")