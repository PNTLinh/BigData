#!/usr/bin/env python
# coding: utf-8

import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()

# Thay đổi các tham số đầu vào cho phù hợp với dữ liệu Fraud
parser.add_argument('--input_data', required=True, help="Đường dẫn GCS tới dữ liệu giao dịch sạch")
parser.add_argument('--output', required=True, help="Bảng đích trên BigQuery (Gold Layer)")

args = parser.parse_args()

input_data = args.input_data
output = args.output

# Khởi tạo Spark Session
spark = SparkSession.builder.appName('fraud_analytics_sql').getOrCreate()

# Cấu hình bucket tạm cho đầu ra BigQuery (Sử dụng bucket Dataproc hiện có của bạn)
spark.conf.set('temporaryGcsBucket', 'dataproc-temp-asia-east2-285145462114-ku4fpzno')

# Đọc dữ liệu từ tầng Silver (Parquet format)
df_fraud = spark.read.parquet(input_data)

# Đăng ký DataFrame thành một bảng tạm để sử dụng Spark SQL
df_fraud.createOrReplaceTempView('fraud_data')

# Thực hiện truy vấn Spark SQL để phân tích các chỉ số gian lận
# Thay thế logic tính doanh thu taxi bằng phân tích rủi ro tài chính
df_result = spark.sql("""
SELECT 
    -- Nhóm theo loại giao dịch và khung giờ
    type AS transaction_type,
    date_trunc('hour', transaction_timestamp) AS transaction_hour, 

    -- Các chỉ số phân tích rủi ro (Risk Metrics)
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_amount_vol,
    AVG(amount) AS avg_transaction_amount,

    -- Thống kê số lượng gian lận đã xác định
    SUM(CAST(isFraud AS INT)) AS confirmed_fraud_count,

    -- Tính toán tỉ lệ gian lận trên tổng số giao dịch
    ROUND(SUM(CAST(isFraud AS INT)) / COUNT(*) * 100, 2) AS fraud_rate_percentage,

    -- Phân tích sai lệch số dư trung bình (Balance Error)
    AVG(ABS(oldbalanceOrg - amount - newbalanceOrig)) AS avg_balance_discrepancy
FROM
    fraud_data
GROUP BY
    1, 2
ORDER BY 
    transaction_hour DESC, fraud_rate_percentage DESC
""")

# Ghi kết quả cuối cùng (Gold Layer) trực tiếp vào BigQuery phục vụ Dashboard
df_result.write.format('bigquery') \
    .option('table', output) \
    .mode('overwrite') \
    .save()

print(f"Đã hoàn thành phân tích SQL. Kết quả được lưu tại: {output}")