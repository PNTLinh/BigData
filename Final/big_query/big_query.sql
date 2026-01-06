-- Tạo external table trỏ tới đường dẫn GCS chứa dữ liệu giao dịch
CREATE OR REPLACE EXTERNAL TABLE `bigdata-405714.fraud_detection.external_fraud_transactions`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_bigdata-405714/data/raw/transactions/2025-*.parquet']
);

-- Kiểm tra 10 dòng dữ liệu đầu tiên
SELECT * FROM `bigdata-405714.fraud_detection.external_fraud_transactions` LIMIT 10;
-- Tạo bảng đã được phân vùng từ external table
CREATE OR REPLACE TABLE `bigdata-405714.fraud_detection.fraud_transactions_partitioned`
PARTITION BY
  DATE(transaction_timestamp) AS
SELECT * FROM `bigdata-405714.fraud_detection.external_fraud_transactions`;

-- Kiểm tra các loại hình giao dịch trong một khoảng thời gian
SELECT DISTINCT(transaction_type)
FROM `bigdata-405714.fraud_detection.fraud_transactions_partitioned`
WHERE DATE(transaction_timestamp) BETWEEN '2025-01-01' AND '2025-01-31';
-- Tạo bảng phân vùng theo ngày và gom cụm theo loại giao dịch
CREATE OR REPLACE TABLE `bigdata-405714.fraud_detection.fraud_transactions_clustered`
PARTITION BY DATE(transaction_timestamp)
CLUSTER BY transaction_type AS
SELECT * FROM `bigdata-405714.fraud_detection.external_fraud_transactions`;

-- Truy vấn kiểm tra số lượng giao dịch gian lận theo loại 'TRANSFER'
SELECT count(*) as total_fraud_cases
FROM `bigdata-405714.fraud_detection.fraud_transactions_clustered`
WHERE DATE(transaction_timestamp) BETWEEN '2025-01-01' AND '2025-12-31'
  AND transaction_type = 'TRANSFER'
  AND is_fraud = 1;

SELECT table_name, partition_id, total_rows
FROM `fraud_detection.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'fraud_transactions_partitioned'
ORDER BY total_rows DESC;