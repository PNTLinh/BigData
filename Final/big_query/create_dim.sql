-- 1. Tạo bảng Dimension cho Tài khoản (Accounts)
CREATE OR REPLACE TABLE bigdata-405714.production.dim_accounts AS
SELECT DISTINCT
    nameOrig AS account_id,
    -- Giả định logic phân loại khách hàng đơn giản
    CASE
        WHEN nameOrig LIKE 'C%' THEN 'Customer'
        WHEN nameOrig LIKE 'M%' THEN 'Merchant'
        ELSE 'Unknown'
    END AS account_type
FROM `bigdata-405714.trips_data_all.external_fraud_transactions`;

-- 2. Tạo bảng Dimension cho Thời gian (Datetime)
CREATE OR REPLACE TABLE bigdata-405714.production.dim_datetime AS
SELECT
    DISTINCT step AS datetime_id, -- Trong bộ Paysim, step đại diện cho giờ
    TIMESTAMP_SECONDS(step * 3600) AS transaction_timestamp,
    EXTRACT(DATE FROM TIMESTAMP_SECONDS(step * 3600)) AS transaction_date,
    EXTRACT(HOUR FROM TIMESTAMP_SECONDS(step * 3600)) AS transaction_hour
FROM `bigdata-405714.trips_data_all.external_fraud_transactions`;

-- 3. Tạo bảng Dimension cho Chi tiết Giao dịch (Transaction Details)
CREATE OR REPLACE TABLE bigdata-405714.production.dim_transaction_details AS
SELECT DISTINCT
    GENERATE_UUID() AS detail_id,
    type AS transaction_type,
    isFlaggedFraud AS system_flagged_initial
FROM `bigdata-405714.trips_data_all.external_fraud_transactions`;

-- 4. Tạo bảng Fact chính (Fact Transactions)
-- Bảng này sẽ lưu trữ các khóa ngoại và các giá trị số (measures)
CREATE OR REPLACE TABLE bigdata-405714.production.fact_transactions
PARTITION BY DATE(transaction_timestamp) AS
SELECT
    GENERATE_UUID() AS transaction_id,
    step AS datetime_id,
    nameOrig AS origin_account_id,
    nameDest AS dest_account_id,
    type AS transaction_type,
    amount,
    oldbalanceOrg,
    newbalanceOrig,
    oldbalanceDest,
    newbalanceDest,
    isFraud,
    -- Join để lấy timestamp phục vụ việc Partitioning
    TIMESTAMP_SECONDS(step * 3600) AS transaction_timestamp
FROM `bigdata-405714.trips_data_all.external_fraud_transactions`;