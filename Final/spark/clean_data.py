#!/usr/bin/env python
# coding: utf-8

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ID = 'bigdata-405714'

# Các cột đặc trưng của bộ dữ liệu gian lận tài chính
FRAUD_COLUMNS = [
    'step',
    'type',
    'amount',
    'nameOrig',
    'oldbalanceOrg',
    'newbalanceOrig',
    'nameDest',
    'oldbalanceDest',
    'newbalanceDest',
    'isFraud'
]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_table', default='fact_transactions_raw', type=str)
    args = parser.parse_args()
    return args


def process_fraud_table(input_table, output):
    # Đọc dữ liệu từ BigQuery Bronze Layer
    df = spark.read.format('bigquery').option('table', input_table).load()

    # Chọn các cột cần thiết và thực hiện làm sạch (Data Cleaning)
    df_cleaned = df.select(FRAUD_COLUMNS).dropna() \
        .filter(F.col('amount') > 0) \
        .filter(F.col('oldbalanceOrg') >= 0) \
        .filter(F.col('newbalanceOrig') >= 0) \
        .filter(F.col('type').isin(['TRANSFER', 'CASH_OUT', 'CASH_IN', 'DEBIT', 'PAYMENT'])) \
        .dropDuplicates()

    # Thêm đặc trưng mới: Sự chênh lệch số dư (Balance Error)
    df_cleaned = df_cleaned.withColumn(
        'errorBalanceOrig',
        F.round(F.abs(F.col('oldbalanceOrg') - F.col('amount') - F.col('newbalanceOrig')), 2)
    )

    # Ghi dữ liệu đã làm sạch vào tầng Silver trên BigQuery
    df_cleaned.write.format('bigquery') \
        .option('table', output) \
        .mode('overwrite') \
        .save()

    print(f"Hoàn thành làm sạch dữ liệu. Kết quả lưu tại: {output}")


def main(args):
    input_full_path = f"{PROJECT_ID}.production.{args.input_table}"
    output_full_path = f"{PROJECT_ID}.production.fact_transactions_cleaned"

    process_fraud_table(input_full_path, output_full_path)


if __name__ == "__main__":
    args = parse_args()

    # Khởi tạo Spark Session cho Dataproc
    spark = SparkSession.builder \
        .appName('fraud_data_cleaning') \
        .getOrCreate()

    # Cấu hình bucket tạm cho Dataproc BigQuery connector
    spark.conf.set('temporaryGcsBucket', 'dataproc-temp-asia-east2-285145462114-ku4fpzno')

    main(args)