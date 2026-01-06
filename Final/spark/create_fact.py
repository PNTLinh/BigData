import pyspark
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

PROJECT_ID = 'bigdata-405714'

parser = argparse.ArgumentParser()
parser.add_argument('--dataset', default='production', type=str)
args = parser.parse_args()


class FraudFactProcessor:
    def __init__(self, spark):
        self.spark = spark

    def load_cleaned_data(self):
        # Tải dữ liệu từ tầng Silver (đã làm sạch)
        input_table = f"{PROJECT_ID}.production.fact_transactions_cleaned"
        df = self.spark.read.format('bigquery').option('table', input_table).load()
        return df

    def read_fraud_dim_tables(self):
        # Đọc các bảng chiều phục vụ cho bài toán Fraud
        dim_dict = {}
        dim_dict['dim_accounts'] = self.spark.read.format('bigquery') \
            .option('table', f'{PROJECT_ID}.{args.dataset}.dim_accounts').load()
        dim_dict['dim_type'] = self.spark.read.format('bigquery') \
            .option('table', f'{PROJECT_ID}.{args.dataset}.dim_transaction_type').load()
        dim_dict['dim_datetime'] = self.spark.read.format('bigquery') \
            .option('table', f'{PROJECT_ID}.{args.dataset}.dim_datetime').load()
        return dim_dict

    def create_fact_table(self, cleaned_df, dim_accounts, dim_type, dim_datetime):
        # Sử dụng Broadcast Join để tối ưu hóa hiệu năng Big Data
        # Join bảng giao dịch với các bảng chiều dựa trên ID tài khoản, loại giao dịch và step
        fact_table = cleaned_df \
            .join(F.broadcast(dim_accounts).alias('orig'), cleaned_df.nameOrig == F.col('orig.account_id'), 'left') \
            .join(F.broadcast(dim_accounts).alias('dest'), cleaned_df.nameDest == F.col('dest.account_id'), 'left') \
            .join(F.broadcast(dim_type), on='type', 'left') \
            .join(F.broadcast(dim_datetime), cleaned_df.step == dim_datetime.datetime_id, 'left') \
            .select(
            F.col('transaction_timestamp'),
            F.col('orig.account_id').alias('origin_account_id'),
            F.col('dest.account_id').alias('dest_account_id'),
            'type_id',
            'amount',
            'oldbalanceOrg',
            'newbalanceOrig',
            'errorBalanceOrig',
            'isFraud'
        )
        return fact_table


# --- THI ĐIỂM CHẠY (EXECUTION) ---
spark = SparkSession.builder.appName('create_fraud_fact').getOrCreate()
# Cấu hình bucket tạm cho đầu ra BigQuery
spark.conf.set('temporaryGcsBucket', 'dataproc-temp-asia-east2-285145462114-ku4fpzno')

processor = FraudFactProcessor(spark)

# 1. Tải dữ liệu
cleaned_df = processor.load_cleaned_data()
dim_tables = processor.read_fraud_dim_tables()

# 2. Tạo bảng Fact cuối cùng (Gold Layer)
fact_table = processor.create_fact_table(
    cleaned_df,
    dim_tables['dim_accounts'],
    dim_tables['dim_type'],
    dim_tables['dim_datetime']
)

# 3. Ghi dữ liệu vào BigQuery Production
# Bảng fact_table này sẽ là nguồn cho Dashboard và Analytics SQL
fact_table.write.format('bigquery') \
    .option('table', f'{PROJECT_ID}.{args.dataset}.fact_table') \
    .mode('overwrite') \
    .save()

print(f"Đã hoàn thành tạo bảng Fact tại {args.dataset}.fact_table")