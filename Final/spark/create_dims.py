import pyspark
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

PROJECT_ID = 'bigdata-405714'

parser = argparse.ArgumentParser()
parser.add_argument('--dataset', default='production', type=str)
args = parser.parse_args()


class FraudDataProcessor:
    def __init__(self, spark):
        self.spark = spark

    def load_data(self):
        # Tải dữ liệu đã được làm sạch từ tầng Silver
        input_table = f"{PROJECT_ID}.production.fact_transactions_cleaned"
        df = self.spark.read.format('bigquery').option('table', input_table).load()
        return df

    def create_dim_accounts(self, combined_df):
        # Tạo bảng định danh tài khoản và phân loại (Customer/Merchant)
        orig_accounts = combined_df.select(F.col('nameOrig').alias('account_id'))
        dest_accounts = combined_df.select(F.col('nameDest').alias('account_id'))

        dim_accounts = orig_accounts.union(dest_accounts).distinct() \
            .withColumn('account_type',
                        F.when(F.col('account_id').startswith('C'), 'Customer')
                        .otherwise('Merchant'))
        return dim_accounts

    def create_dim_transaction_type(self, combined_df):
        # Phân loại các hành vi giao dịch tài chính
        dim_type = combined_df.select('type').distinct() \
            .withColumn('type_id', F.monotonically_increasing_id())
        return dim_type

    def create_dim_datetime(self, combined_df):
        # Chuyển đổi 'step' thành các chiều thời gian chi tiết
        dim_datetime = combined_df.select('step').distinct() \
            .withColumn('transaction_timestamp', F.timestamp_seconds(F.col('step') * 3600)) \
            .select(
            F.col('step').alias('datetime_id'),
            'transaction_timestamp',
            F.year('transaction_timestamp').alias('year'),
            F.month('transaction_timestamp').alias('month'),
            F.dayofmonth('transaction_timestamp').alias('day'),
            F.hour('transaction_timestamp').alias('hour'),
            F.dayofweek('transaction_timestamp').alias('weekday')
        )
        return dim_datetime


# --- MAIN EXECUTION ---
spark = SparkSession.builder.appName('create_fraud_dims').getOrCreate()
spark.conf.set('temporaryGcsBucket', 'dataproc-temp-asia-east2-285145462114-ku4fpzno')

processor = FraudDataProcessor(spark)
combined_df = processor.load_data()

# Tạo các bảng Dimension
dim_accounts = processor.create_dim_accounts(combined_df)
dim_type = processor.create_dim_transaction_type(combined_df)
dim_datetime = processor.create_dim_datetime(combined_df)

# Ghi dữ liệu lên BigQuery Production Layer
dataframe_dict = {
    'dim_accounts': dim_accounts,
    'dim_transaction_type': dim_type,
    'dim_datetime': dim_datetime
}

for name, df in dataframe_dict.items():
    df.write.format('bigquery').option('table', f'{PROJECT_ID}.{args.dataset}.{name}').mode('overwrite').save()
    print(f"Đã cập nhật bảng chiều: {name}")