from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import os

# Cấu hình hệ thống
PROJECT_ID = 'bigdata-405714'
CONSUME_TOPIC_FRAUD = 'fraud_transactions'  # Đổi từ 'rides' sang topic tài chính
KAFKA_ADDRESS = "35.220.200.137"
KAFKA_BOOTSTRAP_SERVERS = f'{KAFKA_ADDRESS}:9092'

GCP_GCS_BUCKET = "dtc_data_lake_bigdata-405714"
GCS_STORAGE_PATH = 'gs://' + GCP_GCS_BUCKET + '/realtime_fraud'
CHECKPOINT_PATH = 'gs://' + GCP_GCS_BUCKET + '/realtime_fraud/checkpoint/'
CHECKPOINT_PATH_BQ = 'gs://' + GCP_GCS_BUCKET + '/realtime_fraud/checkpoint_bq/'

# Cập nhật FRAUD_SCHEMA cho bài toán tài chính
FRAUD_SCHEMA = T.StructType([
    T.StructField("step", T.IntegerType()),
    T.StructField("type", T.StringType()),
    T.StructField("amount", T.FloatType()),
    T.StructField("nameOrig", T.StringType()),
    T.StructField("oldbalanceOrg", T.FloatType()),
    T.StructField("newbalanceOrig", T.FloatType()),
    T.StructField("nameDest", T.StringType()),
    T.StructField("oldbalanceDest", T.FloatType()),
    T.StructField("newbalanceDest", T.FloatType()),
    T.StructField("isFraud", T.IntegerType()),
    T.StructField("isFlaggedFraud", T.IntegerType())
])


def read_from_kafka(consume_topic: str):
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", consume_topic) \
        .option("startingOffsets", "latest") \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .option("failOnDataLoss", "false") \
        .load()


def parse_fraud_from_kafka(df, schema):
    assert df.isStreaming is True, "DataFrame doesn't receive streaming data"
    df = df.selectExpr("CAST(value AS STRING)")

    # Giả định dữ liệu Kafka gửi dưới dạng CSV string phân tách bằng dấu phẩy
    col = F.split(df['value'], ',')

    for idx, field in enumerate(schema):
        df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))

    return df.select([field.name for field in schema])


def create_file_write_stream(stream, storage_path, checkpoint_path):
    return (stream
            .writeStream
            .format("parquet")
            .option("path", storage_path)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime="10 seconds")
            .outputMode("append"))


def create_bq_write_stream(stream, checkpoint_path):
    # Đẩy trực tiếp vào bảng Gold Layer trong BigQuery để Dashboard cập nhật liên tục
    return (stream
            .writeStream
            .format("bigquery")
            .option("table", f"{PROJECT_ID}.production.realtime_fraud_alerts")
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime="10 seconds")
            .outputMode("append"))


if __name__ == "__main__":
    spark = SparkSession.builder.appName('fraud-streaming-consumer').getOrCreate()

    # Cấu hình bucket tạm cho Dataproc BigQuery connector
    spark.conf.set('temporaryGcsBucket', 'dataproc-temp-asia-east2-285145462114-ku4fpzno')
    spark.sparkContext.setLogLevel('WARN')

    # 1. Đọc luồng dữ liệu từ Kafka
    df_consume = read_from_kafka(consume_topic=CONSUME_TOPIC_FRAUD)

    # 2. Parse dữ liệu giao dịch
    df_fraud = parse_fraud_from_kafka(df_consume, FRAUD_SCHEMA)

    # 3. Logic phát hiện gian lận nhanh: Lọc các giao dịch có isFraud = 1
    df_alerts = df_fraud.filter(F.col("isFraud") == 1)

    # 4. Ghi song song ra GCS (Data Lake) và BigQuery (Data Warehouse)
    query_gcs = create_file_write_stream(df_fraud, GCS_STORAGE_PATH, CHECKPOINT_PATH)
    query_bq = create_bq_write_stream(df_alerts, CHECKPOINT_PATH_BQ)

    query_gcs.start()
    query_bq.start()

    spark.streams.awaitAnyTermination()