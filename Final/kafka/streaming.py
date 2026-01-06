from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import os

# Cấu hình hệ thống
KAFKA_ADDRESS = "35.220.200.137"
KAFKA_BOOTSTRAP_SERVERS = f'{KAFKA_ADDRESS}:9092'
PRODUCE_TOPIC_FRAUD_CSV = CONSUME_TOPIC_FRAUD_CSV = 'fraud_transactions'
TOPIC_WINDOWED_FRAUD_STATS = 'fraud_stats_windowed'

# Sử dụng FRAUD_SCHEMA đã định nghĩa cho bài toán tài chính
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
        .option("checkpointLocation", "/checkpoint/fraud_input") \
        .option("failOnDataLoss", "false") \
        .load()


def parse_fraud_message(df, schema):
    """ Parse nội dung từ Kafka message thành các cột tài chính """
    assert df.isStreaming is True
    df = df.selectExpr("CAST(value AS STRING)")
    col = F.split(df['value'], ',')
    for idx, field in enumerate(schema):
        df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))

    # Tạo timestamp giả lập từ step để chạy Window function
    return df.withColumn("timestamp", F.timestamp_seconds(F.col("step") * 3600))


def sink_console(df, output_mode='append'):
    return df.writeStream \
        .outputMode(output_mode) \
        .trigger(processingTime='5 seconds') \
        .format("console") \
        .option("truncate", False) \
        .start()


def sink_kafka(df, topic):
    return df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .outputMode('complete') \
        .option("topic", topic) \
        .option("checkpointLocation", "/checkpoint/fraud_output") \
        .start()


def op_windowed_fraud_analysis(df, window_duration, slide_duration):
    """ Thống kê tổng tiền gian lận theo loại giao dịch trong cửa sổ thời gian """
    return df.groupBy(
        F.window(timeColumn=df.timestamp, windowDuration=window_duration, slideDuration=slide_duration),
        df.type
    ).agg(
        F.count("*").alias("total_transactions"),
        F.sum("isFraud").alias("fraud_cases"),
        F.sum("amount").alias("total_amount")
    )


if __name__ == "__main__":
    os.environ['KAFKA_ADDRESS'] = KAFKA_ADDRESS
    spark = SparkSession.builder.appName('fraud-streaming-analysis').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # 1. Đọc stream
    df_raw = read_from_kafka(consume_topic=CONSUME_TOPIC_FRAUD_CSV)

    # 2. Parse dữ liệu & Thêm timestamp
    df_fraud = parse_fraud_message(df_raw, FRAUD_SCHEMA)

    # 3. Phân tích: Đếm số vụ gian lận theo khung giờ và loại giao dịch
    df_stats = op_windowed_fraud_analysis(df_fraud, "1 hour", "30 minutes")

    # 4. Sink ra Console để debug
    sink_console(df_fraud, output_mode='append')
    sink_console(df_stats, output_mode='complete')

    # 5. Sink kết quả phân tích ngược lại Kafka
    df_kafka_output = df_stats.select(
        F.col("type").alias("key"),
        F.concat_ws(", ", "fraud_cases", "total_amount").alias("value")
    )
    kafka_query = sink_kafka(df_kafka_output, TOPIC_WINDOWED_FRAUD_STATS)

    spark.streams.awaitAnyTermination()