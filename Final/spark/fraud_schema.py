import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types

if __name__ == "__main__":
    # Khởi tạo Spark Session chạy local để test chuyển đổi format
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("fraud_schema_conversion") \
        .getOrCreate()

    # Định nghĩa cấu trúc dữ liệu cho bài toán Gian lận tài chính (Financial Fraud)
    fraud_schema = types.StructType([
        types.StructField("step", types.IntegerType(), True),
        types.StructField("type", types.StringType(), True),
        types.StructField("amount", types.DoubleType(), True),
        types.StructField("nameOrig", types.StringType(), True),
        types.StructField("oldbalanceOrg", types.DoubleType(), True),
        types.StructField("newbalanceOrig", types.DoubleType(), True),
        types.StructField("nameDest", types.StringType(), True),
        types.StructField("oldbalanceDest", types.DoubleType(), True),
        types.StructField("newbalanceDest", types.DoubleType(), True),
        types.StructField("isFraud", types.IntegerType(), True),
        types.StructField("isFlaggedFraud", types.IntegerType(), True)
    ])

    # Giả lập quy trình chuyển đổi từ CSV sang Parquet cho dữ liệu Fraud
    # Thay thế vòng lặp taxi bằng vòng lặp theo part (như Linh đã cấu hình trong Prefect)
    dataset_name = "paysim"

    for part in range(1, 4):  # Giả sử Linh có 3 phần dữ liệu
        print(f'Đang chuyển đổi dữ liệu cho {dataset_name} part {part}')

        # Đường dẫn theo cấu trúc thư mục mới của dự án Fraud
        input_path = f'data/raw/transactions/{dataset_name}/part_{part:02d}/'
        output_path = f'data/pq/transactions/{dataset_name}/part_{part:02d}/'

        try:
            # Đọc dữ liệu CSV thô với Schema đã định nghĩa
            df_fraud = spark.read \
                .option("header", "true") \
                .schema(fraud_schema) \
                .csv(input_path)

            # Repartition để tối ưu hóa việc lưu trữ phân tán và ghi file Parquet
            df_fraud.repartition(4).write.parquet(output_path, mode='overwrite')
            print(f'Hoàn thành part {part}')
        except Exception as e:
            print(f'Lỗi khi xử lý part {part}: {str(e)}')
            continue