import pandas as pd
import os
from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(dataset_name: str, part: int) -> str:
    """
    Tải dữ liệu giao dịch tài chính từ Bronze Layer trên GCS về local
    """
    # Cấu trúc đường dẫn mới cho dự án Fraud Detection
    gcs_path = f"data/raw/transactions/{dataset_name}/fraud_data_part_{part:02}.parquet"
    gcs_block = GcsBucket.load("gcs-bucket")

    # Thiết lập thư mục local an toàn cho Windows
    # Sử dụng đường dẫn tuyệt đối để tránh lỗi folder không tồn tại
    local_base_path = Path("data").absolute()
    if not local_base_path.exists():
        local_base_path.mkdir(parents=True)

    gcs_block.get_directory(
        from_path=gcs_path,
        local_path=str(local_base_path)
    )

    # Trả về đường dẫn file đã tải về
    return str(local_base_path / gcs_path)


@task()
def transform_fraud_data(path: str) -> pd.DataFrame:
    """
    Đọc dữ liệu và chuẩn hóa các trường quan trọng
    """
    df = pd.read_parquet(path)

    # Đảm bảo nhãn gian lận đúng kiểu dữ liệu để BigQuery không bị lỗi schema
    if 'isFraud' in df.columns:
        df['isFraud'] = df['isFraud'].astype(int)

    print(f"Đã xử lý xong {len(df)} dòng dữ liệu từ {path}")
    return df


@task()
def write_bq(df: pd.DataFrame, table_name: str) -> int:
    """
    Ghi dữ liệu vào BigQuery Production Dataset (tầng Silver/Gold)
    """
    credentials = GcpCredentials.load("gcp-creds")

    # Đổi destination_table sang dataset 'production' phù hợp với dự án Fraud
    df.to_gbq(
        destination_table=f"production.{table_name}",
        project_id="bigdata-405714",
        credentials=credentials.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )
    return len(df)


@flow()
def etl_gcs_to_bq_fraud(part: int, dataset_name: str, table_name: str) -> int:
    """
    Flow con xử lý từng phần dữ liệu đơn lẻ
    """
    local_path = extract_from_gcs(dataset_name, part)
    df = transform_fraud_data(local_path)
    row_count = write_bq(df, table_name=table_name)
    return row_count


@flow(log_prints=True)
def el_parent_fraud_gcs_to_bq(
        parts: list[int] = [1, 2],
        dataset_name: str = "paysim",
        table_name: str = "fact_transactions_raw"
):
    """
    Flow chính quản lý việc nạp toàn bộ dữ liệu giao dịch vào BigQuery
    """
    total_rows = 0
    for part in parts:
        rows = etl_gcs_to_bq_fraud(part, dataset_name, table_name)
        total_rows += rows

    print(f"Hoàn thành! Tổng cộng {total_rows} giao dịch đã được nạp vào BigQuery.")


if __name__ == "__main__":
    # Ví dụ: Nạp 3 phần dữ liệu đầu tiên của bộ dữ liệu paysim
    el_parent_fraud_gcs_to_bq(parts=[1, 2, 3], dataset_name="paysim", table_name="fact_transactions_raw")