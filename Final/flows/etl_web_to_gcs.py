import pandas as pd
import os
from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """
    Tải tập dữ liệu giao dịch tài chính từ nguồn web (GitHub/Kaggle)
    """
    print(f"Đang tải dữ liệu từ: {dataset_url}")
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Xử lý kiểu dữ liệu đặc thù cho bài toán Fraud Detection
    """
    # Chuyển đổi nhãn gian lận và đánh dấu hệ thống sang Int64 (hỗ trợ Null)
    if 'isFraud' in df.columns:
        df['isFraud'] = df['isFraud'].astype('Int64')
    if 'isFlaggedFraud' in df.columns:
        df['isFlaggedFraud'] = df['isFlaggedFraud'].astype('Int64')

    # Đảm bảo các trường số tiền và số dư là float64
    float_cols = ['amount', 'oldbalanceOrg', 'newbalanceOrig', 'oldbalanceDest', 'newbalanceDest']
    for col in float_cols:
        if col in df.columns:
            df[col] = df[col].astype('float64')

    print(df.head(2))
    print(f"Số lượng bản ghi: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, dataset_name: str, part: int) -> Path:
    """
    Ghi dữ liệu thành file Parquet cục bộ (Bronze Layer format)
    """
    # Tổ chức thư mục theo kiến trúc dữ liệu tài chính
    local_dir = Path(f"data/raw/transactions/{dataset_name}")
    if not local_dir.exists():
        local_dir.mkdir(parents=True)

    filename = f"fraud_data_part_{part:02}.parquet"
    path = local_dir / filename

    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path):
    """
    Tải file Parquet lên GcsBucket (Bronze Layer)
    """
    gcs_block = GcsBucket.load("gcs-bucket")
    # Sử dụng forward slash cho đường dẫn trên GCS để tránh lỗi Windows path
    gcs_path = str(path).replace('\\', '/')

    gcs_block.upload_from_path(
        from_path=str(path),
        to_path=gcs_path
    )
    return


@flow()
def etl_web_to_gcs_fraud(dataset_name: str, part: int, base_url: str):
    """
    Luồng ETL con tải một phần dữ liệu lên Cloud
    """
    # Giả định file nguồn là csv.gz
    dataset_url = f"{base_url}/fraud_data_part_{part:02}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, dataset_name, part)
    write_gcs(path)


@flow()
def etl_parent_fraud_web_to_gcs(
        parts: list[int] = [1, 2],
        dataset_name: str = "paysim",
        base_url: str = "https://raw.githubusercontent.com/your-repo/main/data"
):
    """
    Luồng chính quản lý việc tải toàn bộ các phần dữ liệu gian lận
    """
    for part in parts:
        etl_web_to_gcs_fraud(dataset_name, part, base_url)


if __name__ == "__main__":
    # Tham số chạy thử nghiệm cho 3 phần dữ liệu
    etl_parent_fraud_web_to_gcs(parts=[1, 2, 3], dataset_name="paysim")