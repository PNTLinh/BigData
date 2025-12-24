import os
import zipfile
import pandas as pd
from pathlib import Path
import kaggle

os.environ['KAGGLE_USERNAME'] = "pntlinh2204" 
os.environ['KAGGLE_KEY'] = "KGAT_e4ee8d44cd57a900a76d7c56a4e19f24"

class DataDownloader:
    def __init__(self):
        try:
            self.project_dir = Path(__file__).resolve().parent.parent.parent
        except NameError:
            self.project_dir = Path.cwd() 
        
        self.raw_data_dir = self.project_dir / 'data' / 'raw'
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)

    def download_data(self):
        print("Load data")
        from kaggle.api.kaggle_api_extended import KaggleApi
        api = KaggleApi()
        api.authenticate()
        dataset_name = 'wordsforthewise/lending-club'
        api.dataset_download_files(
                dataset_name,
                path=str(self.raw_data_dir),
                unzip=True
            )
        print("Download và giải nén thành công!")
        
    
    def check_data(self):
    
        files = list(self.raw_data_dir.rglob('*.csv'))
        
        if files:
            print(f"\nTìm thấy {len(files)} file CSV:")
            for f in files:
                size_mb = f.stat().st_size / (1024*1024)
                print(f"  - {f.name} ({size_mb:.2f} MB)")
            
            main_file = max(files, key=lambda x: x.stat().st_size)
            print(f"\nĐang đọc sample từ file lớn nhất: {main_file.name}...")
            
            try:
                df = pd.read_csv(main_file, nrows=5)
                print(df.head())
                print(f"\nShape sample: {df.shape}")
                print("Cấu trúc cột:", list(df.columns[:10]), "...")
            except Exception as e:
                print(f"Không đọc được file CSV: {e}")
        else:
            print("Không tìm thấy file CSV nào! Có thể quá trình giải nén chưa xong hoặc lỗi.")

if __name__ == "__main__":
    downloader = DataDownloader()
    downloader.download_data()
    downloader.check_data()