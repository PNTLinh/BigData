## Project Overview
A comprehensive big data project analyzing Lending Club loan data for credit risk prediction using distributed computing concepts.

## Objectives
- Analyze loan default patterns
- Build ML models for risk prediction
- Create interactive dashboard
- Simulate distributed processing
- Demonstrate scalability and fault-tolerance

## Architecture
```
├── data/
│ ├── raw/ # Raw CSV files
│ ├── processed/ # Cleaned data
│ └── samples/ # Sample datasets
├── notebooks/
│ ├── 1_eda_analysis.ipynb # Exploratory analysis
│ └── 2_model_training.ipynb # ML modeling
├── src/
│ ├── ingestion/ # Data download scripts
│ ├── processing/ # Data cleaning & processing
│ └── models/ # ML model code
├── dashboard/
│ └── app.py # Streamlit dashboard
├── models/ # Saved ML models
├── reports/ # Generated reports & charts
└── docs
```

### 1. Setup Environment
```
# Clone repository
git clone <repo-url>
cd lending-club-project

python -m venv venv

venv\Scripts\activate

pip install -r requirements.txt
```

### 2. Load data
```
python src/ingestion/download_data.py
```

### 3. Run EDA
```
jupyter notebook notebooks/eda_analysis.ipynb
```

### 4. Run Data Cleaning
```
python src/processing/data_cleaning.py
```

### 5. Train Models
```
jupyter notebook notebooks/model_training.ipynb
```

### 6. Launch Dashboard
```
streamlit run src/dashboard/app.py
```

### 7. Simulate Distributed Processing
```
python src/processing/spark_processing.py
```