import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder, StandardScaler
import pickle
import os

class DataPreprocessor:
    def __init__(self, df):
        self.df = df.copy()
        self.label_encoders = {}
        self.scaler = StandardScaler()
        
    def clean_data(self):
        print("Starting data cleaning pipeline...")
        
        self.handle_missing_values()
        
        self.convert_data_types()
        
        self.create_features()
        
        self.remove_outliers()
        
        self.encode_categorical()
        
        print(f"Data cleaning completed. Shape: {self.df.shape}")
        return self.df
    
    def handle_missing_values(self):
        print("  Handling missing values...")
        
        cols_to_drop = [
            'mths_since_last_delinq', 'mths_since_last_record',
            'mths_since_last_major_derog', 'desc', 'next_pymnt_d'
        ]
        
        cols_to_drop = [col for col in cols_to_drop if col in self.df.columns]
        self.df = self.df.drop(columns=cols_to_drop)
        
        numerical_cols = self.df.select_dtypes(include=[np.number]).columns
        for col in numerical_cols:
            if self.df[col].isnull().sum() > 0:
                self.df[col] = self.df[col].fillna(self.df[col].median())
        
        categorical_cols = self.df.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            if self.df[col].isnull().sum() > 0:
                self.df[col] = self.df[col].fillna(self.df[col].mode()[0])
    
    def convert_data_types(self):
        print("  Converting data types...")
        
        percent_cols = ['int_rate', 'revol_util']
        for col in percent_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].astype(str).str.replace('%', '').astype(float)
        
        if 'issue_d' in self.df.columns:
            self.df['issue_date'] = pd.to_datetime(self.df['issue_d'], format='%b-%Y')
            self.df['issue_year'] = self.df['issue_date'].dt.year
            self.df['issue_month'] = self.df['issue_date'].dt.month
        
        if 'earliest_cr_line' in self.df.columns:
            self.df['earliest_cr_line_date'] = pd.to_datetime(
                self.df['earliest_cr_line'], format='%b-%Y', errors='coerce'
            )
    
    def create_features(self):
        print("  Creating new features...")
        
        if 'annual_inc' in self.df.columns and 'loan_amnt' in self.df.columns:
            self.df['loan_to_income'] = self.df['loan_amnt'] / self.df['annual_inc']
        
        if 'revol_bal' in self.df.columns and 'annual_inc' in self.df.columns:
            self.df['revol_util_to_income'] = self.df['revol_bal'] / self.df['annual_inc']
        
        if 'grade' in self.df.columns:
            grade_mapping = {'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5, 'F': 6, 'G': 7}
            self.df['grade_numeric'] = self.df['grade'].map(grade_mapping)
        
        if 'emp_length' in self.df.columns:
            self.df['emp_length_numeric'] = self.df['emp_length'].str.extract('(\d+)').astype(float)
            self.df['emp_length_numeric'] = self.df['emp_length_numeric'].fillna(0)
        
        if 'term' in self.df.columns:
            self.df['term_numeric'] = self.df['term'].str.extract('(\d+)').astype(int)
        
        if 'loan_status' in self.df.columns:
            default_statuses = ['Charged Off', 'Default', 'Late (31-120 days)']
            self.df['is_default'] = self.df['loan_status'].apply(
                lambda x: 1 if x in default_statuses else 0
            )
        
        risk_factors = []
        if 'grade_numeric' in self.df.columns:
            risk_factors.append(self.df['grade_numeric'] * 20)
        if 'dti' in self.df.columns:
            risk_factors.append(self.df['dti'])
        if 'delinq_2yrs' in self.df.columns:
            risk_factors.append(self.df['delinq_2yrs'] * 10)
        
        if risk_factors:
            self.df['risk_score'] = sum(risk_factors) / len(risk_factors)
    
    def remove_outliers(self, threshold=3):
        print("  Removing outliers...")
        
        numerical_cols = self.df.select_dtypes(include=[np.number]).columns
        
        for col in numerical_cols:
            if col not in ['is_default', 'issue_year', 'issue_month']:
                Q1 = self.df[col].quantile(0.25)
                Q3 = self.df[col].quantile(0.75)
                IQR = Q3 - Q1
                
                lower_bound = Q1 - threshold * IQR
                upper_bound = Q3 + threshold * IQR
                
                self.df[col] = np.where(self.df[col] < lower_bound, lower_bound, self.df[col])
                self.df[col] = np.where(self.df[col] > upper_bound, upper_bound, self.df[col])
    
    def encode_categorical(self):
        print("  Encoding categorical variables...")
        
        categorical_cols = ['home_ownership', 'verification_status', 'purpose', 
                          'addr_state', 'application_type']
        
        categorical_cols = [col for col in categorical_cols if col in self.df.columns]
        
        for col in categorical_cols:
            le = LabelEncoder()
            self.df[col + '_encoded'] = le.fit_transform(self.df[col].astype(str))
            self.label_encoders[col] = le
        
        if 'purpose' in self.df.columns:
            top_purposes = self.df['purpose'].value_counts().head(10).index
            for purpose in top_purposes:
                self.df[f'purpose_{purpose}'] = (self.df['purpose'] == purpose).astype(int)
    
    def save_clean_data(self, output_path):
        print(f"Saving cleaned data to {output_path}")
        
        self.df.to_csv(output_path, index=False)
        
        with open('models/preprocessing_objects.pkl', 'wb') as f:
            pickle.dump({
                'label_encoders': self.label_encoders,
                'scaler': self.scaler
            }, f)
        
        print("Cleaning objects saved successfully!")

if __name__ == "__main__":
    df = pd.read_csv('data/processed/eda_processed_data.csv')

    preprocessor = DataPreprocessor(df)
    cleaned_df = preprocessor.clean_data()
    
    preprocessor.save_clean_data('data/processed/cleaned_data.csv')
    
    cleaned_df.head(10000).to_csv('data/processed/cleaned_sample.csv', index=False)
    
    print("\nData cleaning pipeline completed!")