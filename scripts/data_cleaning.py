import pandas as pd
import numpy as np

class DataCleaning:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def handle_missing_values(self, strategy='mean', columns=None):
        """
        Handle missing values with specified strategy: 'mean', 'median', 'mode', 'drop', 'ffill', or 'bfill'
        """
        df = self.df
        if columns is None:
            columns = df.columns

        for col in columns:
            if df[col].isnull().any():  # Only process columns with missing values
                if strategy == 'mean' and df[col].dtype in [np.float64, np.int64]:
                    df[col].fillna(df[col].mean(), inplace=True)
                elif strategy == 'median' and df[col].dtype in [np.float64, np.int64]:
                    df[col].fillna(df[col].median(), inplace=True)
                elif strategy == 'mode':
                    mode_values = df[col].mode()
                    if len(mode_values) > 0:
                        df[col].fillna(mode_values[0], inplace=True)
                elif strategy == 'ffill':
                    df[col].fillna(method='ffill', inplace=True)
                elif strategy == 'bfill':
                    df[col].fillna(method='bfill', inplace=True)
                elif strategy == 'drop':
                    df.dropna(subset=[col], inplace=True)
                else:
                    # For non-numeric columns or unsupported strategies, use forward fill
                    df[col].fillna(method='ffill', inplace=True)
        
        self.df = df

    def remove_duplicates(self):
        """
        Remove duplicate rows from the dataframe
        """
        self.df.drop_duplicates(inplace=True)

    def fix_data_types(self, column_types: dict):
        """
        Fix data types of columns using a dictionary {column_name: data_type}
        Example: {'age': 'int', 'date': 'datetime64[ns]'}
        """
        for col, dtype in column_types.items():
            try:
                if dtype == 'datetime':
                    self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                else:
                    self.df[col] = self.df[col].astype(dtype)
            except Exception as e:
                print(f"Could not convert column '{col}' to type '{dtype}': {e}")

    def clean_column_names(self):
        """
        Standardize column names by making them lowercase and replacing spaces with underscores
        """
        self.df.columns = (
            self.df.columns
            .str.strip()
            .str.lower()
            .str.replace(' ', '_')
            .str.replace(r'[^\w\s]', '', regex=True)
        )

    def remove_outliers(self, columns, method='iqr', threshold=1.5):
        """
        Remove outliers from specified numeric columns using the IQR or Z-score method
        """
        if method == 'iqr':
            for col in columns:
                if col in self.df.columns and self.df[col].dtype in [np.float64, np.int64]:
                    Q1 = self.df[col].quantile(0.25)
                    Q3 = self.df[col].quantile(0.75)
                    IQR = Q3 - Q1
                    self.df = self.df[
                        ~((self.df[col] < (Q1 - threshold * IQR)) | (self.df[col] > (Q3 + threshold * IQR)))
                    ]
        elif method == 'zscore':
            try:
                from scipy.stats import zscore
                numeric_cols = [col for col in columns if col in self.df.columns and self.df[col].dtype in [np.float64, np.int64]]
                if numeric_cols:
                    z_scores = np.abs(zscore(self.df[numeric_cols]))
                    self.df = self.df[(z_scores < threshold).all(axis=1)]
            except ImportError:
                print("scipy not available, skipping zscore outlier removal")

    def clean_data(self):
        """
        Perform complete cleaning: remove duplicates, clean column names, and handle missing values
        """
        self.remove_duplicates()
        self.clean_column_names()
        self.handle_missing_values(strategy='ffill')  # Add default missing value handling

    def get_clean_data(self):
        return self.df