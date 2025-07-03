import os
import pandas as pd
import requests
from sqlalchemy import create_engine

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data")

class DataIngestion:
    def __init__(self, db_url=None):
        """Initialize data ingestion with an optional database connection."""
        self.engine = create_engine(db_url) if db_url else None

    def load_csv(self, file_name):
        """Loads a CSV file into a DataFrame."""
        file_path = os.path.join(DATA_DIR, file_name)
        try:
            df = pd.read_csv(file_path)
            print(f"✅ CSV Loaded Successfully: {file_path}")
            return df
        except Exception as e:
            print(f"❌ Error loading CSV: {e}")
            return None

    def load_excel(self, file_name, sheet_name=0):
        """Loads an Excel file into a DataFrame."""
        file_path = os.path.join(DATA_DIR, file_name)
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            print(f"✅ Excel Loaded Successfully: {file_path}")
            return df
        except Exception as e:
            print(f"❌ Error loading Excel: {e}")
            return None

    def connect_database(self, db_url):
        """Establishes a database connection."""
        try:
            self.engine = create_engine(db_url)
            print("✅ Database Connection Successful")
        except Exception as e:
            print(f"❌ Error Connecting to Database: {e}")

    def load_from_database(self, query):
        """Fetches data from a database using SQL."""
        if not self.engine:
            print("❌ No database connection. Call connect_database() first.")
            return None
        try:
            df = pd.read_sql(query, self.engine)
            print("✅ Data Loaded from Database Successfully")
            return df
        except Exception as e:
            print(f"❌ Error loading data from database: {e}")
            return None

    def fetch_from_api(self, api_url, params=None):
        """Fetches data from an API and returns it as a DataFrame."""
        try:
            response = requests.get(api_url, params=params)
            response.raise_for_status()
            data = response.json()
            df = pd.DataFrame(data)
            print(f"✅ API Data Fetched Successfully: {api_url}")
            return df
        except Exception as e:
            print(f"❌ Error fetching data from API: {e}")
            return None