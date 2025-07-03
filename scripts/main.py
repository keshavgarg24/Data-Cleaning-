from data_ingestion import DataIngestion
from data_cleaning import DataCleaning
from ai_agent import AIAgent

# Database Configuration
DB_USER = "postgres"
DB_PASSWORD = "0707"
DB_HOST = "localhost"
DB_PORT = "5433"
DB_NAME = "demodb"

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Initialising components
ingestion = DataIngestion(DB_URL)
cleaner = DataCleaning()
ai_agent = AIAgent()

# Load and Clean CSV Data
df_csv = ingestion.load_csv("sample_data.csv")
if df_csv is not None:
    print("\n Cleaning CSV Data...")
    df_csv = cleaner.clean_data(df_csv)
    df_csv = ai_agent.process_data(df_csv)
    print("Cleaned CSV Data\n", df_csv)

# Load and Clean Excel Data
df_excel = ingestion.load_excel("sample_data.xlsx")
if df_excel is not None:
    print("\n Cleaning Excel Data...")
    df_excel = cleaner.clean_data(df_excel)
    df_excel = ai_agent.process_data(df_excel)
    print("Cleaned Excel Data\n", df_excel)

# Load and Clean Data from Database
df_db = ingestion.load_from_database("SELECT * FROM my_table")
if df_db is not None:
    print("\n Cleaning Database Data...")
    df_db = cleaner.clean_data(df_db)
    df_db = ai_agent.process_data(df_db)
    print("Cleaned Database Data\n", df_db)

# Fetch and Clean Data from API
API_URL  = "https://jsonplaceholder.typicode.com/posts"
df_api = ingestion.fetch_from_api(API_URL)
if df_api is not None:
    print("\n Cleaning API Data...")
    df_api = df_api.head(20)  # Limiting to 20 rows for processing
    if "body" in df_api.columns:
        df_api["body"] = df_api["body"].apply(lambda x: x[:100] + "..." if isinstance(x,str) else x)  # Truncate long text
    df_api = cleaner.clean_data(df_api)
    df_api = ai_agent.process_data(df_api)
    print("Cleaned API Data\n", df_api)
