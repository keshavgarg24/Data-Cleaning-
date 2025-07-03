import streamlit as st
import requests
import pandas as pd
import json
from io import StringIO

# FAST API URL
FASTAPI_URL = "http://127.0.0.1:8000"

# Streamlit UI Configuration
st.set_page_config(page_title="AutoData - Cleaning Automation", layout="wide")

st.markdown("<h1 style='text-align: center; color: #6C63FF;'>🧹 AutoData - Data Cleaning Automation</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: grey;'>Automate data cleaning using Rule-based and AI-driven techniques</p>", unsafe_allow_html=True)

st.sidebar.markdown("## 🔍 Select Data Source")
data_source = st.sidebar.selectbox("Choose from the following sources:",
                                   ["CSV/Excel File", "Database Query", "API Data"])

st.markdown("---")

#CSV/Excel Upload
if data_source == "CSV/Excel File":
    st.header("📁 Upload CSV or Excel File")
    uploaded_file = st.file_uploader("Upload your data file here", type=["csv", "xlsx", "xls"])

    if uploaded_file is not None:
        file_extension = uploaded_file.name.split('.')[-1]
        if file_extension == "csv":
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)

        st.success("✅ File Uploaded Successfully!")
        st.markdown("### 🔍 Raw Data Preview")
        st.dataframe(df, use_container_width=True)

    if uploaded_file and st.button("🚀 Clean Data"):
        with st.spinner("Cleaning your data with AI magic..."):
            files = {"file": (uploaded_file.name, uploaded_file.getvalue())}
            response = requests.post(f"{FASTAPI_URL}/clean_data/", files=files)

        if response.status_code == 200:
            st.success("✅ Data Cleaned Successfully!")
            st.markdown("### 🧾 Cleaned Data")
            try:
                cleaned_data_raw = response.json()["cleaned_data"]
                cleaned_data = pd.DataFrame(json.loads(cleaned_data_raw)) if isinstance(cleaned_data_raw, str) else pd.DataFrame(cleaned_data_raw)
                st.dataframe(cleaned_data, use_container_width=True)
            except Exception as e:
                st.error(f"❌ Error converting data to DataFrame: {e}")
        else:
            st.error(f"❌ Cleaning failed: {response.text}")

#Database Query
elif data_source == "Database Query":
    st.header("🗄️ Query from a Database")
    db_url = st.text_input("🔗 Enter your Database URL", "postgresql://postgres:0707@localhost:5433/demodb")
    query = st.text_area("🧾 Write your SQL Query", "SELECT * FROM my_table")

    if st.button("🚀 Fetch and Clean Data"):
        with st.spinner("Executing query and cleaning data..."):
            response = requests.post(f"{FASTAPI_URL}/clean_db/", json={"query": query, "db_url": db_url})

        if response.status_code == 200:
            st.success("✅ Data Cleaned Successfully!")
            try:
                cleaned_data_raw = response.json()["cleaned_data"]
                cleaned_data = pd.DataFrame(json.loads(cleaned_data_raw)) if isinstance(cleaned_data_raw, str) else pd.DataFrame(cleaned_data_raw)
                st.markdown("### 🧾 Cleaned Data")
                st.dataframe(cleaned_data, use_container_width=True)
            except Exception as e:
                st.error(f"❌ Error converting data to DataFrame: {e}")
        else:
            st.error(f"❌ Failed to fetch/clean data from database: {response.text}")

#API Data
elif data_source == "API Data":
    st.header("🌐 Fetch from an API Endpoint")
    api_url = st.text_input("🔗 Enter API Endpoint", "https://jsonplaceholder.typicode.com/posts")

    if st.button("🚀 Fetch and Clean Data"):
        with st.spinner("Fetching and cleaning API data..."):
            response = requests.post(f"{FASTAPI_URL}/clean_api/", json={"api_url": api_url})

        if response.status_code == 200:
            st.success("✅ API Data Cleaned Successfully!")
            try:
                cleaned_data_raw = response.json()["cleaned_data"]
                cleaned_data = pd.DataFrame(json.loads(cleaned_data_raw)) if isinstance(cleaned_data_raw, str) else pd.DataFrame(cleaned_data_raw)
                st.markdown("### 🧾 Cleaned Data")
                st.dataframe(cleaned_data, use_container_width=True)
            except Exception as e:
                st.error(f"❌ Error converting data to DataFrame: {e}")
        else:
            st.error(f"❌ Failed to clean API data: {response.text}")

#Footer
st.markdown("""
<br><hr><br>
<div style='text-align: center'>
    <p>🚀 Built by <a href="https://github.com/keshavgarg24" target="_blank">Keshav</a> to simplify life for Data Engineers 💡</p>
</div>
""", unsafe_allow_html=True)
