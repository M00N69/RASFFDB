import streamlit as st
import pandas as pd
import sqlite3
import requests
import datetime
from github import Github
from io import BytesIO
import os

# Configuration
st.set_page_config(
    page_title="RASFF Alerts Dashboard",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="expanded"
)

DB_PATH = "rasff_data.db"
GITHUB_REPO = "M00N69/RASFFDB"
DB_GITHUB_URL = "https://raw.githubusercontent.com/M00N69/RASFFDB/main/rasff_data.db"

# GitHub functions...
def download_github_db():
    response = requests.get(DB_GITHUB_URL)
    if response.status_code == 200:
        with open(DB_PATH, 'wb') as f:
            f.write(response.content)
        st.success("Database downloaded from GitHub")
    else:
        st.error("Failed to download database")

def push_to_github():
    try:
        g = Github(os.getenv("GITHUB_TOKEN"))
        repo = g.get_repo(GITHUB_REPO)
        with open(DB_PATH, 'rb') as f:
            repo.update_file(
                path=DB_PATH,
                message="Manual update via Streamlit",
                content=f.read(),
                sha=repo.get_contents(DB_PATH).sha
            )
        st.success("Database updated on GitHub")
    except Exception as e:
        st.error(f"GitHub error: {e}")

# Database functions...
def create_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS rasff_data (
            reference TEXT PRIMARY KEY,
            date TEXT,
            date_of_case DATETIME,
            notifying_country TEXT,
            country_origin TEXT,
            product_category TEXT,
            product_type TEXT,
            subject TEXT,
            hazard_substance TEXT,
            hazard_category TEXT,
            classification TEXT,
            risk_decision TEXT,
            distribution TEXT,
            attention TEXT,
            follow_up TEXT,
            year INTEGER,
            month INTEGER,
            week INTEGER
        )
    ''')
    conn.commit()
    conn.close()

def update_database_structure():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    columns = [col[1] for col in cursor.execute("PRAGMA table_info(rasff_data)").fetchall()]
    
    for col in ["year", "month", "week"]:
        if col not in columns:
            cursor.execute(f"ALTER TABLE rasff_data ADD COLUMN {col} INTEGER")
    
    conn.commit()
    conn.close()

# Data processing functions...
def process_excel_upload(file):
    try:
        xls = pd.ExcelFile(file)
        dfs = [pd.read_excel(xls, sheet_name=sn) for sn in xls.sheet_names]
        df = pd.concat(dfs, ignore_index=True)
        
        df['date_of_case'] = pd.to_datetime(df['date'], errors='coerce')
        df['year'] = df['date_of_case'].dt.year
        df['month'] = df['date_of_case'].dt.month
        df['week'] = df['date_of_case'].dt.isocalendar().week
        
        return df
    except Exception as e:
        st.error(f"Data processing error: {e}")
        return None

def update_from_uploaded_file(df):
    conn = sqlite3.connect(DB_PATH)
    existing_refs = pd.read_sql("SELECT reference FROM rasff_data", conn)["reference"].tolist()
    new_data = df[~df['reference'].isin(existing_refs)].dropna(subset=['reference'])
    
    if not new_data.empty:
        new_data.to_sql('rasff_data', conn, if_exists='append', index=False)
        st.success(f"{len(new_data)} new alerts added!")
    else:
        st.info("No new data found")
    conn.close()

# Main app logic...
def main():
    st.title("🚨 RASFF Alerts Dashboard")
    
    # Initialize database
    if not os.path.exists(DB_PATH):
        download_github_db()
    create_database()
    update_database_structure()
    
    # Sidebar controls
    st.sidebar.title("⚙️ Actions")
    if st.sidebar.button("Fetch latest GitHub database"):
        download_github_db()
    
    if st.sidebar.button("Push updates to GitHub"):
        if st.sidebar.checkbox("Confirm update"):
            push_to_github()
    
    # File upload section
    st.sidebar.title("📤 File Upload")
    uploaded_file = st.sidebar.file_uploader("Upload Excel/XLSX file", type=["xlsx", "xls"])
    if uploaded_file:
        if st.sidebar.button("Process uploaded file"):
            df = process_excel_upload(uploaded_file)
            if df is not None:
                update_from_uploaded_file(df)
                push_to_github()
    
    # Automatic update logic (keep your existing update_database() function here)
    
    # Display data
    df = pd.read_sql("SELECT * FROM rasff_data", sqlite3.connect(DB_PATH))
    st.write("## 📊 Data")
    st.dataframe(df.head(10))

if __name__ == "__main__":
    main()
