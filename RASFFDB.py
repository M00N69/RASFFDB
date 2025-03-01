import streamlit as st
import pandas as pd
import sqlite3
import requests
import datetime
from github import Github
from io import BytesIO
import os

# Configuration Streamlit
st.set_page_config(
    page_title="🚨 RASFF Alerts Dashboard",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Constantes
DB_PATH = "rasff_data.db"
GITHUB_REPO = "M00N69/RASFFDB"
DB_GITHUB_URL = "https://raw.githubusercontent.com/M00N69/RASFFDB/main/rasff_data.db"

# --- FONCTIONS GITHUB ---
def download_github_db():
    response = requests.get(DB_GITHUB_URL)
    if response.status_code == 200:
        with open(DB_PATH, 'wb') as f:
            f.write(response.content)
        st.success("Base GitHub téléchargée")
    else:
        st.error("Échec du téléchargement")

def push_to_github():
    try:
        g = Github(os.getenv("GITHUB_TOKEN"))
        repo = g.get_repo(GITHUB_REPO)
        with open(DB_PATH, 'rb') as f:
            repo.update_file(
                DB_PATH,
                "Mise à jour automatique",
                f.read(),
                repo.get_contents(DB_PATH).sha
            )
        st.success("Base mise à jour sur GitHub")
    except Exception as e:
        st.error(f"Erreur GitHub : {e}")

# --- GESTION DE LA BASE DE DONNÉES ---
def create_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS rasff_data (
            reference TEXT PRIMARY KEY,
            category TEXT,
            type TEXT,
            subject TEXT,
            date TEXT,
            notifying_country TEXT,
            classification TEXT,
            risk_decision TEXT,
            distribution TEXT,
            forAttention TEXT,
            forFollowUp TEXT,
            operator TEXT,
            origin TEXT,
            hazards TEXT,
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
    existing_columns = [col[1] for col in cursor.execute("PRAGMA table_info(rasff_data)").fetchall()]
    
    # Ajout des colonnes manquantes
    new_columns = [
        ("category", "TEXT"),
        ("type", "TEXT"),
        ("subject", "TEXT"),
        ("classification", "TEXT"),
        ("risk_decision", "TEXT"),
        ("distribution", "TEXT"),
        ("forAttention", "TEXT"),
        ("forFollowUp", "TEXT"),
        ("operator", "TEXT"),
        ("origin", "TEXT"),
        ("hazards", "TEXT"),
        ("year", "INTEGER"),
        ("month", "INTEGER"),
        ("week", "INTEGER")
    ]
    
    for col_name, col_type in new_columns:
        if col_name not in existing_columns:
            cursor.execute(f"ALTER TABLE rasff_data ADD COLUMN {col_name} {col_type}")
    
    conn.commit()
    conn.close()

# --- GESTION DES FICHIERS EXCEL ---
def upload_excel_file():
    uploaded_file = st.file_uploader("Uploader Excel/XLSX", type=["xlsx", "xls"])
    if uploaded_file:
        try:
            if uploaded_file.name.endswith('.xls'):
                xls = pd.ExcelFile(uploaded_file, engine='xlrd')
            else:
                xls = pd.ExcelFile(uploaded_file)
            dfs = []
            for sheet in xls.sheet_names:
                dfs.append(pd.read_excel(xls, sheet_name=sheet))
            return pd.concat(dfs, ignore_index=True)
        except Exception as e:
            st.error(f"Erreur lecture fichier : {e}")
    return None

# --- LOGIQUE DE MISE À JOUR ---
def get_missing_weeks():
    conn = sqlite3.connect(DB_PATH)
    last_date = pd.read_sql("SELECT MAX(date) AS last_date FROM rasff_data", conn)["last_date"].values[0]
    current_date = datetime.datetime.now()
    current_year = current_date.year
    current_week = current_date.isocalendar().week
    
    missing_weeks = []
    if last_date:
        last_year = datetime.datetime.strptime(last_date, "%Y-%m-%d").year
        last_week = datetime.datetime.strptime(last_date, "%Y-%m-%d").isocalendar().week
    else:
        last_year = current_year - 1
        last_week = 52
    
    for week in range(last_week + 1, 53):
        missing_weeks.append((last_year, week))
    for week in range(1, current_week + 1):
        missing_weeks.append((current_year, week))
    return missing_weeks

def update_database():
    missing_weeks = get_missing_weeks()
    total_added = 0
    
    for year, week in missing_weeks:
        url = f"https://www.sirene-diffusion.fr/regia/000-rasff/{str(year)[2:]}/rasff-{year}-{str(week).zfill(2)}.xls"
        try:
            response = requests.get(url, timeout=15)
            if response.status_code != 200:
                st.write(f"Fichier {year}-W{week} non trouvé")
                continue
            
            xls = pd.ExcelFile(BytesIO(response.content))
            df = pd.concat([pd.read_excel(xls, sheet_name=s) for s in xls.sheet_names], ignore_index=True)
            
            # Nettoyage et conversion
            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            df["date_of_case"] = pd.to_datetime(df["date"], errors="coerce")
            df["year"] = df["date_of_case"].dt.year
            df["month"] = df["date_of_case"].dt.month
            df["week"] = df["date_of_case"].dt.isocalendar().week
            
            # Suppression des doublons
            conn = sqlite3.connect(DB_PATH)
            existing_refs = pd.read_sql("SELECT reference FROM rasff_data", conn)["reference"].tolist()
            new_data = df[~df["reference"].isin(existing_refs)].dropna(subset=["reference"])
            
            # Insertion
            new_data.to_sql("rasff_data", conn, if_exists="append", index=False)
            total_added += len(new_data)
            st.write(f"Semaine {year}-W{week}: {len(new_data)} nouvelles alertes ajoutées")
            
        except Exception as e:
            st.error(f"Erreur pour {year}-W{week}: {str(e)[:50]}")
    
    return total_added

def process_uploaded_file(df):
    df["date_of_case"] = pd.to_datetime(df["date"], errors="coerce")
    df["year"] = df["date_of_case"].dt.year
    df["month"] = df["date_of_case"].dt.month
    df["week"] = df["date_of_case"].dt.isocalendar().week
    
    conn = sqlite3.connect(DB_PATH)
    existing_refs = pd.read_sql("SELECT reference FROM rasff_data", conn)["reference"].tolist()
    new_data = df[~df["reference"].isin(existing_refs)].dropna(subset=["reference"])
    
    if not new_data.empty:
        new_data.to_sql("rasff_data", conn, if_exists="append", index=False)
        st.success(f"{len(new_data)} alertes ajoutées depuis le fichier")
    else:
        st.info("Aucune donnée nouvelle trouvée")
    conn.close()

# --- INTERFACE STREAMLIT ---
def main():
    st.title("🚨 RASFF Alerts Dashboard")
    
    # Initialisation de la base
    if not os.path.exists(DB_PATH):
        download_github_db()
    create_database()
    update_database_structure()
    
    # Menu latéral
    st.sidebar.title("🔧 Paramètres")
    if st.sidebar.button("🔄 Récupérer GitHub"):
        download_github_db()
    
    if st.sidebar.button("🔄 Push vers GitHub"):
        if st.sidebar.checkbox("Confirmer"):
            push_to_github()
    
    # Mise à jour manuelle
    uploaded_df = upload_excel_file()
    if uploaded_df is not None:
        if st.sidebar.button("Traiter le fichier uploadé"):
            try:
                process_uploaded_file(uploaded_df)
                push_to_github()
            except Exception as e:
                st.error(f"Erreur traitement fichier : {e}")
    
    # Mise à jour automatique
    if st.button("🔄 Mettre à jour automatique"):
        with st.spinner("Mise à jour en cours..."):
            new_alerts = update_database()
            if new_alerts > 0:
                push_to_github()
    
    # Affichage des données
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM rasff_data", conn)
    st.write("## 📊 Statistiques")
    st.dataframe(df)

if __name__ == "__main__":
    main()
