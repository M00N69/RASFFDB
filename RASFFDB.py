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
    page_title="🚨 RASFF Alerts",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Constantes
DB_PATH = "rasff_data.db"
GITHUB_REPO = "M00N69/RASFFDB"
DB_GITHUB_URL = "https://raw.githubusercontent.com/M00N69/RASFFDB/main/rasff_data.db"

# Structure de la base de données
TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS rasff (
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
);
"""

# Initialisation de la base
def init_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(TABLE_SCHEMA)
    conn.commit()
    conn.close()

# Téléchargement depuis GitHub
def download_from_github():
    response = requests.get(DB_GITHUB_URL)
    if response.status_code == 200:
        with open(DB_PATH, "wb") as f:
            f.write(response.content)
        st.success("Base GitHub téléchargée")
    else:
        st.error("Échec du téléchargement")

# Récupération des semaines manquantes
def get_missing_weeks():
    conn = sqlite3.connect(DB_PATH)
    last_date = pd.read_sql("SELECT MAX(date) AS last_date FROM rasff", conn).iloc[0][0]
    conn.close()
    
    if last_date:
        last_dt = datetime.datetime.strptime(last_date, "%d-%m-%Y %H:%M:%S")
    else:
        last_dt = datetime.datetime(2020, 1, 1)
    
    current_dt = datetime.datetime.now()
    current_year = current_dt.year
    current_week = current_dt.isocalendar().week
    
    missing_weeks = []
    for year in range(last_dt.year, current_year + 1):
        start_week = 1 if year != last_dt.year else last_dt.isocalendar().week + 1
        end_week = 52 if year != current_year else current_week
        for week in range(start_week, end_week + 1):
            missing_weeks.append((year, week))
    return missing_weeks

# Mise à jour de la base
def update_database():
    conn = sqlite3.connect(DB_PATH)
    existing_refs = pd.read_sql("SELECT reference FROM rasff", conn)["reference"].tolist()
    missing_weeks = get_missing_weeks()
    
    for year, week in missing_weeks:
        url = f"https://www.sirene-diffusion.fr/regia/000-rasff/{str(year)[2:]}/rasff-{year}-{str(week).zfill(2)}.xls"
        try:
            response = requests.get(url, timeout=15)
            xls = pd.ExcelFile(BytesIO(response.content))
            df = pd.concat([pd.read_excel(xls, sheet_name=s) for s in xls.sheet_names], ignore_index=True)
            
            # Nettoyage des données
            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df["year"] = df["date"].dt.year
            df["month"] = df["date"].dt.month
            df["week"] = df["date"].dt.isocalendar().week
            
            # Suppression des doublons
            new_data = df[~df["reference"].isin(existing_refs)].dropna(subset=["reference"])
            if not new_data.empty:
                new_data.to_sql("rasff", conn, if_exists="append", index=False)
                st.write(f"Semaine {year}-W{week}: {len(new_data)} alertes ajoutées")
        
        except Exception as e:
            st.error(f"Erreur pour {year}-W{week}: {str(e)[:50]}")
    
    conn.close()

# Synchronisation GitHub
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
        st.error(f"Erreur GitHub: {e}")

# Interface Streamlit
def main():
    # Initialisation
    if not os.path.exists(DB_PATH):
        download_from_github()
    init_database()
    
    # Mise à jour automatique
    st.sidebar.button("🔄 Mettre à jour les données", on_click=update_database)
    
    # Récupération des données
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM rasff", conn)
    
    # Filtrage
    st.title("🚨 RASFF Alerts Dashboard")
    selected_country = st.sidebar.selectbox("Pays", ["Tous"] + sorted(df["notifying_country"].unique()))
    selected_year = st.sidebar.selectbox("Année", ["Tous"] + sorted(df["year"].unique(), reverse=True))
    selected_category = st.sidebar.selectbox("Catégorie", ["Toutes"] + sorted(df["category"].unique()))
    
    # Application des filtres
    filtered_df = df.copy()
    if selected_country != "Tous":
        filtered_df = filtered_df[filtered_df["notifying_country"] == selected_country]
    if selected_year != "Tous":
        filtered_df = filtered_df[filtered_df["year"] == selected_year]
    if selected_category != "Toutes":
        filtered_df = filtered_df[filtered_df["category"] == selected_category]
    
    # Affichage
    st.write(f"## 📊 {len(filtered_df)} alertes ({selected_year})")
    st.dataframe(filtered_df, height=600)
    
    # Graphiques
    st.write("## 🌟 Répartition par pays")
    st.bar_chart(filtered_df["notifying_country"].value_counts().head(10))
    
    # Synchronisation GitHub
    if st.sidebar.button("🔄 Synchroniser GitHub"):
        push_to_github()

if __name__ == "__main__":
    main()
    
