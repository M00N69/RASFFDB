import streamlit as st
import pandas as pd
import sqlite3
import requests
import datetime
from io import BytesIO

# Configuration Streamlit
st.set_page_config(
    page_title="🚨 RASFF Alerts",
    page_icon="🚨",
    layout="wide"
)

# Constantes
DB_PATH = "rasff_data.db"
GITHUB_URL = "https://raw.githubusercontent.com/M00N69/RASFFDB/main/rasff_data.db"

# Structure de la base
TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS alerts (
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

# --- FONCTIONS ---
def init_db():
    """Initialise la base de données"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(TABLE_SCHEMA)
    conn.commit()
    conn.close()

def download_from_github():
    """Télécharge la base depuis GitHub"""
    response = requests.get(GITHUB_URL)
    if response.status_code == 200:
        with open(DB_PATH, "wb") as f:
            f.write(response.content)
        st.success("Base GitHub téléchargée")
    else:
        st.error("Échec du téléchargement")

def get_last_date():
    """Récupère la dernière date enregistrée"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT MAX(date) AS last_date FROM alerts", conn)
    conn.close()
    return df.iloc[0]["last_date"]

def get_missing_weeks():
    """Calcule les semaines manquantes"""
    last_date = get_last_date()
    if not last_date:
        start_year = 2020
        start_week = 1
    else:
        last_dt = datetime.datetime.strptime(last_date, "%d-%m-%Y %H:%M:%S")
        start_year = last_dt.year
        start_week = last_dt.isocalendar().week + 1
    
    current_dt = datetime.datetime.now()
    current_year = current_dt.year
    current_week = current_dt.isocalendar().week
    
    missing = []
    for year in range(start_year, current_year + 1):
        week_start = start_week if year == start_year else 1
        week_end = current_week if year == current_year else 52
        for week in range(week_start, week_end + 1):
            missing.append((year, week))
    return missing

def update_database():
    """Mets à jour la base avec les semaines manquantes"""
    missing_weeks = get_missing_weeks()
    if not missing_weeks:
        st.info("Aucune semaine manquante trouvée")
        return
    
    conn = sqlite3.connect(DB_PATH)
    existing_refs = pd.read_sql("SELECT reference FROM alerts", conn)["reference"].tolist()
    
    for year, week in missing_weeks:
        url = f"https://www.sirene-diffusion.fr/regia/000-rasff/{str(year)[2:]}/rasff-{year}-{str(week).zfill(2)}.xls"
        try:
            response = requests.get(url, timeout=15)
            xls = pd.ExcelFile(BytesIO(response.content))
            df = pd.concat([pd.read_excel(xls, sheet_name=s) for s in xls.sheet_names], ignore_index=True)
            
            # Nettoyage des données
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df["year"] = df["date"].dt.year
            df["month"] = df["date"].dt.month
            df["week"] = df["date"].dt.isocalendar().week
            
            # Suppression des doublons
            new_data = df[~df["reference"].isin(existing_refs)].dropna(subset=["reference"])
            if not new_data.empty:
                new_data.to_sql("alerts", conn, if_exists="append", index=False)
                st.write(f"Semaine {year}-W{week}: {len(new_data)} alertes ajoutées")
        
        except Exception as e:
            st.error(f"Erreur pour {year}-W{week}: {str(e)[:50]}")
    
    conn.close()

# --- INTERFACE ---
def main():
    # Initialisation
    if not os.path.exists(DB_PATH):
        download_from_github()
    init_db()
    
    # Mise à jour automatique au démarrage
    st.spinner("Mise à jour automatique en cours...")
    update_database()
    
    # Récupération des données
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM alerts", conn)
    
    # Filtrage
    st.title("🚨 RASFF Alerts")
    selected_year = st.sidebar.selectbox("Année", sorted(df['year'].unique(), reverse=True))
    selected_country = st.sidebar.selectbox("Pays", ["Tous"] + sorted(df['notifying_country'].unique()))
    
    # Application des filtres
    filtered_df = df.copy()
    if selected_year != "Tous":
        filtered_df = filtered_df[filtered_df['year'] == selected_year]
    if selected_country != "Tous":
        filtered_df = filtered_df[filtered_df['notifying_country'] == selected_country]
    
    # Affichage
    st.write(f"## 📊 {len(filtered_df)} alertes trouvées")
    st.dataframe(filtered_df, height=600)
    
    # Graphiques
    st.write("## 🌟 Répartition par pays")
    st.bar_chart(filtered_df['notifying_country'].value_counts().head(10))

if __name__ == "__main__":
    main()
    
