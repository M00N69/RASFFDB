import streamlit as st
import pandas as pd
import sqlite3
import requests
import datetime
from github import Github
from io import BytesIO
import os

# === CONFIGURATION STREAMLIT ===
st.set_page_config(
    page_title="RASFF Alerts Dashboard",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="expanded"
)

# === CONFIGURATION BASE DE DONNÉES ===
DB_PATH = "rasff_data.db"
GITHUB_REPO = "M00N69/RASFFDB"
DB_GITHUB_URL = "https://raw.githubusercontent.com/M00N69/RASFFDB/main/rasff_data.db"

# === FONCTIONS D'INTÉGRATION GITHUB ===
def download_github_db():
    """Télécharge la base de données depuis GitHub"""
    response = requests.get(DB_GITHUB_URL)
    if response.status_code == 200:
        with open(DB_PATH, 'wb') as f:
            f.write(response.content)
        st.success("Base GitHub téléchargée avec succès")
    else:
        st.error("Échec du téléchargement de la base GitHub")

def push_to_github():
    """Pousse les modifications vers GitHub"""
    try:
        g = Github(os.getenv("GITHUB_TOKEN"))
        repo = g.get_repo(GITHUB_REPO)
        with open(DB_PATH, 'rb') as f:
            repo.update_file(
                path=DB_PATH,
                message="Mise à jour manuelle via Streamlit",
                content=f.read(),
                sha=repo.get_contents(DB_PATH).sha
            )
        st.success("Base GitHub mise à jour avec succès !")
    except Exception as e:
        st.error(f"Erreur GitHub : {e}")

# === FONCTIONS BASE DE DONNÉES ===
def create_database():
    """Crée la structure de la base de données"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS rasff_data (
            date_of_case TEXT,
            reference TEXT PRIMARY KEY,
            notification_from TEXT,
            country_origin TEXT,
            product_category TEXT,
            product TEXT,
            hazard_substance TEXT,
            hazard_category TEXT,
            year INTEGER,
            month INTEGER,
            week INTEGER
        )
    ''')
    conn.commit()
    conn.close()

def update_database_structure():
    """Ajoute les colonnes manquantes"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    columns = [col[1] for col in cursor.execute("PRAGMA table_info(rasff_data)").fetchall()]
    
    if "year" not in columns:
        cursor.execute("ALTER TABLE rasff_data ADD COLUMN year INTEGER")
    if "month" not in columns:
        cursor.execute("ALTER TABLE rasff_data ADD COLUMN month INTEGER")
    if "week" not in columns:
        cursor.execute("ALTER TABLE rasff_data ADD COLUMN week INTEGER")
    
    conn.commit()
    conn.close()

# === LOGIQUE DE MISE À JOUR ===
def get_missing_weeks():
    """Calcule les semaines manquantes"""
    conn = sqlite3.connect(DB_PATH)
    last_date = pd.read_sql("SELECT MAX(date_of_case) AS last_date FROM rasff_data", conn)["last_date"][0]
    current_date = datetime.datetime.now()
    current_year = current_date.year
    current_week = current_date.isocalendar()[1]
    
    missing_weeks = []
    if last_date:
        last_year = datetime.datetime.strptime(last_date, "%Y-%m-%d").year
        last_week = datetime.datetime.strptime(last_date, "%Y-%m-%d").isocalendar()[1]
    else:
        last_year = current_year - 1
        last_week = 52
    
    # Ajoute les semaines manquantes de l'année précédente
    for week in range(last_week + 1, 53):
        missing_weeks.append((last_year, week))
    
    # Ajoute les semaines de l'année courante
    for week in range(1, current_week + 1):
        missing_weeks.append((current_year, week))
    
    return missing_weeks

def update_database():
    """Met à jour la base avec les semaines manquantes"""
    missing_weeks = get_missing_weeks()
    total_new = 0
    
    for year, week in missing_weeks:
        url = f"https://www.sirene-diffusion.fr/regia/000-rasff/{str(year)[2:]}/rasff-{year}-{str(week).zfill(2)}.xls"
        try:
            response = requests.get(url, timeout=15)
            xls_data = BytesIO(response.content)
            df = pd.read_excel(xls_data)
            
            # Nettoyage et conversion des données
            df["date_of_case"] = pd.to_datetime(df["date"], errors="coerce")
            df["year"] = df["date_of_case"].dt.year
            df["month"] = df["date_of_case"].dt.month
            df["week"] = df["date_of_case"].dt.isocalendar().week
            
            # Insertion sans doublons
            conn = sqlite3.connect(DB_PATH)
            existing_refs = pd.read_sql("SELECT reference FROM rasff_data", conn)["reference"].tolist()
            new_data = df[~df["reference"].isin(existing_refs)]
            new_data.to_sql("rasff_data", conn, if_exists="append", index=False)
            conn.close()
            
            total_new += len(new_data)
            st.write(f"Semaine {year}-S{week} : {len(new_data)} nouvelles alertes ajoutées")
        
        except Exception as e:
            st.write(f"Erreur pour {year}-S{week} : {str(e)[:50]}...")
    
    return total_new

# === INTERFACE STREAMLIT ===
def main():
    st.title("🚨 RASFF Alerts Dashboard")
    
    # Initialisation de la base
    if not os.path.exists(DB_PATH):
        download_github_db()
    create_database()
    update_database_structure()
    
    # Menu latéral
    st.sidebar.title("⚙️ Paramètres")
    if st.sidebar.button("🔄 Récupérer la base GitHub"):
        download_github_db()
    
    if st.sidebar.button("🔄 Mettre à jour manuellement GitHub"):
        if st.sidebar.checkbox("Confirmer la mise à jour GitHub"):
            push_to_github()
    
    # Mise à jour automatique
    if st.button("🔄 Mettre à jour la base"):
        with st.spinner("Mise à jour en cours..."):
            new_alerts = update_database()
            if new_alerts > 0:
                st.success(f"{new_alerts} nouvelles alertes ajoutées !")
                push_to_github()
            else:
                st.info("Aucune mise à jour nécessaire")
    
    # Affichage des données
    df = pd.read_sql("SELECT * FROM rasff_data", sqlite3.connect(DB_PATH))
    st.write("## 📊 Statistiques")
    st.dataframe(df)
    
if __name__ == "__main__":
    main()
