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
    layout="wide"
)

# Constantes
DB_PATH = "rasff_data.db"
GITHUB_REPO = "M00N69/RASFFDB"
DB_GITHUB_URL = "https://raw.githubusercontent.com/M00N69/RASFFDB/main/rasff_data.db"

# Structure de la base de données
COLUMNS = [
    "reference", "category", "type", "subject", "date",
    "notifying_country", "classification", "risk_decision",
    "distribution", "forAttention", "forFollowUp",
    "operator", "origin", "hazards", "year", "month", "week"
]

# Initialisation de la base
def init_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS alerts ({', '.join([f"{col} TEXT" for col in COLUMNS])})
    """)
    conn.commit()
    conn.close()

# Récupération automatique de la base GitHub
def download_github_db():
    response = requests.get(DB_GITHUB_URL)
    if response.status_code == 200:
        with open(DB_PATH, "wb") as f:
            f.write(response.content)
        st.success("Base GitHub téléchargée")
    else:
        st.error("Échec du téléchargement")

# Mise à jour automatique des semaines
def update_database():
    conn = sqlite3.connect(DB_PATH)
    current_date = datetime.datetime.now()
    
    # Dernière date dans la base
    last_date = pd.read_sql("SELECT MAX(date) AS last_date FROM alerts", conn)["last_date"][0]
    start_year = int(last_date[:4]) if last_date else 2020
    
    # Génération des semaines manquantes
    missing_weeks = []
    for year in range(start_year, current_date.year + 1):
        week_start = 1 if year == current_date.year else 1
        week_end = current_date.isocalender().week if year == current_date.year else 52
        for week in range(week_start, week_end + 1):
            if not pd.read_sql(f"SELECT * FROM alerts WHERE week={week} AND year={year}", conn).empty:
                continue
            missing_weeks.append((year, week))
    
    # Téléchargement des semaines manquantes
    for year, week in missing_weeks:
        url = f"https://www.sirene-diffusion.fr/regia/000-rasff/{str(year)[2:]}/rasff-{year}-{str(week).zfill(2)}.xls"
        try:
            response = requests.get(url, timeout=15)
            xls = pd.ExcelFile(BytesIO(response.content))
            df = pd.concat([pd.read_excel(xls, sheet_name=s) for s in xls.sheet_names], ignore_index=True)
            
            # Nettoyage des données
            df = df[COLUMNS].applymap(lambda x: x.strip() if isinstance(x, str) else x)
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df["year"] = df["date"].dt.year
            df["month"] = df["date"].dt.month
            df["week"] = df["date"].dt.isocalendar().week
            
            # Insertion sans doublons
            existing_refs = pd.read_sql("SELECT reference FROM alerts", conn)["reference"].tolist()
            new_data = df[~df["reference"].isin(existing_refs)].dropna(subset=["reference"])
            new_data.to_sql("alerts", conn, if_exists="append", index=False)
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
        download_github_db()
    init_database()
    
    # Mise à jour automatique au démarrage
    st.sidebar.button("🔄 Mettre à jour la base", on_click=update_database)
    
    # Récupération des données
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM alerts", conn)
    
    # Filtrage interactif
    st.sidebar.title("FilterWhere")
    category = st.sidebar.selectbox("Catégorie", ["Toutes"] + df["category"].unique().tolist())
    country = st.sidebar.selectbox("Pays", ["Tous"] + df["notifying_country"].unique().tolist())
    year = st.sidebar.selectbox("Année", ["Toutes"] + sorted(df["year"].unique().tolist(), reverse=True))
    
    # Application des filtres
    filtered_df = df.copy()
    if category != "Toutes":
        filtered_df = filtered_df[filtered_df["category"] == category]
    if country != "Tous":
        filtered_df = filtered_df[filtered_df["notifying_country"] == country]
    if year != "Toutes":
        filtered_df = filtered_df[filtered_df["year"] == year]
    
    # Affichage
    st.title("🚨 RASFF Alerts")
    st.dataframe(filtered_df[COLUMNS[:-3]], height=600)
    
    # Graphiques
    st.subheader("Répartition par pays")
    st.bar_chart(filtered_df["notifying_country"].value_counts().head(10))
    
    # Synchronisation GitHub
    if st.sidebar.button("🔄 Push vers GitHub"):
        push_to_github()

if __name__ == "__main__":
    main()
