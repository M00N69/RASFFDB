import streamlit as st
import pandas as pd
import sqlite3
import os
import requests
import base64
from io import BytesIO
from dotenv import load_dotenv

# Charger le token GitHub depuis les secrets Streamlit Cloud
GITHUB_TOKEN = st.secrets["GITHUB_TOKEN"]
REPO_OWNER = "M00N69"
REPO_NAME = "RASFFDB"
FILE_PATH = "rasff_data.db"
GITHUB_API_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{FILE_PATH}"

DB_PATH = "rasff_data.db"

# Fonction pour télécharger le fichier depuis GitHub
def download_from_github():
    url = f"https://raw.githubusercontent.com/{REPO_OWNER}/{REPO_NAME}/main/{FILE_PATH}"
    response = requests.get(url)
    with open(DB_PATH, "wb") as file:
        file.write(response.content)

# Fonction pour mettre à jour le fichier sur GitHub
def update_github():
    with open(DB_PATH, "rb") as file:
        content = file.read()
    encoded_content = base64.b64encode(content).decode()

    response = requests.get(GITHUB_API_URL, headers={
        "Authorization": f"Bearer {GITHUB_TOKEN}"
    })
    response_data = response.json()
    sha = response_data.get("sha", None)

    data = {
        "message": "Mise à jour automatique de la base RASFF",
        "content": encoded_content,
        "sha": sha
    }

    response = requests.put(GITHUB_API_URL, json=data, headers={
        "Authorization": f"Bearer {GITHUB_TOKEN}"
    })

    if response.status_code in [200, 201]:
        print("✅ Mise à jour réussie sur GitHub !")
    else:
        print("❌ Échec de la mise à jour :", response.json())

# Fonction pour vérifier la dernière semaine et année dans la base
def get_last_update_info():
    with sqlite3.connect(DB_PATH) as conn:
        query = "SELECT MAX(year), MAX(week) FROM rasff"
        result = conn.execute(query).fetchone()
    return result

# Fonction pour télécharger et ajouter les nouvelles données
def update_database():
    last_year, last_week = get_last_update_info()
    current_year = pd.Timestamp.now().year
    current_week = pd.Timestamp.now().week

    for year in range(last_year, current_year + 1):
        start_week = last_week + 1 if year == last_year else 1
        end_week = current_week if year == current_year else 52

        for week in range(start_week, end_week + 1):
            week_str = str(week).zfill(2)
            url = f"https://www.sirene-diffusion.fr/regia/000-rasff/{str(year)[-2:]}/rasff-{year}-{week_str}.xls"
            response = requests.get(url)

            if response.status_code == 200:
                df = pd.read_excel(BytesIO(response.content))
                with sqlite3.connect(DB_PATH) as conn:
                    df.to_sql("rasff", conn, if_exists="append", index=False)
                print(f"✅ Données ajoutées pour l'année {year}, semaine {week_str}")
            else:
                print(f"❌ Fichier non trouvé pour l'année {year}, semaine {week_str}")
                break  # Arrêter si un fichier manque pour éviter les boucles infinies

# Initialisation
if not os.path.exists(DB_PATH):
    download_from_github()

# Mise à jour automatique
update_database()

# Synchronisation avec GitHub
update_github()

# Interface Streamlit
def main():
    st.title("🚨 RASFF Alerts Dashboard")

    # Récupération des données
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql("SELECT * FROM rasff", conn)

    # Filtrage
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

if __name__ == "__main__":
    main()
