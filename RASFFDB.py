import streamlit as st
import pandas as pd
import sqlite3
import os
import requests
import base64
from io import BytesIO

st.set_page_config(layout="wide")

# Charger le token GitHub depuis les secrets Streamlit Cloud
GITHUB_TOKEN = st.secrets["GITHUB_TOKEN"]
REPO_OWNER = "M00N69"
REPO_NAME = "RASFFDB"
FILE_PATH = "rasff_data.db"
GITHUB_API_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{FILE_PATH}"

DB_PATH = "rasff_data.db"

# Fonction pour créer les colonnes manquantes
def add_missing_columns():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("ALTER TABLE rasff_notifications ADD COLUMN year INTEGER")
        cursor.execute("ALTER TABLE rasff_notifications ADD COLUMN week INTEGER")
        cursor.execute("UPDATE rasff_notifications SET year = strftime('%Y', date)")
        cursor.execute("UPDATE rasff_notifications SET week = strftime('%W', date)")
        conn.commit()

# Vérifier et ajouter les colonnes 'year' et 'week' si nécessaire
try:
    add_missing_columns()
except sqlite3.OperationalError:
    print("✅ Les colonnes 'year' et 'week' existent déjà.")

# Fonction pour télécharger le fichier depuis GitHub
def download_from_github():
    url = f"https://raw.githubusercontent.com/{REPO_OWNER}/{REPO_NAME}/main/{FILE_PATH}"
    response = requests.get(url)
    with open(DB_PATH, "wb") as file:
        file.write(response.content)

# Fonction pour afficher les dernières entrées dans la base de données
def show_last_entries():
    st.write("📊 Dernières entrées dans la base de données :")
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql("SELECT * FROM rasff_notifications ORDER BY date DESC LIMIT 5", conn)
    st.dataframe(df)

# Fonction pour mettre à jour le fichier sur GitHub
def update_github():
    try:
        with open(DB_PATH, "rb") as file:
            content = file.read()
        encoded_content = base64.b64encode(content).decode()

        response = requests.get(GITHUB_API_URL, headers={
            "Authorization": f"Bearer {GITHUB_TOKEN}"
        })
        if response.status_code != 200:
            st.error("❌ Impossible de récupérer les informations du fichier sur GitHub.")
            return

        response_data = response.json()
        sha = response_data.get("sha", None)

        if sha is None:
            st.error("❌ SHA non trouvé pour le fichier sur GitHub.")
            return

        data = {
            "message": "Mise à jour automatique de la base RASFF",
            "content": encoded_content,
            "sha": sha,
            "branch": "main"
        }

        response = requests.put(GITHUB_API_URL, json=data, headers={
            "Authorization": f"Bearer {GITHUB_TOKEN}"
        })

        if response.status_code in [200, 201]:
            st.success("✅ Mise à jour réussie sur GitHub !")
        else:
            st.error(f"❌ Échec de la mise à jour sur GitHub : {response.json()}")
    except Exception as e:
        st.error(f"❌ Erreur lors de la mise à jour sur GitHub : {e}")

# Fonction pour vérifier la dernière semaine et année dans la base
def get_last_update_info():
    with sqlite3.connect(DB_PATH) as conn:
        query = "SELECT MAX(year), MAX(week) FROM rasff_notifications"
        result = conn.execute(query).fetchone()
    return result

# Fonction pour télécharger et ajouter les semaines manquantes
def update_database():
    last_year, last_week = get_last_update_info()
    current_year = pd.Timestamp.now().year
    current_week = pd.Timestamp.now().week

    # Vérifier les semaines manquantes dans la base de données
    with sqlite3.connect(DB_PATH) as conn:
        query = """
        SELECT year, week FROM rasff_notifications
        GROUP BY year, week
        ORDER BY year, week
        """
        existing_weeks = pd.read_sql(query, conn)

    missing_weeks = []
    for year in range(last_year, current_year + 1):
        for week in range(1, 53):
            if year == current_year and week > current_week:
                break
            if not ((existing_weeks['year'] == year) & (existing_weeks['week'] == week)).any():
                missing_weeks.append((year, week))

    if missing_weeks:
        st.write(f"🔄 {len(missing_weeks)} semaines manquantes détectées.")
    else:
        st.write("✅ Aucune semaine manquante détectée.")

    # Télécharger et insérer les semaines manquantes
    for year, week in missing_weeks:
        week_str = str(week).zfill(2)
        url = f"https://www.sirene-diffusion.fr/regia/000-rasff/{str(year)[-2:]}/rasff-{year}-{week_str}.xls"
        response = requests.get(url)

        if response.status_code == 200:
            st.write(f"📥 Téléchargement réussi pour {url}")
            try:
                df = pd.read_excel(BytesIO(response.content))
                if 'date' not in df.columns:
                    st.error(f"❌ Colonne 'date' manquante dans le fichier {year} - semaine {week_str}")
                    continue

                df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y %H:%M:%S', errors='coerce')
                df['year'] = df['date'].dt.year
                df['week'] = df['date'].dt.isocalendar().week

                with sqlite3.connect(DB_PATH) as conn:
                    df.to_sql("rasff_notifications", conn, if_exists="append", index=False)
                st.write(f"✅ Données insérées pour {year} - semaine {week_str}")
            except Exception as e:
                st.error(f"❌ Erreur lors de l'insertion du fichier Excel : {e}")
        else:
            st.write(f"❌ Fichier non trouvé pour {year} - semaine {week_str}")
            continue

    if not missing_weeks:
        st.write("✅ Toutes les semaines sont déjà à jour.")
    else:
        st.write("✅ Mise à jour des semaines manquantes terminée.")

# Initialisation
if not os.path.exists(DB_PATH):
    download_from_github()

# Interface Streamlit
def main():
    st.title("🚨 RASFF Alerts Dashboard")

    # Bouton pour mettre à jour la base
    if st.button("🔄 Mettre à jour la base RASFF"):
        st.write("📥 Téléchargement des nouvelles données et des semaines manquantes...")
        update_database()
        show_last_entries()
        st.write("📤 Synchronisation avec GitHub...")
        update_github()

    # Récupération des données
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql("SELECT * FROM rasff_notifications", conn)

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

    st.write(f"## 📊 {len(filtered_df)} alertes ({selected_year})")
    st.dataframe(filtered_df, height=600)

    st.write("## 🌟 Répartition par pays")
    st.bar_chart(filtered_df["notifying_country"].value_counts().head(10))

if __name__ == "__main__":
    main()
