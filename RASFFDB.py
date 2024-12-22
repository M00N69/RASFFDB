import streamlit as st
import pandas as pd
import sqlite3
import requests
from datetime import datetime

DB_FILE = "rasff_data.db"  # Base de données persistante

# Fonction pour initialiser la base de données
def initialize_database():
    connection = sqlite3.connect(DB_FILE)
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rasff_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_of_case TEXT,
            reference TEXT,
            notification_from TEXT,
            country_origin TEXT,
            product TEXT,
            product_category TEXT,
            hazard_substance TEXT,
            hazard_category TEXT,
            prodcat TEXT,
            groupprod TEXT,
            hazcat TEXT,
            grouphaz TEXT
        )
    """)
    connection.commit()
    connection.close()

# Fonction pour récupérer les données depuis la base
def fetch_data():
    connection = sqlite3.connect(DB_FILE)
    query = "SELECT * FROM rasff_data ORDER BY date_of_case DESC"
    data = pd.read_sql_query(query, connection)
    connection.close()
    return data

# Fonction pour télécharger les données hebdomadaires
def download_data(year, week):
    url_template = "https://www.sirene-diffusion.fr/regia/000-rasff/{}/rasff-{}-{}.xls"
    url = url_template.format(str(year)[-2:], year, f"{week:02d}")
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return pd.read_excel(response.content)  # Lire directement depuis la réponse
        else:
            st.warning(f"Échec du téléchargement : {response.status_code}")
            return None
    except Exception as e:
        st.error(f"Erreur lors du téléchargement : {e}")
        return None

# Fonction pour nettoyer et insérer les données
def clean_and_insert(df):
    try:
        # Garder uniquement les colonnes nécessaires
        columns_needed = [
            "date_of_case", "reference", "notification_from", "country_origin",
            "product", "product_category", "hazard_substance", "hazard_category"
        ]
        df = df[columns_needed]
        df.fillna("", inplace=True)  # Remplacer les valeurs nulles
        data = df.values.tolist()

        connection = sqlite3.connect(DB_FILE)
        cursor = connection.cursor()
        cursor.executemany("""
            INSERT INTO rasff_data (
                date_of_case, reference, notification_from, country_origin,
                product, product_category, hazard_substance, hazard_category
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, data)
        connection.commit()
        connection.close()
        st.success(f"{len(data)} lignes insérées.")
    except Exception as e:
        st.error(f"Erreur lors de l'insertion des données : {e}")

# Initialiser la base de données
initialize_database()

# Interface Streamlit
st.title("RASFF Data Manager")

menu = st.sidebar.selectbox("Menu", ["Afficher les données", "Télécharger et ajouter des données", "Exporter les données"])

if menu == "Afficher les données":
    st.header("Données RASFF")
    data = fetch_data()
    st.dataframe(data)

elif menu == "Télécharger et ajouter des données":
    st.header("Télécharger des données hebdomadaires")
    
    year = st.number_input("Année", min_value=2020, max_value=2050, value=datetime.now().year)
    week = st.number_input("Semaine", min_value=1, max_value=53, value=datetime.now().isocalendar()[1])
    
    if st.button("Télécharger et ajouter"):
        df = download_data(year, week)
        if df is not None:
            clean_and_insert(df)

elif menu == "Exporter les données":
    st.header("Exporter les données")
    data = fetch_data()
    csv = data.to_csv(index=False)
    st.download_button("Télécharger en CSV", csv, "rasff_data.csv", "text/csv")
