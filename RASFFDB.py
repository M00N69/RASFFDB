import streamlit as st
import pandas as pd
import sqlite3
import requests
from datetime import datetime
import os
from github import Github
import plotly.express as px
from scipy.stats import chi2_contingency
import seaborn as sns
import matplotlib.pyplot as plt

# Configuration
st.set_page_config(page_title="RASFF Data Manager", layout="wide")
DB_FILE = "rasff_data.db"
HEADERS = [
    "date_of_case", "reference", "notification_from", "country_origin",
    "product", "product_category", "hazard_substance", "hazard_category",
    "prodcat", "groupprod", "hazcat", "grouphaz"
]

# Mappings des catégories de produits
PRODUCT_CATEGORY_MAPPING = {
    "alcoholic beverages": ["Alcoholic Beverages", "Beverages"],
    "animal by-products": ["Animal By-products", "Animal Products"],
    "seafood": ["Fish and Seafood", "Seafood"],
    "cereals and bakery products": ["Cereals and Bakery Products", "Grains and Bakery"],
    "milk and milk products": ["Milk and Milk Products", "Dairy"],
    "nuts and seeds": ["Nuts and Seeds", "Seeds and Nuts"],
    "prepared dishes and snacks": ["Prepared Dishes and Snacks", "Prepared Foods"],
    "additives": ["Food Additives and Flavourings", "Additives"],
    "fruits and vegetables": ["Fruits and Vegetables", "Fruits"],
}

# Mappings des catégories de dangers
HAZARD_CATEGORY_MAPPING = {
    "food fraud": ["Adulteration / Fraud", "Food Fraud"],
    "biological hazard": ["Pathogenic Micro-organisms", "Biological Hazard"],
    "chemical hazard": ["Chemical Contamination", "Chemical Hazard"],
    "physical hazard": ["Foreign Bodies", "Physical Hazard"],
}

# Initialiser la base de données
def initialize_database():
    connection = sqlite3.connect(DB_FILE)
    cursor = connection.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS rasff_data (
            {', '.join([f'{col} TEXT' for col in HEADERS])}
        )
    """)
    connection.commit()
    connection.close()

# Charger les données de la base SQLite
@st.cache_data
def load_data_from_db():
    connection = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT * FROM rasff_data", connection)
    connection.close()
    if 'date_of_case' in df.columns:
        df['date_of_case'] = pd.to_datetime(df['date_of_case'], errors='coerce')
    return df

# Obtenir la dernière semaine dans la base de données
def get_last_week_in_db():
    connection = sqlite3.connect(DB_FILE)
    query = "SELECT MAX(date_of_case) FROM rasff_data"
    result = pd.read_sql_query(query, connection).iloc[0, 0]
    connection.close()

    if result:
        try:
            last_date = pd.to_datetime(result)
            return last_date.isocalendar()[:2]  # Renvoie (année, semaine)
        except ValueError:
            st.warning("Erreur dans le format de la date dans la base.")
            return (2024, 1)  # Valeur par défaut
    else:
        return (2024, 1)

# Télécharger et traiter les données pour une semaine donnée
def download_and_process_data(year, week):
    url_template = "https://www.sirene-diffusion.fr/regia/000-rasff/{}/rasff-{}-{}.xls"
    url = url_template.format(str(year)[-2:], year, f"{week:02d}")
    try:
        response = requests.get(url)
        if response.status_code == 200:
            df = pd.read_excel(response.content)
            return clean_and_map_data(df)
        else:
            st.warning(f"Échec du téléchargement pour {year}, semaine {week}.")
    except Exception as e:
        st.error(f"Erreur lors du téléchargement : {e}")
    return None

# Nettoyer et mapper les données
def clean_and_map_data(df):
    df.rename(columns={
        "Date of Case": "date_of_case",
        "Reference": "reference",
        "Notification From": "notification_from",
        "Country Origin": "country_origin",
        "Product": "product",
        "Product Category": "product_category",
        "Hazard Substance": "hazard_substance",
        "Hazard Category": "hazard_category"
    }, inplace=True)
    for col in HEADERS:
        if col not in df.columns:
            df[col] = None
    df["prodcat"] = df["product_category"].apply(
        lambda x: PRODUCT_CATEGORY_MAPPING.get(str(x).lower(), ["Unknown", "Unknown"])[0]
    )
    df["groupprod"] = df["product_category"].apply(
        lambda x: PRODUCT_CATEGORY_MAPPING.get(str(x).lower(), ["Unknown", "Unknown"])[1]
    )
    df["hazcat"] = df["hazard_substance"].apply(
        lambda x: HAZARD_CATEGORY_MAPPING.get(str(x).lower(), ["Unknown", "Unknown"])[0]
    )
    df["grouphaz"] = df["hazard_substance"].apply(
        lambda x: HAZARD_CATEGORY_MAPPING.get(str(x).lower(), ["Unknown", "Unknown"])[1]
    )
    return df[HEADERS]

# Mettre à jour la base avec les semaines manquantes
def update_database():
    last_year, last_week = get_last_week_in_db()
    current_year, current_week = datetime.now().isocalendar()[:2]

    with st.spinner("Mise à jour des données..."):
        for year in range(last_year, current_year + 1):
            start_week = last_week + 1 if year == last_year else 1
            end_week = current_week if year == current_year else 53
            for week in range(start_week, end_week + 1):
                data = download_and_process_data(year, week)
                if data is not None:
                    save_to_database(data)
        st.success("Base de données mise à jour avec succès !")

# Sauvegarder dans la base SQLite
def save_to_database(data):
    if data is not None:
        connection = sqlite3.connect(DB_FILE)
        data.to_sql("rasff_data", connection, if_exists="append", index=False)
        connection.close()

# Synchroniser avec GitHub
def push_db_to_github():
    token = os.getenv("GITHUB_TOKEN")
    repo_name = "M00N69/RASFFDB"
    file_path = DB_FILE

    try:
        g = Github(token)
        repo = g.get_repo(repo_name)

        with open(file_path, "rb") as f:
            content = f.read()

        try:
            contents = repo.get_contents(file_path)
            repo.update_file(contents.path, "Mise à jour de la base de données", content, contents.sha)
        except:
            repo.create_file(file_path, "Ajout initial de la base de données", content)

        st.success("Le fichier .db a été mis à jour sur GitHub.")
    except Exception as e:
        st.error(f"Erreur lors de la mise à jour sur GitHub : {e}")

# Vue Base de Données avec Filtres
def view_database(df: pd.DataFrame):
    st.header("Base de Données")
    st.sidebar.header("Filtres")

    if 'date_of_case' not in df.columns:
        st.error("Colonne 'date_of_case' introuvable dans les données.")
        return

    df['year'] = df['date_of_case'].dt.year
    df['week'] = df['date_of_case'].dt.isocalendar().week
    min_year, max_year = df['year'].min(), df['year'].max()

    selected_year = st.sidebar.selectbox("Année", list(range(min_year, max_year + 1)))
    selected_weeks = st.sidebar.slider("Semaines", 1, 53, (1, 53))

    filtered_df = df[(df['year'] == selected_year) &
                     (df['week'] >= selected_weeks[0]) &
                     (df['week'] <= selected_weeks[1])]

    categories = st.sidebar.multiselect("Catégories de Produits", sorted(df['prodcat'].dropna().unique()))
    if categories:
        filtered_df = filtered_df[filtered_df['prodcat'].isin(categories)]

    hazards = st.sidebar.multiselect("Catégories de Dangers", sorted(df['hazcat'].dropna().unique()))
    if hazards:
        filtered_df = filtered_df[filtered_df['hazcat'].isin(hazards)]

    st.dataframe(filtered_df)

# Main
def main():
    initialize_database()
    df = load_data_from_db()

    menu = st.sidebar.radio(
        "Navigation",
        ["Tableau de Bord", "Base de Données", "Mise à Jour", "Synchronisation GitHub"]
    )

    if menu == "Tableau de Bord":
        st.title("Tableau de Bord")
        display_dashboard(df)
    elif menu == "Base de Données":
        view_database(df)
    elif menu == "Mise à Jour":
        if st.button("Mettre à jour la base de données"):
            update_database()
    elif menu == "Synchronisation GitHub":
        if st.button("Pousser le fichier .db vers GitHub"):
            push_db_to_github()

if __name__ == "__main__":
    main()
