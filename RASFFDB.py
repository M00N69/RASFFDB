import streamlit as st
import pandas as pd
import sqlite3
import requests
from datetime import datetime
import os
from github import Github

# Configuration
DB_FILE = "rasff_data.db"
HEADERS = [
    "date_of_case", "reference", "notification_from", "country_origin",
    "product", "product_category", "hazard_substance", "hazard_category",
    "prodcat", "groupprod", "hazcat", "grouphaz"
]

# Mappings des colonnes entre les fichiers Excel et le format attendu
WEEKLY_COLUMN_MAPPING = {
    "date": "date_of_case",
    "reference": "reference",
    "notifying_country": "notification_from",
    "origin": "country_origin",
    "subject": "product",
    "category": "product_category",
    "hazards": "hazard_substance"
}

# Mappings des catégories de produits
PRODUCT_CATEGORY_MAPPING = {
    "alcoholic beverages": ["Alcoholic Beverages", "Beverages"],
    "animal by-products": ["Animal By-products", "Animal Products"],
    "bivalve molluscs and products thereof": ["Bivalve Molluscs", "Seafood"],
    "cereals and bakery products": ["Cereals and Bakery Products", "Grains and Bakery"],
    "dietetic foods, food supplements and fortified foods": ["Dietetic Foods and Supplements", "Specialty Foods"],
    "eggs and egg products": ["Eggs and Egg Products", "Animal Products"],
    "fats and oils": ["Fats and Oils", "Fats and Oils"],
    "fruits and vegetables": ["Fruits and Vegetables", "Fruits and Vegetables"],
    "milk and milk products": ["Milk and Milk Products", "Dairy"],
    "nuts, nut products and seeds": ["Nuts and Seeds", "Seeds and Nuts"],
    "poultry meat and poultry meat products": ["Poultry Meat", "Meat Products"],
    "prepared dishes and snacks": ["Prepared Dishes and Snacks", "Prepared Foods"],
    "soups, broths, sauces and condiments": ["Soups, Broths, Sauces", "Prepared Foods"],
    "non-alcoholic beverages": ["Non-Alcoholic Beverages", "Beverages"],
    "wine": ["Wine", "Beverages"],
    # Ajoutez les autres catégories ici
}

# Mappings des catégories de dangers
HAZARD_CATEGORY_MAPPING = {
    "adulteration / fraud": ["Adulteration / Fraud", "Food Fraud"],
    "allergens": ["Allergens", "Biological Hazard"],
    "biological contaminants": ["Biological Contaminants", "Biological Hazard"],
    "chemical contamination (other)": ["Chemical Contamination", "Chemical Hazard"],
    "pathogenic micro-organisms": ["Pathogenic Micro-organisms", "Biological Hazard"],
    "pesticide residues": ["Pesticide Residues", "Pesticide Hazard"],
    "heavy metals": ["Heavy Metals", "Chemical Hazard"],
    "mycotoxins": ["Mycotoxins", "Biological Hazard"],
    # Ajoutez les autres catégories ici
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

# Obtenir la dernière semaine dans la base de données
def get_last_week_in_db():
    connection = sqlite3.connect(DB_FILE)
    query = "SELECT MAX(date_of_case) FROM rasff_data"
    result = pd.read_sql_query(query, connection).iloc[0, 0]
    connection.close()

    if result:
        try:
            last_date = datetime.strptime(result, "%Y-%m-%d")
            return last_date.isocalendar()[:2]  # Renvoie (année, semaine)
        except ValueError:
            st.warning("La colonne 'date_of_case' contient des valeurs mal formatées.")
            return (2024, 1)  # Valeur par défaut si le format est incorrect
    else:
        return (2024, 1)  # Point de départ par défaut si aucune donnée n'est présente

# Nettoyer et mapper les données
def clean_and_map_data(df):
    df.rename(columns=WEEKLY_COLUMN_MAPPING, inplace=True)
    for col in HEADERS:
        if col not in df.columns:
            df[col] = None
    # Convertir et valider 'date_of_case'
    df["date_of_case"] = pd.to_datetime(df["date_of_case"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["prodcat"] = df["product_category"].apply(
        lambda x: PRODUCT_CATEGORY_MAPPING.get(x.lower(), ["Unknown", "Unknown"])[0] if pd.notnull(x) else "Unknown"
    )
    df["groupprod"] = df["product_category"].apply(
        lambda x: PRODUCT_CATEGORY_MAPPING.get(x.lower(), ["Unknown", "Unknown"])[1] if pd.notnull(x) else "Unknown"
    )
    df["hazcat"] = df["hazard_substance"].apply(
        lambda x: HAZARD_CATEGORY_MAPPING.get(x.lower(), ["Unknown", "Unknown"])[0] if pd.notnull(x) else "Unknown"
    )
    df["grouphaz"] = df["hazard_substance"].apply(
        lambda x: HAZARD_CATEGORY_MAPPING.get(x.lower(), ["Unknown", "Unknown"])[1] if pd.notnull(x) else "Unknown"
    )
    return df[HEADERS]

# Télécharger et traiter les données pour une semaine
def download_and_process_data(year, week):
    url_template = "https://www.sirene-diffusion.fr/regia/000-rasff/{}/rasff-{}-{}.xls"
    url = url_template.format(str(year)[-2:], year, f"{week:02d}")
    try:
        response = requests.get(url)
        if response.status_code == 200:
            df = pd.read_excel(response.content)
            return clean_and_map_data(df)
        else:
            st.warning(f"Échec du téléchargement pour {year} Semaine {week} (code {response.status_code}).")
    except Exception as e:
        st.error(f"Erreur lors du téléchargement : {e}")
    return None

# Sauvegarder dans la base de données
def save_to_database(data):
    if data is not None:
        connection = sqlite3.connect(DB_FILE)
        data.to_sql("rasff_data", connection, if_exists="append", index=False)
        connection.close()

# Charger toutes les semaines manquantes jusqu'à la semaine courante
def load_missing_weeks():
    last_year, last_week = get_last_week_in_db()
    current_year, current_week = datetime.now().isocalendar()[:2]

    with st.spinner("Chargement des données manquantes..."):
        for year in range(last_year, current_year + 1):
            start_week = last_week + 1 if year == last_year else 1
            end_week = current_week if year == current_year else 53

            for week in range(start_week, end_week + 1):
                data = download_and_process_data(year, week)
                if data is not None:
                    save_to_database(data)

        st.success("Toutes les semaines manquantes ont été chargées !")

# Pousser la base de données vers GitHub
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

        st.success("La base de données a été mise à jour sur GitHub.")
    except Exception as e:
        st.error(f"Erreur lors de la mise à jour sur GitHub : {e}")

# Interface Streamlit
st.title("RASFF Data Manager")

menu = st.sidebar.selectbox("Menu", ["Afficher les données", "Charger les semaines manquantes", "Pousser le fichier vers GitHub"])

initialize_database()

if menu == "Afficher les données":
    connection = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT * FROM rasff_data", connection)
    connection.close()
    st.dataframe(df)

elif menu == "Charger les semaines manquantes":
    if st.button("Charger les données manquantes"):
        load_missing_weeks()

elif menu == "Pousser le fichier vers GitHub":
    if st.button("Pousser le fichier .db"):
        push_db_to_github()
