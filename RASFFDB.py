import streamlit as st
import pandas as pd
import sqlite3
import requests
from datetime import datetime

# Configuration
DB_FILE = "rasff_data.db"  # Base de données SQLite persistante
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
    "cephalopods and products thereof": ["Cephalopods", "Seafood"],
    "cereals and bakery products": ["Cereals and Bakery Products", "Grains and Bakery"],
    "cocoa and cocoa preparations, coffee and tea": ["Cocoa, Coffee, and Tea", "Beverages"],
    "compound feeds": ["Compound Feeds", "Animal Feed"],
    "confectionery": ["Confectionery", "Grains and Bakery"],
    "crustaceans and products thereof": ["Crustaceans", "Seafood"],
    "dietetic foods, food supplements and fortified foods": ["Dietetic Foods and Supplements", "Specialty Foods"],
    "eggs and egg products": ["Eggs and Egg Products", "Animal Products"],
    "fats and oils": ["Fats and Oils", "Fats and Oils"],
    "feed additives": ["Feed Additives", "Animal Feed"],
    "feed materials": ["Feed Materials", "Animal Feed"],
    "feed premixtures": ["Feed Premixtures", "Animal Feed"],
    "fish and fish products": ["Fish and Fish Products", "Seafood"],
    "food additives and flavourings": ["Food Additives and Flavourings", "Additives"],
    "food contact materials": ["Food Contact Materials", "Packaging"],
    "fruits and vegetables": ["Fruits and Vegetables", "Fruits and Vegetables"],
    "gastropods": ["Gastropods", "Seafood"],
    "herbs and spices": ["Herbs and Spices", "Spices"],
    "honey and royal jelly": ["Honey and Royal Jelly", "Specialty Foods"],
    "ices and desserts": ["Ices and Desserts", "Grains and Bakery"],
    "live animals": ["Live Animals", "Animal Products"],
    "meat and meat products (other than poultry)": ["Meat (Non-Poultry)", "Meat Products"],
    "milk and milk products": ["Milk and Milk Products", "Dairy"],
    "natural mineral waters": ["Natural Mineral Waters", "Beverages"],
    "non-alcoholic beverages": ["Non-Alcoholic Beverages", "Beverages"],
    "nuts, nut products and seeds": ["Nuts and Seeds", "Seeds and Nuts"],
    "other food product / mixed": ["Mixed Food Products", "Other"],
    "pet food": ["Pet Food", "Animal Feed"],
    "plant protection products": ["Plant Protection Products", "Additives"],
    "poultry meat and poultry meat products": ["Poultry Meat", "Meat Products"],
    "prepared dishes and snacks": ["Prepared Dishes and Snacks", "Prepared Foods"],
    "soups, broths, sauces and condiments": ["Soups, Broths, Sauces", "Prepared Foods"],
    "water for human consumption (other)": ["Water (Human Consumption)", "Beverages"],
    "wine": ["Wine", "Beverages"]
}

# Mappings des catégories de dangers
HAZARD_CATEGORY_MAPPING = {
    "adulteration / fraud": ["Adulteration / Fraud", "Food Fraud"],
    "allergens": ["Allergens", "Biological Hazard"],
    "biological contaminants": ["Biological Contaminants", "Biological Hazard"],
    "biotoxins (other)": ["Biotoxins", "Biological Hazard"],
    "chemical contamination (other)": ["Chemical Contamination", "Chemical Hazard"],
    "environmental pollutants": ["Environmental Pollutants", "Chemical Hazard"],
    "feed additives": ["Feed Additives", "Chemical Hazard"],
    "food additives and flavourings": ["Food Additives and Flavourings", "Additives"],
    "foreign bodies": ["Foreign Bodies", "Physical Hazard"],
    "heavy metals": ["Heavy Metals", "Chemical Hazard"],
    "industrial contaminants": ["Industrial Contaminants", "Chemical Hazard"],
    "mycotoxins": ["Mycotoxins", "Biological Hazard"],
    "natural toxins (other)": ["Natural Toxins", "Biological Hazard"],
    "pathogenic micro-organisms": ["Pathogenic Micro-organisms", "Biological Hazard"],
    "pesticide residues": ["Pesticide Residues", "Pesticide Hazard"],
    "residues of veterinary medicinal": ["Veterinary Medicinal Residues", "Chemical Hazard"]
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

# Nettoyer et mapper les données
def clean_and_map_data(df):
    df.rename(columns=WEEKLY_COLUMN_MAPPING, inplace=True)
    for col in HEADERS:
        if col not in df.columns:
            df[col] = None
    df["prodcat"] = df["product_category"].apply(lambda x: PRODUCT_CATEGORY_MAPPING.get(x.lower(), ["Unknown", "Unknown"])[0] if pd.notnull(x) else "Unknown")
    df["groupprod"] = df["product_category"].apply(lambda x: PRODUCT_CATEGORY_MAPPING.get(x.lower(), ["Unknown", "Unknown"])[1] if pd.notnull(x) else "Unknown")
    df["hazcat"] = df["hazard_substance"].apply(lambda x: HAZARD_CATEGORY_MAPPING.get(x.lower(), ["Unknown", "Unknown"])[0] if pd.notnull(x) else "Unknown")
    df["grouphaz"] = df["hazard_substance"].apply(lambda x: HAZARD_CATEGORY_MAPPING.get(x.lower(), ["Unknown", "Unknown"])[1] if pd.notnull(x) else "Unknown")
    return df[HEADERS]

# Télécharger et traiter les données
def download_and_process_data(year, week):
    url_template = "https://www.sirene-diffusion.fr/regia/000-rasff/{}/rasff-{}-{}.xls"
    url = url_template.format(str(year)[-2:], year, f"{week:02d}")
    try:
        response = requests.get(url)
        if response.status_code == 200:
            df = pd.read_excel(response.content)
            return clean_and_map_data(df)
        else:
            st.warning(f"Échec du téléchargement (code {response.status_code}).")
    except Exception as e:
        st.error(f"Erreur lors du téléchargement : {e}")
    return None

# Sauvegarder dans la base de données
def save_to_database(data):
    if data is not None:
        connection = sqlite3.connect(DB_FILE)
        data.to_sql("rasff_data", connection, if_exists="append", index=False)
        connection.close()
        st.success(f"{len(data)} lignes ajoutées à la base de données.")

# Interface Streamlit
st.title("RASFF Data Manager")

menu = st.sidebar.selectbox("Menu", ["Afficher les données", "Télécharger et ajouter des données"])

initialize_database()

if menu == "Afficher les données":
    connection = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT * FROM rasff_data", connection)
    connection.close()
    st.dataframe(df)

elif menu == "Télécharger et ajouter des données":
    year = st.number_input("Année", min_value=2020, max_value=2050, value=datetime.now().year)
    week = st.number_input("Semaine", min_value=1, max_value=53, value=datetime.now().isocalendar()[1])
    if st.button("Télécharger et ajouter"):
        data = download_and_process_data(year, week)
        if data is not None:
            save_to_database(data)
