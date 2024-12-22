import streamlit as st
import pandas as pd
import sqlite3
import requests
from datetime import datetime
import os
from github import Github
import plotly.express as px
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import chi2_contingency

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
    "cephalopods and products thereof": ["Cephalopods", "Seafood"],
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

# Charger les données de la base SQLite
@st.cache_data
def load_data_from_db():
    connection = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT * FROM rasff_data", connection)
    connection.close()
    df["date_of_case"] = pd.to_datetime(df["date_of_case"], errors="coerce")
    return df

# Pousser le fichier .db sur GitHub
def push_db_to_github():
    token = os.getenv("GITHUB_TOKEN")  # Configurez votre token dans Streamlit Cloud
    repo_name = "M00N69/RASFFDB"  # Remplacez par votre dépôt GitHub
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

# Afficher les statistiques clés
def display_statistics(df: pd.DataFrame):
    st.header("Statistiques Clés")
    col1, col2, col3 = st.columns(3)
    col1.metric("Notifications Totales", len(df))
    col2.metric("Catégories de Produits", df['prodcat'].nunique())
    col3.metric("Catégories de Dangers", df['hazcat'].nunique())

# Visualisations interactives avec Plotly
def display_visualizations(df: pd.DataFrame):
    st.header("Visualisations")
    fig_product = px.bar(
        df['prodcat'].value_counts(),
        x=df['prodcat'].value_counts().index,
        y=df['prodcat'].value_counts().values,
        labels={"x": "Catégories de Produits", "y": "Nombre"},
        title="Répartition des Catégories de Produits"
    )
    st.plotly_chart(fig_product)

    fig_hazard = px.pie(
        df['hazcat'].value_counts(),
        values=df['hazcat'].value_counts().values,
        names=df['hazcat'].value_counts().index,
        title="Répartition des Catégories de Dangers"
    )
    st.plotly_chart(fig_hazard)

# Analyse statistique avec Chi2
def perform_statistical_analysis(df: pd.DataFrame):
    st.title("Analyse Statistique : Test Chi2")
    contingency_table = pd.crosstab(df['prodcat'], df['hazcat'])
    st.write("Table de Contingence", contingency_table)

    chi2_stat, p_value, dof, expected = chi2_contingency(contingency_table)
    st.write(f"**Chi2 Statistique**: {chi2_stat:.2f}")
    st.write(f"**P-value**: {p_value:.4f}")

    if p_value < 0.05:
        st.success("Résultat statistiquement significatif (P-value < 0.05).")
    else:
        st.warning("Résultat non significatif (P-value >= 0.05).")

    st.subheader("Heatmap : Produits vs Dangers")
    fig, ax = plt.subplots(figsize=(12, 8))
    sns.heatmap(contingency_table, annot=True, cmap="coolwarm", fmt="d", ax=ax)
    st.pyplot(fig)

# Main
def main():
    st.title("RASFF Data Analysis and Management")

    # Initialiser la base de données
    initialize_database()

    # Charger les données
    df = load_data_from_db()

    # Navigation
    menu = st.sidebar.selectbox(
        "Menu",
        ["Dashboard", "Analyse Statistique", "Pousser vers GitHub"]
    )

    if menu == "Dashboard":
        st.header("Tableau de Bord")
        display_statistics(df)
        display_visualizations(df)

    elif menu == "Analyse Statistique":
        st.header("Analyse Statistique")
        perform_statistical_analysis(df)

    elif menu == "Pousser vers GitHub":
        if st.button("Pousser le fichier .db vers GitHub"):
            push_db_to_github()

if __name__ == "__main__":
    main()
