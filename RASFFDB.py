import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import sqlite3
import requests
from io import BytesIO
from datetime import datetime
from scipy.stats import chi2_contingency
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px

# Configuration
DB_FILE = "rasff_data.db"  # Base de données SQLite persistante
MAIN_DATA_URL = "https://raw.githubusercontent.com/M00N69/RASFFPORTAL/main/unified_rasff_data_with_grouping.csv"

# Mappings des colonnes entre les fichiers Excel et le format attendu
WEEKLY_COLUMN_MAPPING = {
    "Date of Case": "date_of_case",
    "Reference": "reference",
    "Notification From": "notification_from",
    "Country Origin": "country_origin",
    "Product": "product",
    "Product Category": "product_category",
    "Hazard Substance": "hazard_substance",
    "Hazard Category": "hazard_category"
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
    """
    Initialise la base de données SQLite en créant la table 'rasff_data' si elle n'existe pas.
    """
    with sqlite3.connect(DB_FILE) as connection:
        cursor = connection.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS rasff_data (
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

# Charger les données de la base SQLite
@st.cache_data
def load_data_from_db():
    """
    Charge les données de la base de données SQLite dans un DataFrame pandas.
    """
    with sqlite3.connect(DB_FILE) as connection:
        df = pd.read_sql_query("SELECT * FROM rasff_data", connection)
    if 'date_of_case' in df.columns:
        df['date_of_case'] = pd.to_datetime(df['date_of_case'], errors='coerce')
    return df

# Sauvegarder dans la base SQLite
def save_to_database(data):
    """
    Sauvegarde les données dans la base de données SQLite.
    """
    if data is not None:
        with sqlite3.connect(DB_FILE) as connection:
            data.to_sql("rasff_data", connection, if_exists="append", index=False)

# Télécharger et traiter les données hebdomadaires
def download_and_clean_weekly_data(year, weeks):
    """
    Télécharge et traite les données RASFF pour une année et des semaines spécifiques.
    """
    url_template = "https://www.sirene-diffusion.fr/regia/000-rasff/{}/rasff-{}-{}.xls"
    dfs = []
    for week in weeks:
        url = url_template.format(str(year)[2:], year, str(week).zfill(2))
        response = requests.get(url)
        if response.status_code == 200:
            try:
                df = pd.read_excel(BytesIO(response.content))
                df = df.rename(columns=WEEKLY_COLUMN_MAPPING)
                df = apply_mappings(df)
                dfs.append(df)
                st.info(f"Data for week {week} loaded successfully.")
            except Exception as e:
                st.warning(f"Failed to process data for week {week}: {e}")
        else:
            st.warning(f"Data for week {week} could not be downloaded.")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# Appliquer les mappings
def apply_mappings(df):
    """
    Applique les mappings pour les catégories de produits et de dangers.
    """
    df[['prodcat', 'groupprod']] = df['product_category'].apply(
        lambda x: pd.Series(PRODUCT_CATEGORY_MAPPING.get(str(x).lower(), ["Unknown", "Unknown"]))
    )
    df[['hazcat', 'grouphaz']] = df['hazard_category'].apply(
        lambda x: pd.Series(HAZARD_CATEGORY_MAPPING.get(str(x).lower(), ["Unknown", "Unknown"]))
    )
    return df

# Afficher le tableau de bord
def display_dashboard(df):
    """
    Affiche le tableau de bord avec des statistiques et des visualisations.
    """
    st.header("Key Statistics")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Notifications", len(df))
    col2.metric("Unique Product Categories", df['prodcat'].nunique())
    col3.metric("Unique Hazard Categories", df['hazcat'].nunique())

    st.header("Visualizations")
    fig_notifying_map = px.choropleth(
        df.groupby('notification_from').size().reset_index(name='count'),
        locations='notification_from',
        locationmode='country names',
        color='count',
        scope="europe",
        title="European Map of Notifying Countries",
        color_continuous_scale='Blues'
    )
    st.plotly_chart(fig_notifying_map)

# Afficher l'analyse statistique
def show_statistical_analysis(df):
    """
    Affiche l'analyse statistique avec des tests Chi2 et des visualisations.
    """
    st.title("Statistical Analysis: Correlation and Chi2 Test")
    st.write("This section provides an advanced statistical analysis of the RASFF data, identifying significant correlations between products, hazards, and notifying countries.")

    # Filtrage interactif
    st.sidebar.header("Filter Options for Statistical Analysis")
    selected_prod_categories = st.sidebar.multiselect("Select Product Categories", sorted(df['prodcat'].dropna().unique()))
    selected_hazard_categories = st.sidebar.multiselect("Select Hazard Categories", sorted(df['hazcat'].dropna().unique()))
    selected_notifying_countries = st.sidebar.multiselect("Select Notifying Countries", sorted(df['notification_from'].dropna().unique()))
    
    # Application des filtres
    filtered_df = df.copy()
    if selected_prod_categories:
        filtered_df = filtered_df[filtered_df['prodcat'].isin(selected_prod_categories)]
    if selected_hazard_categories:
        filtered_df = filtered_df[filtered_df['hazcat'].isin(selected_hazard_categories)]
    if selected_notifying_countries:
        filtered_df = filtered_df[filtered_df['notification_from'].isin(selected_notifying_countries)]

    # Vérifiez si les données filtrées sont suffisantes pour l'analyse
    if filtered_df.empty:
        st.warning("No data available for the selected filters. Please adjust the filters.")
        return

    ### Test Chi2 : Catégories de Produits vs Catégories de Dangers ###
    st.subheader("Chi2 Test: Product Categories vs Hazard Categories")
    contingency_table = pd.crosstab(filtered_df['prodcat'], filtered_df['hazcat'])
    st.write("Contingency Table (Filtered Data)", contingency_table)

    # Test du Chi2
    chi2_stat, p_value, dof, expected = chi2_contingency(contingency_table)
    st.write(f"**Chi2 Statistic**: {chi2_stat:.2f}")
    st.write(f"**P-value**: {p_value:.4f}")
    st.write(f"**Degrees of Freedom (dof)**: {dof}")
    st.write("**Expected Frequencies Table**:", pd.DataFrame(expected, index=contingency_table.index, columns=contingency_table.columns))

    # Analyse du résultat
    if p_value < 0.05:
        st.success("The result is **statistically significant** (P-value < 0.05). This indicates a strong association between product categories and hazard categories.")
    else:
        st.warning("The result is **not statistically significant** (P-value >= 0.05). This indicates no strong association between product categories and hazard categories.")

    ### Visualisation Heatmap ###
    st.subheader("Heatmap: Product Categories vs Hazard Categories")
    fig, ax = plt.subplots(figsize=(10, 8))
    sns.heatmap(contingency_table, annot=True, fmt="d", cmap="coolwarm", ax=ax)
    st.pyplot(fig)

    ### Affichage des associations les plus fortes ###
    st.subheader("Top Significant Associations")
    top_associations = (
        contingency_table.stack()
        .reset_index(name='count')
        .sort_values(by='count', ascending=False)
        .head(10)
    )
    st.write("Top 10 Associations (Filtered Data)", top_associations)

    # Graphique des top associations
    fig_bar = px.bar(
        top_associations, 
        x='prodcat', 
        y='count', 
        color='hazcat', 
        title="Top Associations between Product Categories and Hazards",
        labels={"prodcat": "Product Category", "count": "Count", "hazcat": "Hazard Category"}
    )
    st.plotly_chart(fig_bar)

# Exécuter l'application
def main():
    st.set_page_config(page_title="RASFF Data Dashboard", layout="wide")
    initialize_database()

    # Navigation
    selected_page = option_menu(
        "RASFF Dashboard",
        ["Dashboard", "Statistical Analysis"],
        icons=["house", "bar-chart"],
        menu_icon="menu",
        default_index=0,
        orientation="horizontal"
    )

    # Charger les données
    df = load_data_from_db()

    # Afficher la page sélectionnée
    if selected_page == "Dashboard":
        display_dashboard(df)
    elif selected_page == "Statistical Analysis":
        show_statistical_analysis(df)

if __name__ == "__main__":
    main()
