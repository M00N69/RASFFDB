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

# Mappings des colonnes entre les fichiers Excel et le format attendu
WEEKLY_COLUMN_MAPPING = {
    "reference": "reference",
    "category": "product_category",
    "type": "product_type",
    "subject": "subject",
    "date": "date_of_case",
    "notifying_country": "notification_from",
    "classification": "classification",
    "risk_decision": "risk_decision",
    "distribution": "distribution",
    "forAttention": "for_attention",
    "forFollowUp": "for_follow_up",
    "operator": "operator",
    "origin": "country_origin",
    "hazards": "hazard_substance"
}

# Mappings des catégories de produits
PRODUCT_CATEGORY_MAPPING = {
    "fruits and vegetables": ["Fruits and Vegetables", "Fruits and Vegetables"],
    "cocoa and cocoa preparations, coffee and tea": ["Cocoa, Coffee, and Tea", "Beverages"],
    "dietetic foods, food supplements and fortified foods": ["Dietetic Foods and Supplements", "Specialty Foods"],
    "poultry meat and poultry meat products": ["Poultry Meat", "Meat Products"],
    "nuts, nut products and seeds": ["Nuts and Seeds", "Seeds and Nuts"],
    "other food product / mixed": ["Mixed Food Products", "Other"],
    "pet food": ["Pet Food", "Animal Feed"],
    "plant protection products": ["Plant Protection Products", "Additives"],
    "prepared dishes and snacks": ["Prepared Dishes and Snacks", "Prepared Foods"],
    "soups, broths, sauces and condiments": ["Soups, Broths, Sauces", "Prepared Foods"],
    "water for human consumption (other)": ["Water (Human Consumption)", "Beverages"],
    "wine": ["Wine", "Beverages"]
}

# Mappings des catégories de dangers
HAZARD_CATEGORY_MAPPING = {
    "Forchlorfenuron, chlorfenapyr": ["Unauthorised Substance", "Chemical Hazard"],
    "fenobucarb": ["Pesticide Residues", "Pesticide Hazard"],
    "tadalafil": ["Unauthorised Substance", "Chemical Hazard"],
    "Salmonella Enteritidis, Salmonella infantis, Salmonella paratyphi b": ["Pathogenic Micro-organisms", "Biological Hazard"],
    "Dead insects": ["Foreign Bodies", "Physical Hazard"]
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
                product_category TEXT,
                hazard_substance TEXT,
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
def download_and_clean_weekly_data(year, week):
    """
    Télécharge et traite les données RASFF pour une année et une semaine spécifiques.
    """
    url_template = "https://www.sirene-diffusion.fr/regia/000-rasff/{}/rasff-{}-{}.xls"
    url = url_template.format(str(year)[2:], year, str(week).zfill(2))
    response = requests.get(url)
    if response.status_code == 200:
        try:
            # Télécharger le fichier Excel
            df = pd.read_excel(BytesIO(response.content))
            
            # Vérifier si les colonnes attendues existent
            missing_columns = [col for col in WEEKLY_COLUMN_MAPPING.keys() if col not in df.columns]
            if missing_columns:
                st.warning(f"Missing columns in week {week} (Year {year}): {missing_columns}")
                return pd.DataFrame()
            
            # Renommer les colonnes
            df = df.rename(columns=WEEKLY_COLUMN_MAPPING)
            
            # Appliquer les mappings pour les catégories de produits et de dangers
            df = apply_mappings(df)
            
            st.success(f"Data for week {week} (Year {year}) loaded successfully.")
            return df
        except Exception as e:
            st.warning(f"Failed to process data for week {week} (Year {year}): {e}")
    else:
        st.warning(f"Data for week {week} (Year {year}) could not be downloaded.")
    return pd.DataFrame()

# Appliquer les mappings
def apply_mappings(df):
    """
    Applique les mappings pour les catégories de produits et de dangers.
    """
    # Mapping des catégories de produits
    df[['prodcat', 'groupprod']] = df['product_category'].apply(
        lambda x: pd.Series(PRODUCT_CATEGORY_MAPPING.get(str(x).lower(), ["Unknown", "Unknown"]))
    )
    
    # Mapping des catégories de dangers
    df[['hazcat', 'grouphaz']] = df['hazard_substance'].apply(
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
        ["Dashboard", "Statistical Analysis", "Add Weekly Data"],
        icons=["house", "bar-chart", "plus"],
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
    elif selected_page == "Add Weekly Data":
        st.header("Add Weekly Data")
        
        # Slider pour sélectionner l'année de début et de fin
        start_year = st.slider("Start Year", 2024, 2025, 2024)
        end_year = st.slider("End Year", 2024, 2025, 2025)
        
        # Slider pour sélectionner la semaine de début et de fin
        start_week = st.slider("Start Week", 1, 53, 1)
        end_week = st.slider("End Week", 1, 53, 1)
        
        # Afficher la période sélectionnée
        st.write(f"Selected Period: {start_year}-{start_week:02d} to {end_year}-{end_week:02d}")
        
        if st.button("Download and Add Data"):
            all_data = pd.DataFrame()
            for year in range(start_year, end_year + 1):
                start = start_week if year == start_year else 1
                end = end_week if year == end_year else 53
                for week in range(start, end + 1):
                    new_data = download_and_clean_weekly_data(year, week)
                    if not new_data.empty:
                        all_data = pd.concat([all_data, new_data], ignore_index=True)
            
            if not all_data.empty:
                save_to_database(all_data)
                st.success(f"Data for period {start_year}-{start_week:02d} to {end_year}-{end_week:02d} added successfully.")
                st.write("Added Data:", all_data)
            else:
                st.warning("No data was added.")

if __name__ == "__main__":
    main()
