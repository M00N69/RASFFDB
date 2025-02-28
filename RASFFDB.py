import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import plotly.express as px
import requests
from io import BytesIO
from datetime import datetime
from scipy.stats import chi2_contingency
import seaborn as sns
import matplotlib.pyplot as plt
import threading
import sqlite3
import os

# Configuration du thème Streamlit
st.set_page_config(page_title="RASFF Data Dashboard", layout="wide")
st.markdown(
    """
    <style>
    .reportview-container {
        background: #f0f2f6;
    }
    .css-12oz5g7 {
        padding: 1rem 1rem 1.5rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# URL des données CSV principales
MAIN_DATA_URL = "https://raw.githubusercontent.com/M00N69/RASFFPORTAL/main/unified_rasff_data_with_grouping.csv"
DB_FILE = "rasff_data.db"

# Initialisation de la base de données SQLite
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
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
    ''')
    conn.commit()
    conn.close()

# Chargement des données depuis la base de données SQLite
@st.cache_data
def load_data_from_db() -> pd.DataFrame:
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT * FROM rasff_data", conn)
    conn.close()
    return df

# Standard column names expected in the main data
expected_columns = [
    "date_of_case", "reference", "notification_from", "country_origin",
    "product", "product_category", "hazard_substance", "hazard_category",
    "prodcat", "groupprod", "hazcat", "grouphaz"
]

# Column mapping for transforming weekly file structure to match main data
weekly_column_mapping = {
    "Date of Case": "date_of_case",
    "Reference": "reference",
    "Notification From": "notification_from",
    "Country Origin": "country_origin",
    "Product": "product",
    "Product Category": "product_category",
    "Hazard Substance": "hazard_substance",
    "Hazard Category": "hazard_category"
}

# Function to download and clean weekly data
def download_and_clean_weekly_data(year, weeks):
    url_template = "https://www.sirene-diffusion.fr/regia/000-rasff/{}/rasff-{}-{}.xls"
    dfs = []
    for week in weeks:
        url = url_template.format(str(year)[2:], year, str(week).zfill(2))
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

            try:
                # Attempt to read and transform the weekly data
                df = pd.read_excel(BytesIO(response.content))

                # Rename columns according to the mapping
                df = df.rename(columns=weekly_column_mapping)

                # Ensure all expected columns are present, filling missing columns with None
                for col in expected_columns:
                    if col not in df.columns:
                        df[col] = None  # Add missing column with default None values

                # Select and reorder columns to match the main DataFrame
                df = df[expected_columns]

                # Apply category mappings
                df = apply_mappings(df)

                dfs.append(df)
                st.info(f"Data for week {week} loaded successfully from {url}.")
            except Exception as e:
                st.warning(f"Failed to process data for week {week} from {url}: {e}")
        except requests.exceptions.RequestException as e:
            st.warning(f"Failed to download data for week {week} from {url}: {e}")

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        st.write(f"Combined new data shape: {combined_df.shape}")
        return combined_df
    else:
        st.warning("No new data was downloaded for the specified weeks.")
        return pd.DataFrame()  # Return an empty DataFrame if no files could be downloaded

# Apply category mappings
def apply_mappings(df: pd.DataFrame) -> pd.DataFrame:
    product_category_mapping = {
        "alcoholic beverages": ("Alcoholic Beverages", "Beverages"),
        "animal by-products": ("Animal By-products", "Animal Products"),
        "bivalve molluscs and products thereof": ("Bivalve Molluscs", "Seafood"),
        "cephalopods and products thereof": ("Cephalopods", "Seafood"),
        "cereals and bakery products": ("Cereals and Bakery Products", "Grains and Bakery"),
        "cocoa and cocoa preparations, coffee and tea": ("Cocoa, Coffee, and Tea", "Beverages"),
        "compound feeds": ("Compound Feeds", "Animal Feed"),
        "confectionery": ("Confectionery", "Grains and Bakery"),
        "crustaceans and products thereof": ("Crustaceans", "Seafood"),
        "dietetic foods, food supplements and fortified foods": ("Dietetic Foods and Supplements", "Specialty Foods"),
        "eggs and egg products": ("Eggs and Egg Products", "Animal Products"),
        "fats and oils": ("Fats and Oils", "Fats and Oils"),
        "feed additives": ("Feed Additives", "Animal Feed"),
        "feed materials": ("Feed Materials", "Animal Feed"),
        "feed premixtures": ("Feed Premixtures", "Animal Feed"),
        "fish and fish products": ("Fish and Fish Products", "Seafood"),
        "food additives and flavourings": ("Food Additives and Flavourings", "Additives"),
        "food contact materials": ("Food Contact Materials", "Packaging"),
        "fruits and vegetables": ("Fruits and Vegetables", "Fruits and Vegetables"),
        "gastropods": ("Gastropods", "Seafood"),
        "herbs and spices": ("Herbs and Spices", "Spices"),
        "honey and royal jelly": ("Honey and Royal Jelly", "Specialty Foods"),
        "ices and desserts": ("Ices and Desserts", "Grains and Bakery"),
        "live animals": ("Live Animals", "Animal Products"),
        "meat and meat products (other than poultry)": ("Meat (Non-Poultry)", "Meat Products"),
        "milk and milk products": ("Milk and Milk Products", "Dairy"),
        "natural mineral waters": ("Natural Mineral Waters", "Beverages"),
        "non-alcoholic beverages": ("Non-Alcoholic Beverages", "Beverages"),
        "nuts, nut products and seeds": ("Nuts and Seeds", "Seeds and Nuts"),
        "other food product / mixed": ("Mixed Food Products", "Other"),
        "pet food": ("Pet Food", "Animal Feed"),
        "plant protection products": ("Plant Protection Products", "Additives"),
        "poultry meat and poultry meat products": ("Poultry Meat", "Meat Products"),
        "prepared dishes and snacks": ("Prepared Dishes and Snacks", "Prepared Foods"),
        "soups, broths, sauces and condiments": ("Soups, Broths, Sauces", "Prepared Foods"),
        "water for human consumption (other)": ("Water (Human Consumption)", "Beverages"),
        "wine": ("Wine", "Beverages")
    }
    hazard_category_mapping = {
        "GMO / novel food": ("GMO / Novel Food", "Food Composition"),
        "TSEs": ("Transmissible Spongiform Encephalopathies (TSEs)", "Biological Hazard"),
        "adulteration / fraud": ("Adulteration / Fraud", "Food Fraud"),
        "allergens": ("Allergens", "Biological Hazard"),
        "biological contaminants": ("Biological Contaminants", "Biological Hazard"),
        "biotoxins (other)": ("Biotoxins", "Biological Hazard"),
        "chemical contamination (other)": ("Chemical Contamination", "Chemical Hazard"),
        "composition": ("Composition", "Food Composition"),
        "environmental pollutants": ("Environmental Pollutants", "Chemical Hazard"),
        "feed additives": ("Feed Additives", "Chemical Hazard"),
        "food additives and flavourings": ("Food Additives and Flavourings", "Additives"),
        "foreign bodies": ("Foreign Bodies", "Physical Hazard"),
        "genetically modified": ("Genetically Modified", "Food Composition"),
        "heavy metals": ("Heavy Metals", "Chemical Hazard"),
        "industrial contaminants": ("Industrial Contaminants", "Chemical Hazard"),
        "labelling absent/incomplete/incorrect": ("Labelling Issues", "Food Fraud"),
        "migration": ("Migration", "Chemical Hazard"),
        "mycotoxins": ("Mycotoxins", "Biological Hazard"),
        "natural toxins (other)": ("Natural Toxins", "Biological Hazard"),
        "non-pathogenic micro-organisms": ("Non-Pathogenic Micro-organisms", "Biological Hazard"),
        "not determined (other)": ("Not Determined", "Other"),
        "novel food": ("Novel Food", "Food Composition"),
        "organoleptic aspects": ("Organoleptic Aspects", "Other"),
        "packaging defective / incorrect": ("Packaging Issues", "Physical Hazard"),
        "parasitic infestation": ("Parasitic Infestation", "Biological Hazard"),
        "pathogenic micro-organisms": ("Pathogenic Micro-organisms", "Biological Hazard"),
        "pesticide residues": ("Pesticide Residues", "Pesticide Hazard"),
        "poor or insufficient controls": ("Insufficient Controls", "Food Fraud"),
        "radiation": ("Radiation", "Physical Hazard"),
        "residues of veterinary medicinal": ("Veterinary Medicinal Residues", "Chemical Hazard")
    }

    # Map Product Category
    df[['prodcat', 'groupprod']] = df['product_category'].apply(
        lambda x: pd.Series(product_category_mapping.get(str(x).lower(), ("Unknown", "Unknown")))
    )

    # Map Hazard Category
    df[['hazcat', 'grouphaz']] = df['hazard_category'].apply(
        lambda x: pd.Series(hazard_category_mapping.get(str(x).lower(), ("Unknown", "Unknown")))
    )

    return df

# Main RASFF Dashboard class
class RASFFDashboard:
    def __init__(self):
        self.data = load_data_from_db()
        self.data = apply_mappings(self.data)

    def render_sidebar(self) -> pd.DataFrame:
        df = self.data
        st.sidebar.header("Filter Options")

        # Date range selection
        if 'date_of_case' in df.columns and pd.api.types.is_datetime64_any_dtype(df['date_of_case']):
            min_date = df['date_of_case'].min()
            max_date = df['date_of_case'].max()

            start_date, end_date = st.sidebar.slider(
                "Select Date Range",
                min_value=min_date,
                max_value=max_date,
                value=(min_date, max_date),
                format="MM/YYYY"
            )

            df = df[(df['date_of_case'] >= start_date) & (df['date_of_case'] <= end_date)]
        else:
            st.sidebar.warning("Date of Case column is missing or invalid. Date filtering is disabled.")

        # Multiselect filters
        selected_prod_groups = st.sidebar.multiselect("Product Groups", sorted(df['groupprod'].dropna().unique()))
        selected_hazard_groups = st.sidebar.multiselect("Hazard Groups", sorted(df['grouphaz'].dropna().unique()))
        selected_notifying_countries = st.sidebar.multiselect("Notifying Countries", sorted(df['notification_from'].dropna().unique()))
        selected_origin_countries = st.sidebar.multiselect("Country of Origin", sorted(df['country_origin'].dropna().unique()))

        # Apply filters
        if selected_prod_groups:
            df = df[df['groupprod'].isin(selected_prod_groups)]
        if selected_hazard_groups:
            df = df[df['grouphaz'].isin(selected_hazard_groups)]
        if selected_notifying_countries:
            df = df[df['notification_from'].isin(selected_notifying_countries)]
        if selected_origin_countries:
            df = df[df['country_origin'].isin(selected_origin_countries)]

        return df

    def display_statistics(self, df: pd.DataFrame):
        st.header("Key Statistics")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Notifications", len(df))
        col2.metric("Unique Product Categories", df['prodcat'].nunique())
        col3.metric("Unique Hazard Categories", df['hazcat'].nunique())

    def display_visualizations(self, df: pd.DataFrame):
        st.header("Visualizations")

        # European Map for Notifying Countries
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

        # World Map for Origin Countries
        fig_origin_map = px.choropleth(
            df.groupby('country_origin').size().reset_index(name='count'),
            locations='country_origin',
            locationmode='country names',
            color='count',
            title="World Map of Origin Countries",
            color_continuous_scale='Reds'
        )
        st.plotly_chart(fig_origin_map)

        # Bar chart for product categories
        product_counts = df['prodcat'].value_counts().head(10)
        fig_bar = px.bar(product_counts, x=product_counts.index, y=product_counts.values,
                         title="Top 10 Product Categories",
                         color_discrete_sequence=px.colors.qualitative.Prism)
        st.plotly_chart(fig_bar)

        # Pie chart for hazard categories
        hazard_counts = df['hazcat'].value_counts().head(10)
        fig_pie = px.pie(hazard_counts, values=hazard_counts.values, names=hazard_counts.index,
                         title="Top 10 Hazard Categories",
                         color_discrete_sequence=px.colors.qualitative.Pastel)
        st.plotly_chart(fig_pie)

    def update_data_with_weeks(self, year, start_week):
        current_week = datetime.now().isocalendar()[1]
        weeks = range(start_week, current_week + 1)
        new_data = download_and_clean_weekly_data(year, weeks)
        if not new_data.empty:
            conn = sqlite3.connect(DB_FILE)
            new_data.to_sql('rasff_data', conn, if_exists='append', index=False)
            conn.close()
            st.info(f"Data for weeks {start_week} to {current_week} has been updated in the database.")
            # Recharge les données après la mise à jour
            self.data = load_data_from_db()
            self.data = apply_mappings(self.data)

    def run(self):
        st.title("RASFF Data Dashboard")

        # Mise à jour automatique des données
        st.sidebar.header("Mise à jour des données")
        auto_update = st.sidebar.checkbox("Activer la mise à jour automatique", value=False)

        if st.sidebar.button("Mettre à jour les données avec les nouvelles semaines"):
            current_year = datetime.now().year
            self.update_data_with_weeks(current_year, 1)

        # Affichage du DataFrame
        st.dataframe(self.data, height=500)

        # Filtres de la barre latérale
        filtered_df = self.render_sidebar()

        # Mise à jour automatique des données dans un thread séparé
        def auto_update_data():
            while True:
                current_year = datetime.now().year
                self.update_data_with_weeks(current_year, 1)
                import time
                time.sleep(86400)  # Mettre à jour les données quotidiennement (86400 secondes)

        if auto_update:
            threading.Thread(target=auto_update_data, daemon=True).start()

        # Affichage des statistiques
        self.display_statistics(filtered_df)

        # Display visualizations
        self.display_visualizations(filtered_df)

def show_statistical_analysis(df: pd.DataFrame):
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
        st.success(
            "The result is **statistically significant** (P-value < 0.05). This indicates a strong association between product categories and hazard categories."
        )
        st.write("""
        **Interpretation**:
        - Some product categories are more likely to be associated with certain hazard categories.
        - Explore the heatmap and bar charts below for insights on specific associations.
        """)
    else:
        st.warning(
            "The result is **not statistically significant** (P-value >= 0.05). This indicates no strong association between product categories and hazard categories."
        )

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

    ### Test Chi2 : Pays Notifiants vs Groupes de Dangers ###
    st.subheader("Chi2 Test: Notifying Countries vs Hazard Groups")
    country_hazard_table = pd.crosstab(filtered_df['notification_from'], filtered_df['grouphaz'])
    st.write("Contingency Table (Filtered Data)", country_hazard_table)

    # Test du Chi2
    chi2_stat, p_value, dof, expected = chi2_contingency(country_hazard_table)
    st.write(f"**Chi2 Statistic**: {chi2_stat:.2f}")
    st.write(f"**P-value**: {p_value:.4f}")
    st.write(f"**Degrees of Freedom (dof)**: {dof}")
    st.write("**Expected Frequencies Table**:", pd.DataFrame(expected, index=country_hazard_table.index, columns=country_hazard_table.columns))

    # Analyse du résultat
    if p_value < 0.05:
        st.success(
            "The result is **statistically significant** (P-value < 0.05). This indicates a strong association between notifying countries and hazard groups."
        )
        st.write("""
        **Interpretation**:
        - Certain countries are more likely to notify specific types of hazards.
        - Use the visualizations below to identify these patterns.
        """)
    else:
        st.warning(
            "The result is **not statistically significant** (P-value >= 0.05). This indicates no strong association between notifying countries and hazard groups."
        )

    ### Heatmap pour Pays Notifiants vs Groupes de Dangers ###
    st.subheader("Heatmap: Notifying Countries vs Hazard Groups")
    fig, ax = plt.subplots(figsize=(12, 8))
    sns.heatmap(country_hazard_table, annot=True, fmt="d", cmap="Blues", ax=ax)
    st.pyplot(fig)

    ### Associations importantes ###
    st.subheader("Top Significant Associations between Notifying Countries and Hazard Groups")
    top_country_associations = (
        country_hazard_table.stack()
        .reset_index(name='count')
        .sort_values(by='count', ascending=False)
        .head(10)
    )
    st.write("Top 10 Associations", top_country_associations)

    # Bar Chart des associations
    fig_bar_countries = px.bar(
        top_country_associations,
        x='notification_from',
        y='count',
        color='grouphaz',
        title="Top Associations between Notifying Countries and Hazard Groups",
        labels={"notification_from": "Notifying Country", "count": "Count", "grouphaz": "Hazard Group"}
    )
    st.plotly_chart(fig_bar_countries)

# Run the dashboard
def main():
    init_db()
    # Barre de navigation
    selected_page = option_menu(
        "RASFF Dashboard",
        ["Dashboard", "Statistical Analysis"],
        icons=["house", "bar-chart"],
        menu_icon="menu",
        default_index=0,
        orientation="horizontal"
    )

    # Initialisation des pages
    dashboard = RASFFDashboard()
    if selected_page == "Dashboard":
        dashboard.run()
    elif selected_page == "Statistical Analysis":
        show_statistical_analysis(dashboard.data)

if __name__ == "__main__":
    main()

