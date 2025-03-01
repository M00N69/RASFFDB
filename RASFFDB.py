import streamlit as st
import pandas as pd
import sqlite3
import requests
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO
import datetime
import time
from st_aggrid import AgGrid, GridOptionsBuilder
import altair as alt
import os

# === CONFIGURATION STREAMLIT ===
st.set_page_config(
    page_title="RASFF Alerts Dashboard",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS pour améliorer l'interface
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E88E5;
        font-weight: 700;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #004D40;
        font-weight: 600;
    }
    .card {
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        background-color: white;
        margin-bottom: 1rem;
    }
    .info-box {
        background-color: #E3F2FD;
        padding: 1rem;
        border-radius: 5px;
        border-left: 5px solid #1E88E5;
    }
    .success-box {
        background-color: #E8F5E9;
        padding: 1rem;
        border-radius: 5px;
        border-left: 5px solid #4CAF50;
    }
    .warning-box {
        background-color: #FFF8E1;
        padding: 1rem;
        border-radius: 5px;
        border-left: 5px solid #FFB300;
    }
    .error-box {
        background-color: #FFEBEE;
        padding: 1rem;
        border-radius: 5px;
        border-left: 5px solid #F44336;
    }
    .stButton>button {
        width: 100%;
        border-radius: 5px;
        height: 3rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# === CONFIGURATION BASE DE DONNÉES ===
DB_PATH = "rasff_data.db"
CURRENT_YEAR = datetime.datetime.now().year
CURRENT_WEEK = datetime.datetime.now().isocalendar()[1]
WEEKS = list(range(1, 53))

# Configuration pour le cache
cache_ttl = 3600  # 1 heure

# Mapping des colonnes du fichier XLS vers SQLite
COLUMN_MAPPING = {
    "date": "date_of_case",
    "reference": "reference",
    "notifying_country": "notification_from",
    "origin": "country_origin",
    "category": "product_category",
    "subject": "product",
    "hazards": "hazard_substance",
    "classification": "hazard_category",
}

# === FONCTIONS ===
@st.cache_data(ttl=cache_ttl)
def create_database():
    """Crée la base de données si elle n'existe pas"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rasff_data (
            date_of_case TEXT,
            reference TEXT PRIMARY KEY,
            notification_from TEXT,
            country_origin TEXT,
            product_category TEXT,
            product TEXT,
            hazard_substance TEXT,
            hazard_category TEXT,
            year INTEGER,
            month INTEGER,
            week INTEGER
        )
    """)
    conn.commit()
    conn.close()

@st.cache_data(ttl=cache_ttl)
def get_last_year_week():
    """Récupère la dernière année et semaine enregistrée"""
    if not os.path.exists(DB_PATH):
        create_database()
        return CURRENT_YEAR - 1, CURRENT_WEEK  # Default if database doesn't exist yet

    conn = sqlite3.connect(DB_PATH)
    query = "SELECT MAX(date_of_case) FROM rasff_data"
    result = pd.read_sql(query, conn).iloc[0, 0]
    conn.close()

    if result and pd.notna(result):
        last_date = pd.to_datetime(result)
        last_year, last_week = last_date.year, last_date.isocalendar()[1]
        return last_year, last_week
    return CURRENT_YEAR - 1, CURRENT_WEEK  # Par défaut si base vide

@st.cache_data(ttl=60)  # Cache for 1 minute only as this is a network operation
def download_xls(year, week):
    """Télécharge un fichier XLS depuis l'URL RASFF avec barre de progression"""
    with st.spinner(f"📥 Téléchargement des données pour {year}-S{str(week).zfill(2)}..."):
        url = f"https://www.sirene-diffusion.fr/regia/000-rasff/{str(year)[2:]}/rasff-{year}-{str(week).zfill(2)}.xls"
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            return BytesIO(response.content)
        except requests.RequestException as e:
            st.error(f"Erreur lors du téléchargement : {e}")
            return None

def extract_and_clean_xls(xls_data):
    """Lit et nettoie les données d'un fichier XLS"""
    try:
        df = pd.read_excel(xls_data)

        # Rename columns based on mapping
        df = df.rename(columns=COLUMN_MAPPING)

        # Ensure all expected columns exist
        for col in COLUMN_MAPPING.values():
            if col not in df.columns:
                df[col] = None

        # Convert date
        df["date_of_case"] = pd.to_datetime(df["date_of_case"], errors="coerce")

        # Add year, month and week columns
        df["year"] = df["date_of_case"].dt.year
        df["month"] = df["date_of_case"].dt.month
        df["week"] = df["date_of_case"].dt.isocalendar().week

        # Convert date back to string for SQLite
        df["date_of_case"] = df["date_of_case"].dt.strftime("%Y-%m-%d")

        return df[list(COLUMN_MAPPING.values()) + ["year", "month", "week"]]
    except Exception as e:
        st.error(f"Erreur lors du traitement du fichier Excel: {e}")
        return pd.DataFrame()

def update_database(new_data):
    """Insère les nouvelles données dans SQLite en évitant les doublons"""
    if new_data.empty:
        return pd.DataFrame()

    conn = sqlite3.connect(DB_PATH)
    existing_refs = set(pd.read_sql("SELECT reference FROM rasff_data", conn)["reference"])

    new_data = new_data[~new_data["reference"].isin(existing_refs)]

    if not new_data.empty:
        new_data.to_sql("rasff_data", conn, if_exists="append", index=False)

    conn.close()
    return new_data

@st.cache_data(ttl=cache_ttl)
def get_clean_dataframe():
    """Récupère et nettoie les données de la base SQLite"""
    if not os.path.exists(DB_PATH):
        create_database()
        return pd.DataFrame()

    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM rasff_data", conn)
    conn.close()

    if df.empty:
        return pd.DataFrame()

    df["date_of_case"] = pd.to_datetime(df["date_of_case"], errors="coerce")
    df = df.dropna(subset=["date_of_case"])
    return df

def create_interactive_table(df):
    """Crée une table interactive avec AgGrid"""
    if df.empty:
        st.warning("⚠️ Aucune donnée à afficher.")
        return

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=15)
    gb.configure_column("date_of_case", headerName="Date", type=["dateColumnFilter"], sortable=True)
    gb.configure_column("reference", headerName="Référence", sortable=True, filter=True)
    gb.configure_column("notification_from", headerName="Pays notifiant", sortable=True, filter=True)
    gb.configure_column("country_origin", headerName="Pays d'origine", sortable=True, filter=True)
    gb.configure_column("product_category", headerName="Catégorie", sortable=True, filter=True)
    gb.configure_column("product", headerName="Produit", sortable=True, filter=True)
    gb.configure_column("hazard_substance", headerName="Substance", sortable=True, filter=True)
    gb.configure_column("hazard_category", headerName="Type de danger", sortable=True, filter=True)

    gb.configure_selection('single')
    gridOptions = gb.build()

    grid_response = AgGrid(
        df,
        gridOptions=gridOptions,
        data_return_mode='AS_INPUT',
        update_mode='MODEL_CHANGED',
        fit_columns_on_grid_load=False,
        theme='streamlit',
        enable_enterprise_modules=False,
        height=400,
        width='100%',
        reload_data=False
    )

    return grid_response

def create_monthly_chart(df, selected_year):
    """Crée un graphique interactif des alertes par mois"""
    if df.empty:
        return None

    # Filter by year
    year_df = df[df["date_of_case"].dt.year == selected_year]

    if year_df.empty:
        return None

    # Count alerts by month
    monthly_counts = year_df["date_of_case"].dt.month.value_counts().sort_index()
    monthly_data = pd.DataFrame({
        'Mois': [f"{m}" for m in monthly_counts.index],
        'Nombre': monthly_counts.values
    })

    # Create bar chart with Plotly
    fig = px.bar(
        monthly_data,
        x='Mois',
        y='Nombre',
        title=f"Alertes par mois en {selected_year}",
        labels={'Nombre': "Nombre d'alertes"},
        color='Nombre',
        color_continuous_scale='Blues'
    )

    fig.update_layout(
        xaxis_title="Mois",
        yaxis_title="Nombre d'alertes",
        height=400,
        template="plotly_white"
    )

    return fig

def create_country_chart(df, selected_year):
    """Crée un graphique des alertes par pays d'origine"""
    if df.empty:
        return None

    # Filter by year
    year_df = df[df["date_of_case"].dt.year == selected_year]

    if year_df.empty:
        return None

    # Get top 10 countries
    top_countries = year_df["country_origin"].value_counts().nlargest(10)
    country_data = pd.DataFrame({
        'Pays': top_countries.index,
        'Nombre': top_countries.values
    })

    # Create horizontal bar chart with Plotly
    fig = px.bar(
        country_data,
        y='Pays',
        x='Nombre',
        title=f"Top 10 pays d'origine des alertes en {selected_year}",
        orientation='h',
        color='Nombre',
        color_continuous_scale='Reds'
    )

    fig.update_layout(
        yaxis=dict(autorange="reversed"),
        height=400,
        template="plotly_white"
    )

    return fig

def create_category_chart(df, selected_year):
    """Crée un graphique des alertes par catégorie de produit"""
    if df.empty:
        return None

    # Filter by year
    year_df = df[df["date_of_case"].dt.year == selected_year]

    if year_df.empty:
        return None

    # Get top 10 categories
    top_categories = year_df["product_category"].value_counts().nlargest(10)
    category_data = pd.DataFrame({
        'Catégorie': top_categories.index,
        'Nombre': top_categories.values
    })

    # Create pie chart with Plotly
    fig = px.pie(
        category_data,
        names='Catégorie',
        values='Nombre',
        title=f"Répartition par catégorie de produit en {selected_year}",
        hole=0.4
    )

    fig.update_layout(
        height=400,
        template="plotly_white"
    )

    return fig

def create_hazard_chart(df, selected_year):
    """Crée un graphique des alertes par type de danger"""
    if df.empty:
        return None

    # Filter by year
    year_df = df[df["date_of_case"].dt.year == selected_year]

    if year_df.empty:
        return None

    # Get hazard categories
    hazard_counts = year_df["hazard_category"].value_counts()
    hazard_data = pd.DataFrame({
        'Type': hazard_counts.index,
        'Nombre': hazard_counts.values
    })

    # Create treemap with Plotly
    fig = px.treemap(
        hazard_data,
        path=['Type'],
        values='Nombre',
        title=f"Types de dangers signalés en {selected_year}",
        color='Nombre',
        color_continuous_scale='Greens'
    )

    fig.update_layout(
        height=400,
        template="plotly_white"
    )

    return fig

# === INTERFACE STREAMLIT ===

# Sidebar
with st.sidebar:
    st.image("https://ec.europa.eu/food/sites/food/files/safety/img/rasff-header-small.png", width=200)
    st.markdown("## 🔄 Mise à jour des données")

    create_database()
    last_year, last_week = get_last_year_week()

    st.markdown(f"""
    <div class="info-box">
        <h3>📆 Dernière mise à jour</h3>
        <p>Année {last_year} - Semaine {last_week}</p>
    </div>
    """, unsafe_allow_html=True)

    # Calculate weeks to update, with better logic
    current_date = datetime.datetime.now()
    current_year, current_week = current_date.year, current_date.isocalendar()[1]

    # Create a list of (year, week) tuples for all weeks since the last update
    all_weeks_to_update = []

    # Current year weeks
    for w in range(1, current_week + 1):
        if (current_year, w) > (last_year, last_week):
            all_weeks_to_update.append((current_year, w))

    # Previous year weeks (if last update was in previous year)
    if last_year < current_year:
        for w in range(last_week + 1, 53):
            all_weeks_to_update.append((last_year, w))

    # Sort by year and week
    all_weeks_to_update.sort()

    # Display as options
    options = [f"{y}-S{str(w).zfill(2)}" for y, w in all_weeks_to_update]

    if options:
        st.write("### 📅 Semaines à mettre à jour")
        selected_options = st.multiselect(
            "Sélectionnez les semaines:",
            options,
            default=options[:min(5, len(options))]
        )

        selected_weeks = [(int(opt.split('-S')[0]), int(opt.split('-S')[1])) for opt in selected_options]

        if st.button("🔄 Mettre à jour la base", key="update_button"):
            progress_bar = st.progress(0)
            status_text = st.empty()
            total_new = 0

            for i, (year, week) in enumerate(selected_weeks):
                status_text.text(f"Traitement de {year}-S{str(week).zfill(2)}...")
                xls_data = download_xls(year, week)

                if xls_data:
                    df = extract_and_clean_xls(xls_data)
                    if not df.empty:
                        new_data = update_database(df)
                        new_count = len(new_data)
                        total_new += new_count
                        st.markdown(f"""
                        <div class="{'success-box' if new_count > 0 else 'info-box'}">
                            {year}-S{str(week).zfill(2)}: {new_count} nouvelle(s) alerte(s)
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.markdown(f"""
                        <div class="warning-box">
                            {year}-S{str(week).zfill(2)}: Fichier vide ou mal formaté
                        </div>
                        """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div class="error-box">
                        {year}-S{str(week).zfill(2)}: Fichier non disponible
                    </div>
                    """, unsafe_allow_html=True)

                # Update progress
                progress = (i + 1) / len(selected_weeks)
                progress_bar.progress(progress)
                time.sleep(0.1)  # Pour visualiser la progression

            progress_bar.progress(1.0)
            time.sleep(0.5)
            progress_bar.empty()

            if total_new > 0:
                st.markdown(f"""
                <div class="success-box">
                    <h3>✅ Mise à jour terminée</h3>
                    <p>{total_new} nouvelle(s) alerte(s) ajoutée(s)</p>
                </div>
                """, unsafe_allow_html=True)
                # Clear cache to refresh data
                st.cache_data.clear()
            else:
                st.markdown("""
                <div class="info-box">
                    <h3>ℹ️ Aucune nouvelle alerte</h3>
                    <p>La base de données est à jour.</p>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="success-box">
            <h3>✅ Base de données à jour</h3>
            <p>Toutes les données sont à jour.</p>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### 🔍 À propos")
    st.markdown("""
    Cette application permet de suivre les alertes RASFF (Rapid Alert System for Food and Feed)
    de l'Union Européenne concernant la sécurité alimentaire.

    Les données sont téléchargées depuis le site de diffusion officiel.
    """)

# Main content
st.markdown('<h1 class="main-header">🚨 Tableau de bord RASFF</h1>', unsafe_allow_html=True)

# Tabs for different views
tab1, tab2 = st.tabs(["📊 Dashboard", "📋 Données brutes"])

with tab1:
    # Dashboard view
    df = get_clean_dataframe()

    if not df.empty:
        # Metrics row
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            total_alerts = len(df)
            st.metric("Total des alertes", f"{total_alerts:,}")

        with col2:
            current_year_alerts = len(df[df["date_of_case"].dt.year == CURRENT_YEAR])
            last_year_alerts = len(df[df["date_of_case"].dt.year == CURRENT_YEAR - 1])
            delta = current_year_alerts - last_year_alerts
            delta_percent = (delta / last_year_alerts * 100) if last_year_alerts > 0 else 0
            st.metric(f"Alertes {CURRENT_YEAR}", f"{current_year_alerts:,}", f"{delta_percent:.1f}%")

        with col3:
            current_month = datetime.datetime.now().month
            current_month_alerts = len(df[(df["date_of_case"].dt.year == CURRENT_YEAR) &
                                         (df["date_of_case"].dt.month == current_month)])
            st.metric(f"Alertes du mois", f"{current_month_alerts:,}")

        with col4:
            countries_count = df["country_origin"].nunique()
            st.metric("Pays concernés", f"{countries_count:,}")

        # Year selector
        years = sorted(df["date_of_case"].dt.year.unique())
        selected_year = st.selectbox("📅 Sélectionnez une année", years, index=len(years) - 1)

        # Charts row 1
        col1, col2 = st.columns(2)

        with col1:
            monthly_chart = create_monthly_chart(df, selected_year)
            if monthly_chart:
                st.plotly_chart(monthly_chart, use_container_width=True)
            else:
                st.info(f"Pas de données disponibles pour {selected_year}")

        with col2:
            country_chart = create_country_chart(df, selected_year)
            if country_chart:
                st.plotly_chart(country_chart, use_container_width=True)
            else:
                st.info(f"Pas de données disponibles pour {selected_year}")

        # Charts row 2
        col1, col2 = st.columns(2)

        with col1:
            category_chart = create_category_chart(df, selected_year)
            if category_chart:
                st.plotly_chart(category_chart, use_container_width=True)
            else:
                st.info(f"Pas de données disponibles pour {selected_year}")

        with col2:
            hazard_chart = create_hazard_chart(df, selected_year)
            if hazard_chart:
                st.plotly_chart(hazard_chart, use_container_width=True)
            else:
                st.info(f"Pas de données disponibles pour {selected_year}")
    else:
        st.warning("⚠️ Aucune donnée en base. Veuillez mettre à jour la base de données.")
        st.info("👈 Utilisez le menu latéral pour mettre à jour les données.")

with tab2:
    # Raw data view
    st.markdown('<h2 class="sub-header">📋 Données brutes</h2>', unsafe_allow_html=True)

    df = get_clean_dataframe()

    if not df.empty:
        # Filtres interactifs
        col1, col2, col3 = st.columns(3)

        with col1:
            years = sorted(df["date_of_case"].dt.year.unique())
            selected_year = st.selectbox("📅 Année", years, index=len(years) - 1, key="year_filter")

        with col2:
            countries = ["Tous"] + sorted(df["country_origin"].dropna().unique())
            selected_country = st.selectbox("🌍 Pays d'origine", countries, key="country_filter")

        with col3:
            categories = ["Tous"] + sorted(df["product_category"].dropna().unique())
            selected_category = st.selectbox("📦 Catégorie de produit", categories, key="category_filter")

        # Apply filters
        df_filtered = df[df["date_of_case"].dt.year == selected_year]

        if selected_country != "Tous":
            df_filtered = df_filtered[df_filtered["country_origin"] == selected_country]

        if selected_category != "Tous":
            df_filtered = df_filtered[df_filtered["product_category"] == selected_category]

        # Show interactive table
        grid_response = create_interactive_table(df_filtered)

        # Export options
        col1, col2 = st.columns(2)

        with col1:
            if st.button("📥 Exporter en CSV"):
                csv = df_filtered.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Télécharger CSV",
                    data=csv,
                    file_name=f"rasff_data_{selected_year}.csv",
                    mime="text/csv",
                )

        with col2:
            if st.button("📥 Exporter en Excel"):
                output = BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_filtered.to_excel(writer, sheet_name='RASFF_Data', index=False)
                excel_data = output.getvalue()
                st.download_button(
                    label="📥 Télécharger Excel",
                    data=excel_data,
                    file_name=f"rasff_data_{selected_year}.xlsx",
                    mime="application/vnd.ms-excel",
                )
    else:
        st.warning("⚠️ Aucune donnée en base. Veuillez mettre à jour la base de données.")
        st.info("👈 Utilisez le menu latéral pour mettre à jour les données.")

# Footer
st.markdown("---")
st.markdown("Développé avec ❤️ pour la surveillance des alertes alimentaires")
