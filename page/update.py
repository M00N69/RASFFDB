import streamlit as st
import pandas as pd
import sqlite3
import requests
from io import BytesIO

# === CONFIGURATION ===
DB_PATH = "rasff_data.db"
YEAR = 2025  # Modifier selon l'année souhaitée
WEEKS = list(range(1, 53))  # Semaines à vérifier

# Mapping des colonnes XLS → Base SQLite
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

def download_xls(year, week):
    """Télécharge un fichier XLS à partir de l'URL"""
    url = f"https://www.sirene-diffusion.fr/regia/000-rasff/{str(year)[2:]}/rasff-{year}-{str(week).zfill(2)}.xls"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return BytesIO(response.content)
    except requests.RequestException:
        return None

def extract_and_clean_xls(xls_data):
    """Lit et nettoie les données d'un fichier XLS"""
    df = pd.read_excel(xls_data)
    
    # Renommer les colonnes selon le mapping
    df = df.rename(columns=COLUMN_MAPPING)
    
    # Ajouter les colonnes manquantes avec valeur None
    for col in COLUMN_MAPPING.values():
        if col not in df.columns:
            df[col] = None
    
    # Convertir les dates
    df["date_of_case"] = pd.to_datetime(df["date_of_case"], errors="coerce").dt.strftime("%Y-%m-%d")
    
    return df[list(COLUMN_MAPPING.values())]  # Garder uniquement les colonnes utiles

def update_database(new_data):
    """Insère les nouvelles données dans la base SQLite en évitant les doublons"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Vérifier les références existantes pour éviter les doublons
    existing_refs = pd.read_sql("SELECT reference FROM rasff_data", conn)["reference"].tolist()
    new_data = new_data[~new_data["reference"].isin(existing_refs)]
    
    # Insérer les nouvelles données
    if not new_data.empty:
        new_data.to_sql("rasff_data", conn, if_exists="append", index=False)
        st.success(f"{len(new_data)} nouvelles alertes ajoutées !")
    else:
        st.info("Aucune nouvelle alerte à ajouter.")
    
    conn.close()
    return new_data

# === INTERFACE STREAMLIT ===
st.title("🔄 Mise à jour des alertes RASFF")

selected_weeks = st.multiselect("Sélectionnez les semaines à mettre à jour :", WEEKS, default=WEEKS[:4])

if st.button("Mettre à jour la base de données"):
    all_new_data = []
    
    for week in selected_weeks:
        st.write(f"🔍 Vérification de la semaine {week}...")
        xls_data = download_xls(YEAR, week)
        
        if xls_data:
            df = extract_and_clean_xls(xls_data)
            st.write(f"✅ Données chargées pour la semaine {week} : {df.shape[0]} alertes.")
            all_new_data.append(df)
        else:
            st.warning(f"❌ Impossible de récupérer les données pour la semaine {week}.")

    # Mettre à jour la base de données si des données ont été récupérées
    if all_new_data:
        full_data = pd.concat(all_new_data, ignore_index=True)
        inserted_data = update_database(full_data)
        st.dataframe(inserted_data)  # Afficher un aperçu des nouvelles données insérées
