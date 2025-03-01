import streamlit as st
import pandas as pd
import sqlite3
import requests
import datetime
from io import BytesIO
import os

# Configuration Streamlit
st.set_page_config(
    page_title="🚨 RASFF Alerts",
    page_icon="🚨",
    layout="wide",
)

# Constantes
DB_PATH = "rasff_data.db"
GITHUB_REPO = "M00N69/RASFFDB"
DB_GITHUB_URL = "https://raw.githubusercontent.com/M00N69/RASFFDB/main/rasff_data.db"

# Structure de la base de données
TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS rasff (
    reference TEXT PRIMARY KEY,
    category TEXT,
    type TEXT,
    subject TEXT,
    date TEXT,
    notifying_country TEXT,
    classification TEXT,
    risk_decision TEXT,
    distribution TEXT,
    forAttention TEXT,
    forFollowUp TEXT,
    operator TEXT,
    origin TEXT,
    hazards TEXT,
    year INTEGER,
    month INTEGER,
    week INTEGER
);
"""

def init_database():
    """Initialise la base de données si elle n'existe pas"""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(TABLE_SCHEMA)
        conn.commit()
    st.success("Base de données initialisée")

def download_database():
    """Télécharge la base de données depuis GitHub"""
    try:
        response = requests.get(DB_GITHUB_URL)
        if response.status_code == 200:
            with open(DB_PATH, "wb") as f:
                f.write(response.content)
            st.success("Base de données téléchargée depuis GitHub")
            return True
        else:
            st.error(f"Erreur lors du téléchargement: {response.status_code}")
            return False
    except Exception as e:
        st.error(f"Exception lors du téléchargement: {e}")
        return False

def get_missing_weeks():
    """Détermine les semaines manquantes à récupérer"""
    with sqlite3.connect(DB_PATH) as conn:
        query = "SELECT MAX(date) AS last_date FROM rasff"
        result = conn.execute(query).fetchone()
        last_date = result[0] if result[0] else "01-01-2020 00:00:00"

    if last_date:
        try:
            last_dt = datetime.datetime.strptime(last_date, "%d-%m-%Y %H:%M:%S")
        except ValueError:
            # Si le format de date est différent, utiliser une date par défaut
            st.warning(f"Format de date incorrect: {last_date}")
            last_dt = datetime.datetime(2020, 1, 1)
    else:
        last_dt = datetime.datetime(2020, 1, 1)

    current_dt = datetime.datetime.now()
    current_year = current_dt.year
    current_week = current_dt.isocalendar()[1]  # Compatible avec Python < 3.9

    missing_weeks = []
    for year in range(last_dt.year, current_year + 1):
        start_week = 1 if year != last_dt.year else last_dt.isocalendar()[1] + 1
        end_week = 52 if year != current_year else current_week
        for week in range(start_week, end_week + 1):
            missing_weeks.append((year, week))
    
    return missing_weeks

def update_database(progress_bar=None):
    """Met à jour la base de données avec les nouvelles données"""
    with sqlite3.connect(DB_PATH) as conn:
        existing_refs = pd.read_sql("SELECT reference FROM rasff", conn)["reference"].tolist()
        missing_weeks = get_missing_weeks()
        
        if progress_bar is None:
            progress_bar = st.progress(0)
        
        log = st.empty()
        total_added = 0
        total_weeks = len(missing_weeks)
        
        for i, (year, week) in enumerate(missing_weeks):
            progress_bar.progress(i / total_weeks)
            url = f"https://www.sirene-diffusion.fr/regia/000-rasff/{str(year)[2:]}/rasff-{year}-{str(week).zfill(2)}.xls"
            try:
                log.text(f"Récupération {year}-W{week}...")
                response = requests.get(url, timeout=15)
                if response.status_code == 200:
                    xls = pd.ExcelFile(BytesIO(response.content))
                    sheets_data = []
                    for sheet in xls.sheet_names:
                        try:
                            sheet_df = pd.read_excel(xls, sheet_name=sheet)
                            sheets_data.append(sheet_df)
                        except Exception as e:
                            log.text(f"Erreur lecture feuille {sheet}: {e}")
                    
                    if not sheets_data:
                        log.text(f"Aucune donnée pour {year}-W{week}")
                        continue
                        
                    df = pd.concat(sheets_data, ignore_index=True)
                    
                    # Vérifier que les colonnes nécessaires existent
                    required_cols = ["reference", "date"]
                    if not all(col in df.columns for col in required_cols):
                        log.text(f"Colonnes manquantes pour {year}-W{week}")
                        continue
                    
                    # Nettoyage des données
                    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                    
                    # Conversion des dates
                    try:
                        df["date"] = pd.to_datetime(df["date"], errors="coerce")
                        df["year"] = df["date"].dt.year
                        df["month"] = df["date"].dt.month
                        df["week"] = df["date"].apply(lambda x: x.isocalendar()[1] if pd.notnull(x) else None)
                    except Exception as e:
                        log.text(f"Erreur conversion dates {year}-W{week}: {e}")
                        continue
                    
                    # Suppression des doublons
                    new_data = df[~df["reference"].isin(existing_refs)].dropna(subset=["reference"])
                    if not new_data.empty:
                        try:
                            new_data.to_sql("rasff", conn, if_exists="append", index=False)
                            existing_refs.extend(new_data["reference"].tolist())
                            total_added += len(new_data)
                            log.text(f"Semaine {year}-W{week}: {len(new_data)} alertes ajoutées")
                        except Exception as e:
                            log.text(f"Erreur insertion {year}-W{week}: {e}")
                    else:
                        log.text(f"Aucune nouvelle alerte pour {year}-W{week}")
                else:
                    log.text(f"Fichier non trouvé pour {year}-W{week}: {response.status_code}")
            except Exception as e:
                log.text(f"Erreur pour {year}-W{week}: {str(e)}")
        
        progress_bar.progress(1.0)
        return total_added

def push_to_github(github_token):
    """Pousse les modifications vers GitHub"""
    if not github_token:
        st.error("Token GitHub manquant. Veuillez fournir un token valide.")
        return False
    
    try:
        # Utilisation de requêtes HTTP pour éviter d'avoir à installer PyGithub
        headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        # Vérifier si le fichier existe déjà
        check_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{DB_PATH}"
        check_response = requests.get(check_url, headers=headers)
        
        # Lire le contenu du fichier local
        with open(DB_PATH, 'rb') as f:
            content = f.read()
        
        import base64
        encoded_content = base64.b64encode(content).decode("utf-8")
        
        if check_response.status_code == 200:
            # Le fichier existe, on le met à jour
            file_info = check_response.json()
            sha = file_info["sha"]
            
            update_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{DB_PATH}"
            payload = {
                "message": "Mise à jour automatique",
                "content": encoded_content,
                "sha": sha
            }
            
            response = requests.put(update_url, json=payload, headers=headers)
            
            if response.status_code in (200, 201):
                st.success("Base de données mise à jour sur GitHub")
                return True
            else:
                st.error(f"Erreur lors de la mise à jour: {response.status_code} - {response.text}")
                return False
        else:
            # Le fichier n'existe pas, on le crée
            create_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{DB_PATH}"
            payload = {
                "message": "Création initiale",
                "content": encoded_content
            }
            
            response = requests.put(create_url, json=payload, headers=headers)
            
            if response.status_code in (200, 201):
                st.success("Base de données créée sur GitHub")
                return True
            else:
                st.error(f"Erreur lors de la création: {response.status_code} - {response.text}")
                return False
    except Exception as e:
        st.error(f"Erreur GitHub: {e}")
        return False

def main():
    """Fonction principale"""
    st.title("🚨 RASFF Alerts - Mise à jour de la base")
    
    # Télécharger la base si elle n'existe pas
    if not os.path.exists(DB_PATH):
        st.info("Base de données locale non trouvée.")
        if st.button("📥 Télécharger depuis GitHub"):
            success = download_database()
            if not success:
                st.warning("Création d'une nouvelle base locale")
                init_database()
    else:
        init_database()
    
    # Afficher les statistiques de la base
    with sqlite3.connect(DB_PATH) as conn:
        count = conn.execute("SELECT COUNT(*) FROM rasff").fetchone()[0]
        last_date = conn.execute("SELECT MAX(date) FROM rasff").fetchone()[0]
    
    st.write(f"Base actuelle: **{count}** alertes, dernière date: **{last_date}**")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Mise à jour des données
        if st.button("🔄 Mettre à jour les données"):
            progress_bar = st.progress(0)
            updates = update_database(progress_bar)
            st.success(f"{updates} alertes ajoutées à la base")
    
    with col2:
        # Synchronisation GitHub
        github_token = st.text_input("Token GitHub", type="password")
        if st.button("🚀 Synchroniser avec GitHub"):
            if github_token:
                push_to_github(github_token)
            else:
                st.error("Veuillez entrer un token GitHub valide")
    
    # Aperçu des données
    if os.path.exists(DB_PATH):
        st.subheader("Aperçu des dernières alertes")
        with sqlite3.connect(DB_PATH) as conn:
            recent_data = pd.read_sql("SELECT * FROM rasff ORDER BY date DESC LIMIT 10", conn)
            st.dataframe(recent_data)

if __name__ == "__main__":
    main()
    
