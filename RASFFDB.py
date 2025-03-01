import pandas as pd
import sqlite3
import requests
import datetime
from github import Github
from io import BytesIO
import os

# Constantes
DB_PATH = "rasff_data.db"
GITHUB_REPO = "M00N69/RASFFDB"
DB_GITHUB_URL = "https://raw.githubusercontent.com/M00N69/RASFFDB/main/rasff_data.db"
GITHUB_TOKEN = "votre_token_github"  # Remplacez par votre token ou utilisez os.getenv("GITHUB_TOKEN")

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
    print("Base de données initialisée")

def download_database():
    """Télécharge la base de données depuis GitHub"""
    try:
        response = requests.get(DB_GITHUB_URL)
        if response.status_code == 200:
            with open(DB_PATH, "wb") as f:
                f.write(response.content)
            print("Base de données téléchargée depuis GitHub")
            return True
        else:
            print(f"Erreur lors du téléchargement: {response.status_code}")
            return False
    except Exception as e:
        print(f"Exception lors du téléchargement: {e}")
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
            print(f"Format de date incorrect: {last_date}")
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
    
    print(f"Semaines manquantes: {len(missing_weeks)}")
    return missing_weeks

def update_database():
    """Met à jour la base de données avec les nouvelles données"""
    with sqlite3.connect(DB_PATH) as conn:
        existing_refs = pd.read_sql("SELECT reference FROM rasff", conn)["reference"].tolist()
        missing_weeks = get_missing_weeks()
        
        total_added = 0
        for year, week in missing_weeks:
            url = f"https://www.sirene-diffusion.fr/regia/000-rasff/{str(year)[2:]}/rasff-{year}-{str(week).zfill(2)}.xls"
            try:
                print(f"Récupération {year}-W{week}: {url}")
                response = requests.get(url, timeout=15)
                if response.status_code == 200:
                    xls = pd.ExcelFile(BytesIO(response.content))
                    sheets_data = []
                    for sheet in xls.sheet_names:
                        try:
                            sheet_df = pd.read_excel(xls, sheet_name=sheet)
                            sheets_data.append(sheet_df)
                        except Exception as e:
                            print(f"Erreur lecture feuille {sheet}: {e}")
                    
                    if not sheets_data:
                        print(f"Aucune donnée pour {year}-W{week}")
                        continue
                        
                    df = pd.concat(sheets_data, ignore_index=True)
                    
                    # Vérifier que les colonnes nécessaires existent
                    required_cols = ["reference", "date"]
                    if not all(col in df.columns for col in required_cols):
                        print(f"Colonnes manquantes pour {year}-W{week}")
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
                        print(f"Erreur conversion dates {year}-W{week}: {e}")
                        continue
                    
                    # Suppression des doublons
                    new_data = df[~df["reference"].isin(existing_refs)].dropna(subset=["reference"])
                    if not new_data.empty:
                        try:
                            new_data.to_sql("rasff", conn, if_exists="append", index=False)
                            existing_refs.extend(new_data["reference"].tolist())
                            total_added += len(new_data)
                            print(f"Semaine {year}-W{week}: {len(new_data)} alertes ajoutées")
                        except Exception as e:
                            print(f"Erreur insertion {year}-W{week}: {e}")
                    else:
                        print(f"Aucune nouvelle alerte pour {year}-W{week}")
                else:
                    print(f"Fichier non trouvé pour {year}-W{week}: {response.status_code}")
            except Exception as e:
                print(f"Erreur pour {year}-W{week}: {str(e)}")
        
        print(f"Total: {total_added} alertes ajoutées")
        return total_added

def push_to_github():
    """Pousse les modifications vers GitHub"""
    try:
        g = Github(GITHUB_TOKEN)
        repo = g.get_repo(GITHUB_REPO)
        
        with open(DB_PATH, 'rb') as f:
            content = f.read()
        
        try:
            contents = repo.get_contents(DB_PATH)
            repo.update_file(
                DB_PATH,
                "Mise à jour automatique",
                content,
                contents.sha
            )
            print("Base de données mise à jour sur GitHub")
            return True
        except Exception as e:
            print(f"Erreur lors de la mise à jour: {e}")
            # Si le fichier n'existe pas, le créer
            try:
                repo.create_file(
                    DB_PATH,
                    "Création initiale",
                    content
                )
                print("Base de données créée sur GitHub")
                return True
            except Exception as e2:
                print(f"Erreur lors de la création: {e2}")
                return False
    except Exception as e:
        print(f"Erreur d'authentification GitHub: {e}")
        return False

def main():
    """Fonction principale"""
    # Télécharger la base si elle n'existe pas
    if not os.path.exists(DB_PATH):
        success = download_database()
        if not success:
            print("Création d'une nouvelle base")
    
    # Initialiser la structure
    init_database()
    
    # Mettre à jour avec les données récentes
    updates = update_database()
    
    # Pousser vers GitHub si des mises à jour ont été faites
    if updates > 0:
        push_to_github()
    else:
        print("Aucune mise à jour à synchroniser")

if __name__ == "__main__":
    main()
