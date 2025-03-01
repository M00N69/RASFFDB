🛠 Explication Complète du Code :
🟢 Partie 1 : Fonctionnalités Utilisateur (Interface Streamlit)
Objectif: Fournir un tableau de bord interactif pour consulter les alertes RASFF.

🌟 1. Mise en Page et Configuration:
python
Copier
Modifier
import streamlit as st
st.set_page_config(layout="wide")  # ✅ Mode large activé
Fonction: Définit la page en mode large (wide) pour un affichage optimisé des tableaux et graphiques.
🌟 2. Titre et Interface:
python
Copier
Modifier
st.title("🚨 RASFF Alerts Dashboard")
Fonction: Affiche le titre principal du tableau de bord.
🌟 3. Filtrage Dynamique:
python
Copier
Modifier
selected_country = st.sidebar.selectbox("Pays", ["Tous"] + sorted(df["notifying_country"].unique()))
selected_year = st.sidebar.selectbox("Année", ["Tous"] + sorted(df["year"].unique(), reverse=True))
selected_category = st.sidebar.selectbox("Catégorie", ["Toutes"] + sorted(df["category"].unique()))
Fonction: Ajoute des menus déroulants dans la barre latérale pour :
Filtrer par pays notifiant.
Filtrer par année.
Filtrer par catégorie de produit.
🌟 4. Application des Filtres et Affichage des Données:
python
Copier
Modifier
filtered_df = df.copy()
if selected_country != "Tous":
    filtered_df = filtered_df[filtered_df["notifying_country"] == selected_country]
if selected_year != "Tous":
    filtered_df = filtered_df[filtered_df["year"] == selected_year]
if selected_category != "Toutes":
    filtered_df = filtered_df[filtered_df["category"] == selected_category]

st.write(f"## 📊 {len(filtered_df)} alertes ({selected_year})")
st.dataframe(filtered_df, height=600)
Fonction:
Applique les filtres sélectionnés par l'utilisateur.
Affiche les résultats filtrés dans un tableau dynamique.
🌟 5. Visualisation avec des Graphiques:
python
Copier
Modifier
st.write("## 🌟 Répartition par pays")
st.bar_chart(filtered_df["notifying_country"].value_counts().head(10))
Fonction: Génère un graphique en barres des alertes par pays (Top 10).
🔵 Partie 2 : Construction et Gestion des Données
Objectif: Gérer automatiquement le téléchargement, la mise à jour et l'intégration des données dans la base de données SQLite.

🔄 1. Téléchargement de la Base de Données depuis GitHub:
python
Copier
Modifier
def download_from_github():
    url = f"https://raw.githubusercontent.com/{REPO_OWNER}/{REPO_NAME}/main/{FILE_PATH}"
    response = requests.get(url)
    with open(DB_PATH, "wb") as file:
        file.write(response.content)
Fonction: Télécharge la base (rasff_data.db) si elle n'existe pas localement.
🛠 2. Ajout des Colonnes Manquantes (year et week):
python
Copier
Modifier
def add_missing_columns():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("ALTER TABLE rasff_notifications ADD COLUMN year INTEGER")
        cursor.execute("ALTER TABLE rasff_notifications ADD COLUMN week INTEGER")
        cursor.execute("UPDATE rasff_notifications SET year = strftime('%Y', date)")
        cursor.execute("UPDATE rasff_notifications SET week = strftime('%W', date)")
        conn.commit()
Fonction:
Ajoute les colonnes year et week si elles n'existent pas.
Remplit ces colonnes à partir de la colonne date.
🔄 3. Téléchargement Automatique des Nouvelles Semaines:
python
Copier
Modifier
def update_database():
    last_year, last_week = get_last_update_info()
    current_year = pd.Timestamp.now().year
    current_week = pd.Timestamp.now().week

    for year in range(last_year, current_year + 1):
        start_week = last_week + 1 if year == last_year else 1
        end_week = current_week if year == current_year else 52

        for week in range(start_week, end_week + 1):
            week_str = str(week).zfill(2)
            url = f"https://www.sirene-diffusion.fr/regia/000-rasff/{str(year)[-2:]}/rasff-{year}-{week_str}.xls"
            response = requests.get(url)

            if response.status_code == 200:
                df = pd.read_excel(BytesIO(response.content))
                df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y %H:%M:%S')
                df['year'] = df['date'].dt.year
                df['week'] = df['date'].dt.isocalendar().week

                with sqlite3.connect(DB_PATH) as conn:
                    df.to_sql("rasff_notifications", conn, if_exists="append", index=False)
                print(f"✅ Données ajoutées pour l'année {year}, semaine {week_str}")
            else:
                print(f"❌ Fichier non trouvé pour l'année {year}, semaine {week_str}")
                break
Fonction:
Vérifie la dernière semaine enregistrée.
Télécharge les fichiers .xls manquants.
Insère les nouvelles données dans rasff_notifications.
🔄 4. Synchronisation Automatique avec GitHub:
python
Copier
Modifier
def update_github():
    with open(DB_PATH, "rb") as file:
        content = file.read()
    encoded_content = base64.b64encode(content).decode()

    response = requests.get(GITHUB_API_URL, headers={
        "Authorization": f"Bearer {GITHUB_TOKEN}"
    })
    response_data = response.json()
    sha = response_data.get("sha", None)

    data = {
        "message": "Mise à jour automatique de la base RASFF",
        "content": encoded_content,
        "sha": sha
    }

    response = requests.put(GITHUB_API_URL, json=data, headers={
        "Authorization": f"Bearer {GITHUB_TOKEN}"
    })
Fonction:
Encode le fichier .db en base64.
Utilise l'API GitHub pour pousser les modifications.
🟢 Conclusion : Comment Tout Fonctionne Ensemble
Au démarrage :

Télécharge la base si absente.
Ajoute les colonnes year et week si manquantes.
Télécharge et insère les nouvelles semaines automatiquement.
Synchronise les changements sur GitHub.
Interface Utilisateur:

Tableau de bord avec filtres (pays, année, catégorie).
Affichage dynamique des données.
Graphiques interactifs.

