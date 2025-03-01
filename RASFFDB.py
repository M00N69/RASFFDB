# === AJOUTS POUR GÉRER L'UPLOAD DE FICHIER ===
from google.colab import files
import io

def upload_excel_file():
    """Permet l'upload d'un fichier Excel via Streamlit"""
    uploaded_file = st.file_uploader(" Charger un fichier Excel", type=["xlsx"])
    if uploaded_file:
        try:
            xls = pd.ExcelFile(uploaded_file)
            dfs = [pd.read_excel(xls, sheet_name=sn) for sn in xls.sheet_names]
            return pd.concat(dfs, ignore_index=True)
        except Exception as e:
            st.error(f"Erreur lecture fichier : {e}")
    return None

# === MODIFICATIONS DE LA STRUCTURE DE LA BASE ===
def create_database():
    """Crée la structure complète de la base"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS rasff_data (
            reference TEXT PRIMARY KEY,
            date TEXT,
            date_of_case DATETIME,
            notifying_country TEXT,
            country_origin TEXT,
            product_category TEXT,
            product_type TEXT,
            subject TEXT,
            hazard_substance TEXT,
            hazard_category TEXT,
            classification TEXT,
            risk_decision TEXT,
            distribution TEXT,
            attention TEXT,
            follow_up TEXT,
            year INTEGER,
            month INTEGER,
            week INTEGER
        )
    ''')
    conn.commit()
    conn.close()

# === AJOUT DE LA FONCTION DE MISE À JOUR MANUELLE ===
def update_from_uploaded_file(df):
    """Insère les données uploadées dans la base"""
    df['date_of_case'] = pd.to_datetime(df['date'], errors='coerce')
    df['year'] = df['date_of_case'].dt.year
    df['month'] = df['date_of_case'].dt.month
    df['week'] = df['date_of_case'].dt.isocalendar().week

    conn = sqlite3.connect(DB_PATH)
    existing_refs = pd.read_sql("SELECT reference FROM rasff_data", conn)['reference'].tolist()
    new_data = df[~df['reference'].isin(existing_refs)].dropna(subset=['reference'])
    
    if not new_data.empty:
        new_data.to_sql('rasff_data', conn, if_exists='append', index=False)
        st.success(f"{len(new_data)} nouvelles alertes ajoutées depuis le fichier uploadé")
    else:
        st.info("Aucune donnée nouvelle trouvée")
    conn.close()

# === MODIFICATIONS DE L'INTERFACE ===
def main():
    st.title("🚨 RASFF Alerts Dashboard")
    
    # Initialisation de la base
    if not os.path.exists(DB_PATH):
        download_github_db()
    create_database()
    update_database_structure()
    
    # Menu latéral
    st.sidebar.title("⚙️ Paramètres")
    if st.sidebar.button("🔄 Récupérer GitHub"):
        download_github_db()
    
    if st.sidebar.button("🔄 Push GitHub"):
        if st.sidebar.checkbox("Confirmer le push"):
            push_to_github()
    
    # Upload manuel de fichier
    st.sidebar.title("📤 Upload Manuel")
    if st.sidebar.button("Mettre à jour à partir du fichier"):
        df = upload_excel_file()
        if isinstance(df, pd.DataFrame):
            with st.spinner("Traitement en cours..."):
                update_from_uploaded_file(df)
                push_to_github()
    
    # Mise à jour automatique par semaines
    if st.button("🔄 Mise à jour automatique"):
        new_alerts = update_database()
        if new_alerts > 0:
            push_to_github()
    
    # Affichage des données
    df = pd.read_sql("SELECT * FROM rasff_data", sqlite3.connect(DB_PATH))
    st.write("## 📊 Données")
    st.dataframe(df.head(10))
    
    # Statistiques basiques
    st.write("## 📊 Statistiques")
    st.write(f"Total alertes : {len(df)}")
    st.write("Par pays d'origine :")
    st.bar_chart(df['country_origin'].value_counts().head(10))
