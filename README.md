# RASFF Data Manager

RASFF Data Manager est une application interactive développée en Python et Streamlit, qui permet de gérer les données hebdomadaires du système d'alerte rapide pour les denrées alimentaires et les aliments pour animaux (RASFF). Cette application inclut des fonctionnalités avancées pour télécharger, nettoyer, et sauvegarder les données dans une base SQLite, ainsi que pour mettre à jour ces données directement sur GitHub.

---

## **Fonctionnalités**

1. **Téléchargement Automatique :**
   - Télécharge les données hebdomadaires manquantes jusqu'à la semaine courante.
   - Vérifie automatiquement la dernière semaine présente dans la base de données.

2. **Nettoyage des Données :**
   - Effectue un mappage des colonnes pour harmoniser les données.
   - Ajoute des catégories dérivées pour les produits et les dangers (prodcat, groupprod, hazcat, grouphaz).

3. **Sauvegarde dans SQLite :**
   - Stocke les données dans une base de données SQLite persistante (`rasff_data.db`).

4. **Mise à Jour GitHub :**
   - Pousse automatiquement la base de données mise à jour vers un dépôt GitHub via l'API.

5. **Interface Intuitive :**
   - Affichage des données via un tableau interactif.
   - Menu clair pour charger les données manquantes et synchroniser avec GitHub.

---

## **Installation**

### Prérequis
- Python 3.8 ou une version plus récente.
- Un compte GitHub avec un dépôt dédié pour stocker la base de données (`rasff_data.db`).
- Un token GitHub personnel avec les permissions nécessaires (voir ci-dessous).

### Instructions

1. Clonez ce dépôt :
   ```bash
   git clone https://github.com/M00N69/RASFFDB.git
   cd RASFFDB
Installez les dépendances nécessaires :

bash
Copier le code
pip install -r requirements.txt
Configurez votre token GitHub :

Générer un token GitHub avec les permissions repo ou public_repo pour un dépôt public.
Ajoutez ce token comme une variable d'environnement nommée GITHUB_TOKEN.
Exemple sous Linux/MacOS :

bash
Copier le code
export GITHUB_TOKEN=your_personal_access_token
Sous Windows :

cmd
Copier le code
set GITHUB_TOKEN=your_personal_access_token
Lancez l'application Streamlit :

bash
Copier le code
streamlit run RASFFDB.py
Ouvrez l'application dans votre navigateur :

Streamlit affichera un lien comme http://localhost:8501. Cliquez pour accéder à l'interface.
Utilisation
Menu Principal
Afficher les Données :

Affiche les données actuelles stockées dans la base SQLite.
Charger les Semaines Manquantes :

Télécharge automatiquement les semaines non présentes dans la base jusqu'à la semaine courante.
Nettoie et insère les données dans la base SQLite.
Pousser le Fichier vers GitHub :

Pousse la base rasff_data.db mise à jour dans le dépôt GitHub configuré.
Structure du Projet
bash
Copier le code
RASFFDB/
│
├── RASFFDB.py         # Code principal de l'application
├── requirements.txt   # Dépendances Python
├── rasff_data.db      # Base de données SQLite (créée automatiquement)
└── README.md          # Documentation du projet
Dépendances
Les bibliothèques utilisées dans ce projet incluent :

streamlit : Pour l'interface utilisateur.
pandas : Pour le traitement des données tabulaires.
sqlite3 : Pour stocker les données localement.
requests : Pour télécharger les fichiers Excel depuis des URLs.
openpyxl : Pour lire et traiter les fichiers Excel.
PyGithub : Pour interagir avec l'API GitHub.
Pour installer toutes les dépendances :

bash
Copier le code
pip install -r requirements.txt
Mappages Utilisés
Catégories de Produits
Les produits sont classés selon les catégories suivantes :

Exemples :
"alcoholic beverages" → ["Alcoholic Beverages", "Beverages"]
"fruits and vegetables" → ["Fruits and Vegetables", "Fruits and Vegetables"]
Voir le fichier RASFFDB.py pour les détails complets.
Catégories de Dangers
Les dangers sont classés selon les catégories suivantes :

Exemples :
"adulteration / fraud" → ["Adulteration / Fraud", "Food Fraud"]
"pathogenic micro-organisms" → ["Pathogenic Micro-organisms", "Biological Hazard"]
Voir le fichier RASFFDB.py pour les détails complets.
Contributions
Les contributions sont les bienvenues ! Voici comment vous pouvez contribuer :

Forkez ce dépôt.
Créez une branche pour vos modifications :
bash
Copier le code
git checkout -b feature-xyz
Testez vos modifications.
Soumettez une Pull Request.
Auteurs
[Votre Nom ou Pseudo GitHub]
Développeur principal de l'application.
Licence
Ce projet est sous licence MIT. Consultez le fichier LICENSE pour plus de détails.

Remarques Importantes
Si vous utilisez cette application sur Streamlit Cloud, assurez-vous de configurer les secrets nécessaires (GITHUB_TOKEN) dans la section Secrets de l'application.
La base SQLite (rasff_data.db) sera recréée automatiquement si elle est manquante.
