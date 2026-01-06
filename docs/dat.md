# Dossier d'Architecture Technique (DAT) - Projet ECF 1

Ce document présente les choix structurants réalisés pour la mise en œuvre du pipeline de données dans le cadre du projet ECF .

---

## 1. Choix d'architecture globale

### Architecture proposée : Medallion (Lakehouse)
J'ai opté pour une architecture **Medallion**, qui organise les données en trois niveaux de maturité.

* **Pourquoi ce choix ?** Cette structure permet de séparer le stockage brut du stockage analytique. Elle offre la flexibilité d'un **Data Lake** pour l'ingestion et la puissance d'un **Data Warehouse** pour l'interrogation SQL.
* **Avantages :**
    * **Traçabilité :** Les données originales sont conservées en couche Bronze.
    * **Qualité :** Les transformations sont isolées et testables entre chaque couche.
    * **Robustesse :** Un crash de la base SQL n'entraîne pas de perte de données (re-traitement possible depuis MinIO).
* **Inconvénients :**
    * **Espace de stockage :** La donnée est dupliquée sous différentes formes.
    * **Maintenance :** Nécessite de maintenir deux systèmes (MinIO pour les fichiers, PostgreSQL pour le SQL).



---

## 2. Choix des technologies

| Composant | Technologie | Justification | Alternative |
| :--- | :--- | :--- | :--- |
| **Ingestion** | **Scrapy / Python** | Puissant pour le scraping web et l'automatisation des flux. | BeautifulSoup (plus limité). |
| **Stockage Objet** | **MinIO** | Stockage compatible S3, idéal pour gérer les fichiers bruts (Raw Data). | Azure Blob Storage / AWS S3. |
| **Base de données** | **PostgreSQL** | Moteur relationnel standard, performant pour les jointures et le décisionnel. | MongoDB (NoSQL). |
| **Orchestration** | **Python (Pipeline)** | Scripting sur mesure permettant un contrôle total du flux ETL. | Apache Airflow (plus complexe). |

---

## 3. Organisation des données

Les données transitent par trois compartiments logiques pour garantir leur intégrité :

1.  **Bronze (Raw) :** Stockage dans Minio des exports bruts (Excel/JSON) directement issus des scrapers.
2.  **Silver (Cleaned) :** Stockage dans MinioDonnées nettoyées (types convertis, doublons supprimés) au format CSV.
3.  **Gold (Curated) :** Données enrichies (Géocodage API) et anonymisées, chargées dans des tables PostgreSQL.

J'aurai pu également conserver les données Gold dans minio mais ça ne paraissait pas nécessaire pour le besoin final qui de faire du reporting sur base SQL.

**Convention de nommage :**
- **Fichiers :** `[source]_[date]_[heure].ext`
- **Tables SQL :** Préfixe `fact_` pour les données transactionnelles et `dim_` pour les référentiels.

---

## 4. Modélisation des données

### Modèle en Flocon (Snowflake Schema)
La couche finale (Gold) utilise un modèle relationnel structuré pour optimiser les performances analytiques.

* **Tables principales :**
    * `fact_books` : Catalogue des livres (titres, prix, notes).
    * `fact_quotes` : Base de citations et auteurs.
    * `fact_products` : Données e-commerce (Laptops/Informatique).
    * `dim_partners` : Référentiel des librairies avec données géographiques.
* **Justification :** Ce modèle permet des analyses croisées (ex: corrélation thématique entre citations et livres) tout en assurant une cohérence des données géographiques.



---

## 5. Conformité RGPD

La protection des données personnelles est intégrée dès la conception du pipeline :

## 5. Conformité RGPD & Confidentialité

La protection des données personnelles et la sécurité des données sensibles sont intégrées dès la conception du pipeline (Privacy by Design) :

* **Données identifiées :** La source "Librairies" contient des noms de contacts, des emails et des téléphones.
* **Mesures de protection des personnes :**
    * **Pseudonymisation :** Hachage cryptographique `SHA-256` des adresses emails pour créer un identifiant technique unique (`contact_id`).
    * **Minimisation :** Suppression automatique des colonnes identifiantes (`contact_nom`, `contact_telephone`) lors de la transition vers la zone Gold.
* **Confidentialité des Affaires :** * Les **chiffres d'affaires** des librairies partenaires sont traités comme des **données strictement confidentielles**. 

* **Droit à l'effacement :** L'utilisation du `contact_id` permet de retrouver et supprimer les données d'un individu sur simple demande, tout en conservant les agrégats statistiques anonymes.


---