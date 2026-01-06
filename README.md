# ECF Data Engineering : Pipeline Multi-Sources & Analytics (Medallion)

## 📝 Description
Ce projet implémente un pipeline de données ETL (Extract, Transform, Load) complet pour collecter, transformer et analyser des données provenant de quatre sources distinctes. L'objectif est de démontrer la capacité à orchestrer des flux de données complexes, à gérer une infrastructure hybride (S3/SQL) et à assurer la conformité et la qualité des données.

**Sources de données :**
- **Books to Scrape** : Catalogue de livres (Prix, notes, thématiques).
- **Quotes to Scrape** : Citations et métadonnées auteurs.
- **E-commerce Site** : Données techniques et tarifs informatiques (Laptops).
- **Partenaires** : Données de géolocalisation des librairies (API Géo).

## Architecture

```text
┌──────────────────────────────────────────┐
│              SOURCES DU WEB              │
│ (Books, Quotes, E-commerce, Librairies)  │
└───────────────────┬──────────────────────┘
                    │ Scraping (Scrapy / Python)
                    ▼
┌──────────────────────────────────────────┐
│                PIPELINE                  │
│      Extract → Transform → Load          │
└─────────┬────────────────────────┬───────┘
          │                        │
    ┌─────▼─────┐            ┌─────▼─────┐
    │   MinIO   │            │ PostgreSQL│
    │ (Bronze/Silver)        │   (Gold)  │
    │           │            │           │
    │ Fichiers Bruts         │ Tables de Faits
    │ Données Cleaned        │ Rapports SQL

```
## Justification de l'architecture hybride

L'architecture repose sur le schéma **Medallion**, garantissant une séparation stricte des responsabilités et une traçabilité totale des données.



| Couche | Technologie | Rôle |
| :--- | :--- | :--- |
| **Bronze** | **MinIO (S3)** | Stockage des fichiers bruts (JSON/XLSX) tels qu'extraits des scrapers. |
| **Silver** | **MinIO (S3)** | Données nettoyées, dédoublonnées et converties au format CSV. |
| **Gold** | **PostgreSQL** | Données enrichies, anonymisées et structurées pour le reporting final. |

---

## Démarrage rapide

## Prérequis

- Python 3.10+
- Docker et Docker Compose
- Git

## Installation


```bash
# 1. Cloner le projet
git clone <url-du-repo>
cd ECF_1_Clement_Raczek

# 2. Créer l'environnement virtuel
python -m venv venv

# 3. Activer l'environnement virtuel
# Sur Windows :
.\venv\Scripts\activate
# Sur Linux/Mac :
source venv/bin/activate

# 4. Installer les dépendances
pip install --upgrade pip
pip install -r requirements.txt

# 5. Lancer l'infrastructure (MinIO & Postgres)
docker-compose up -d

## Vérification de l'infrastructure

- **MinIO Console** : http://localhost:9001
  - Login : `minioadmin`
  - Password : `minioadmin123`

- **PostGreSQL** : http://localhost:5433
  - Base de données : `analytics`
  - Login : `dataeng`
  - Password : `dataeng123`

## Utilisation

# Exécuter le pipeline

**Lancement complet (Reset + Ingest + Clean + Gold)**
python -m src.pipeline --all

**Lancement complet avec résumé statistique final**
python -m src.pipeline --all --analytics

**Uniquement la phase d'extraction (Bronze)**
python -m src.pipeline --ingest

**Uniquement la phase de transformation (Silver)**
python -m src.pipeline --clean

**Uniquement l'injection en base de données et reporting (Gold)**
python -m src.pipeline --gold


### Options disponibles

--all	Exécute la totalité du pipeline ETL
--ingest	Lance uniquement les scrapers (Books, Quotes, Commerce)
--clean	Lance les scripts de nettoyage Pandas
--gold	Lance l'injection PostgreSQL et génère le rapport Excel



## Structure du projet

```
ECF_1_Clement_Raczek/
├── config/             # Paramètres MinIO, DB et API
├── sql/                # Scripts et rapports Excel générés
├── src/
│   ├── ingestion/      # Scrapers (Bronze)
│   ├── processing/     # Nettoyage et Loading (Silver/Gold)
│   ├── storage/        # Clients MinIO et Scripts Reset
│   ├── analytics/      # Vues SQL et tests de qualité
│   └── pipeline.py     # Orchestrateur principal
├── docker-compose.yml  # Infrastructure
└── requirements.txt    # Dépendances (incluant xlsxwriter)
```






### Variables d'environnement (.env)

```env
# ================================
# Configuration MinIO (Data Lake)
# ================================
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
MINIO_SECURE=false

MINIO_BUCKET_BRONZE=bronze
MINIO_BUCKET_SILVER=silver
MINIO_BUCKET_GOLD=gold


# ================================
# Configuration PostgreSQL
# ================================
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=analytics
POSTGRES_USER=dataeng
POSTGRES_PASSWORD=dataeng123


# ================================
# Configuration Pipeline
# ================================
PIPELINE_ENV=dev
SCRAPER_DELAY_SECONDS=1
LOG_LEVEL=INFO

```

### Modification de la configuration

Éditez `config/settings.py` pour modifier :
- URLs et credentials
- Délai entre requêtes
- Nombre max de pages
- Noms des buckets

## Analytics disponibles

Le pipeline génère automatiquement :
- Un rapport excel reprenant une vue globale des tables mais limités à 100 lignes par requête
- Un raport excel répondant aux questions de l'ECF



## Ressources

## Ressources

## Scraping & Extraction
* [Scrapy Documentation](https://docs.scrapy.org/en/latest/) : Framework principal utilisé pour l'orchestration des spiders Books et Quotes.
* [webscraper.io Test Sites](https://webscraper.io/test-sites) : Plateforme cible pour l'apprentissage du scraping e-commerce.
* [BeautifulSoup Documentation](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) : Bibliothèque utilisée pour le parsing chirurgical des données e-commerce.

## Stockage & Infrastructure
* [MinIO Python SDK](https://min.io/docs/minio/linux/developers/python/minio-py.html) : Gestion du stockage objet S3 pour les couches Bronze et Silver.
* [PostgreSQL Documentation](https://www.postgresql.org/docs/) : Moteur de base de données relationnelle pour la couche Gold.
* [SQLAlchemy & Pandas](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html) : Outils de Mapping Objet-Relationnel (ORM) et d'injection massive de données.

## Architecture & Qualité
* [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture) : Concept de structuration des données par niveaux de qualité (Bronze, Silver, Gold).
* [Data Quality in ETL](https://www.metabase.com/learn/data-stack/data-quality) : Principes de validation SQL implémentés dans `sql_test.py`.
